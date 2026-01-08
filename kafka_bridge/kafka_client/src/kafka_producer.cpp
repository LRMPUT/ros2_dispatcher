// Copyright 2025 Maciej Krupka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "kafka_client/kafka_producer.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <utility>

#include <librdkafka/rdkafkacpp.h>

namespace kafka_client
{
namespace
{
bool set_optional_conf(
  RdKafka::Conf & conf, const std::string & key, const std::optional<int> & value,
  std::string & error)
{
  if (value.has_value()) {
    return conf.set(key, std::to_string(*value), error) == RdKafka::Conf::CONF_OK;
  }
  return true;
}
}  // namespace

KafkaProducer::KafkaProducer(KafkaProducerConfig config)
: config_(std::move(config))
{
}

KafkaProducer::~KafkaProducer()
{
  stop();
}

bool KafkaProducer::create_producer_locked(std::string * error_message)
{
  std::string errstr;
  std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

  auto set_conf = [&](const std::string & key, const std::string & value) {
      return conf->set(key, value, errstr) == RdKafka::Conf::CONF_OK;
    };

  if (!set_conf("bootstrap.servers", config_.bootstrap_servers) ||
    !set_conf("client.id", config_.client_id) ||
    !set_conf("acks", config_.acks))
  {
    *error_message = errstr;
    return false;
  }

  if (!set_optional_conf(*conf, "linger.ms", config_.linger_ms, errstr) ||
    !set_optional_conf(*conf, "batch.size", config_.batch_size, errstr) ||
    !set_optional_conf(*conf, "queue.buffering.max.kbytes", config_.queue_buffering_max_kbytes,
      errstr) ||
    !set_optional_conf(*conf, "max.in.flight.requests.per.connection", config_.max_in_flight,
      errstr) ||
    !set_optional_conf(*conf, "retries", config_.retries, errstr) ||
    !set_optional_conf(*conf, "retry.backoff.ms", config_.retry_backoff_ms, errstr))
  {
    *error_message = errstr;
    return false;
  }

  if (config_.enable_idempotence) {
    if (!set_conf("enable.idempotence", "true")) {
      *error_message = errstr;
      return false;
    }
  }

  auto producer = std::unique_ptr<RdKafka::Producer>(RdKafka::Producer::create(conf.get(), errstr));
  if (!producer) {
    *error_message = errstr;
    return false;
  }

  producer_ = std::move(producer);
  status_ = ProducerStatus::RUNNING;
  last_error_.clear();
  return true;
}

KafkaSendResult KafkaProducer::try_produce_locked(const QueuedRecord & record)
{
  if (!producer_) {
    return {SendStatus::PRODUCER_UNAVAILABLE, "producer not available"};
  }

  RdKafka::Headers * headers = RdKafka::Headers::create();
  for (const auto & header : record.headers) {
    headers->add(header.key, header.value);
  }

  RdKafka::ErrorCode result = producer_->produce(
    record.topic,
    RdKafka::Topic::PARTITION_UA,
    RdKafka::Producer::RK_MSG_COPY,
    const_cast<uint8_t *>(record.value.data()),
    record.value.size(),
    record.key.empty() ? nullptr : record.key.data(),
    record.key.empty() ? 0 : record.key.size(),
    record.timestamp_ms,
    nullptr,
    headers);

  if (result == RdKafka::ERR_NO_ERROR) {
    // ownership of headers transferred to librdkafka on success
    ++sent_ok_;
    return {SendStatus::SENT, {}};
  }

  // on error, we need to delete headers since ownership wasn't transferred
  delete headers;

  if (result == RdKafka::ERR__QUEUE_FULL) {
    return {SendStatus::QUEUE_FULL, RdKafka::err2str(result), false};
  }

  ++send_errors_;
  update_last_error_locked(RdKafka::err2str(result));
  return {SendStatus::ERROR, RdKafka::err2str(result), false};
}

KafkaSendResult KafkaProducer::send(
  const std::string & topic,
  const std::vector<uint8_t> & key,
  const std::vector<uint8_t> & value,
  int64_t timestamp_ms,
  const std::vector<KafkaHeader> & headers)
{
  std::unique_lock<std::mutex> lock(mutex_);
  if (!running_.load(std::memory_order_acquire)) {
    return {SendStatus::PRODUCER_UNAVAILABLE, "producer stopped", false};
  }

  QueuedRecord record{topic, key, value, timestamp_ms, headers};

  if (producer_) {
    auto result = try_produce_locked(record);
    if (result.status != SendStatus::QUEUE_FULL || config_.drop_when_full) {
      if (result.status == SendStatus::QUEUE_FULL && config_.drop_when_full) {
        ++dropped_queue_full_;
      }
      return result;
    }
    // fallthrough for retry when queue is full and buffering is allowed
  }

  if (pending_records_.size() >= config_.max_queue_messages) {
    ++dropped_queue_full_;
    return {SendStatus::QUEUE_FULL, "pending buffer full", false};
  }

  pending_records_.emplace_back(std::move(record));
  cv_.notify_all();
  return {SendStatus::QUEUE_FULL, "buffered for retry", true};
}

void KafkaProducer::poll_loop()
{
  std::unique_lock<std::mutex> lock(mutex_);
  while (running_.load(std::memory_order_acquire)) {
    if (!producer_) {
      std::string err;
      if (!create_producer_locked(&err)) {
        status_ = ProducerStatus::DEGRADED;
        update_last_error_locked(err);
      }
    }

    if (producer_) {
      while (!pending_records_.empty()) {
        auto record = pending_records_.front();
        auto result = try_produce_locked(record);
        if (result.status == SendStatus::QUEUE_FULL && !config_.drop_when_full) {
          break;
        } else if (result.status == SendStatus::QUEUE_FULL && config_.drop_when_full) {
          ++dropped_queue_full_;
        }
        pending_records_.pop_front();
      }
    }

    lock.unlock();
    if (producer_) {
      producer_->poll(config_.poll_timeout_ms);
    }
    lock.lock();

    const int wait_ms = producer_ ? config_.poll_timeout_ms : config_.reconnect_backoff_ms;
    cv_.wait_for(
      lock, std::chrono::milliseconds(wait_ms),
      [this]() {
        return !running_.load(std::memory_order_acquire) || !pending_records_.empty();
      });
  }
}

bool KafkaProducer::start()
{
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    return true;
  }

  status_ = ProducerStatus::STARTING;
  std::string err;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!create_producer_locked(&err)) {
      if (config_.startup_mode == StartupMode::STRICT) {
        running_.store(false, std::memory_order_release);
        status_ = ProducerStatus::FAILED;
        update_last_error_locked(err);
        return false;
      }
      status_ = ProducerStatus::DEGRADED;
      update_last_error_locked(err);
    }
  }

  poll_thread_ = std::thread(&KafkaProducer::poll_loop, this);
  return true;
}

void KafkaProducer::stop()
{
  bool expected = true;
  if (!running_.compare_exchange_strong(expected, false)) {
    return;
  }

  cv_.notify_all();
  if (poll_thread_.joinable()) {
    poll_thread_.join();
  }

  std::unique_lock<std::mutex> lock(mutex_);
  if (producer_) {
    producer_->flush(config_.flush_timeout_ms);
  }
  producer_.reset();
  pending_records_.clear();
  status_ = ProducerStatus::STOPPED;
}

void KafkaProducer::update_last_error_locked(const std::string & message)
{
  last_error_ = message;
}

ProducerHealth KafkaProducer::health() const
{
  std::lock_guard<std::mutex> lock(mutex_);
  ProducerHealth health;
  health.status = status_;
  health.last_error = last_error_;
  health.sent_ok = sent_ok_.load();
  health.dropped_queue_full = dropped_queue_full_.load();
  health.send_errors = send_errors_.load();
  return health;
}

}  // namespace kafka_client
