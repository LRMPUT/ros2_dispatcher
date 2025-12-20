#include "kafka_client/kafka_producer.hpp"

#include <chrono>
#include <memory>
#include <rdkafkacpp.h>
#include <stdexcept>
#include <utility>

namespace kafka_client
{
namespace
{
constexpr int kDefaultPollIntervalMs = 50;
constexpr int kMetadataTimeoutMs = 5000;

class DeliveryCallback : public RdKafka::DeliveryReportCb
{
public:
  explicit DeliveryCallback(KafkaProducer * parent)
  : parent_(parent)
  {}

  void dr_cb(RdKafka::Message & message) override
  {
    if (message.err() == RdKafka::ERR_NO_ERROR) {
      parent_->on_delivery_success();
      return;
    }

    parent_->on_delivery_failure(message.errstr());
  }

private:
  KafkaProducer * parent_;
};
}  // namespace

KafkaProducer::KafkaProducer(ProducerConfig config)
: config_(std::move(config))
{
}

KafkaProducer::~KafkaProducer()
{
  stop();
}

bool KafkaProducer::set_config_if_present(
  RdKafka::Conf & conf, const std::string & key, const std::optional<int> & value,
  std::string * error_message)
{
  if (!value.has_value()) {
    return true;
  }

  std::string errstr;
  if (conf.set(key, std::to_string(value.value()), errstr) != RdKafka::Conf::CONF_OK) {
    if (error_message != nullptr) {
      *error_message = errstr;
    }
    return false;
  }
  return true;
}

bool KafkaProducer::ensure_ready_locked(std::string * error_message)
{
  if (!producer_) {
    if (error_message != nullptr) {
      *error_message = "Producer is not initialised";
    }
    return false;
  }

  const auto state = state_.load(std::memory_order_acquire);
  if (state == ProducerState::RUNNING || state == ProducerState::DEGRADED)
  {
    return true;
  }

  if (state == ProducerState::STARTING) {
    return true;
  }

  if (error_message != nullptr) {
    if (state == ProducerState::FAILED) {
      std::lock_guard<std::mutex> status_lock(status_mutex_);
      *error_message = last_error_.empty() ? "Producer failed to start" : last_error_;
    } else {
      *error_message = "Producer is not started";
    }
  }
  return false;
}

bool KafkaProducer::start(std::string * error_message)
{
  std::lock_guard<std::mutex> lock(mutex_);

  if (config_.bootstrap_servers.empty()) {
    if (error_message != nullptr) {
      *error_message = "bootstrap_servers is required";
    }
    update_state(ProducerState::FAILED);
    return false;
  }

  if (state_.load(std::memory_order_acquire) == ProducerState::RUNNING ||
    state_.load(std::memory_order_acquire) == ProducerState::DEGRADED)
  {
    return true;
  }

  update_state(ProducerState::STARTING);

  std::string errstr;
  std::unique_ptr<RdKafka::Conf> global_conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  if (!global_conf) {
    if (error_message != nullptr) {
      *error_message = "Failed to allocate librdkafka global configuration";
    }
    update_state(ProducerState::FAILED);
    return false;
  }

  if (global_conf->set("bootstrap.servers", config_.bootstrap_servers, errstr) !=
    RdKafka::Conf::CONF_OK)
  {
    if (error_message != nullptr) {
      *error_message = errstr;
    }
    update_state(ProducerState::FAILED);
    return false;
  }

  if (!config_.client_id.empty() &&
    global_conf->set("client.id", config_.client_id, errstr) != RdKafka::Conf::CONF_OK)
  {
    if (error_message != nullptr) {
      *error_message = errstr;
    }
    update_state(ProducerState::FAILED);
    return false;
  }

  if (global_conf->set("acks", config_.acks, errstr) != RdKafka::Conf::CONF_OK) {
    if (error_message != nullptr) {
      *error_message = errstr;
    }
    update_state(ProducerState::FAILED);
    return false;
  }

  if (!set_config_if_present(*global_conf, "linger.ms", config_.linger_ms, error_message)) {
    update_state(ProducerState::FAILED);
    return false;
  }
  if (!set_config_if_present(*global_conf, "batch.size", config_.batch_size, error_message)) {
    update_state(ProducerState::FAILED);
    return false;
  }
  if (!set_config_if_present(
      *global_conf, "queue.buffering.max.kbytes", config_.queue_buffering_max_kbytes,
      error_message))
  {
    update_state(ProducerState::FAILED);
    return false;
  }
  if (!set_config_if_present(*global_conf, "max.in.flight", config_.max_in_flight, error_message)) {
    update_state(ProducerState::FAILED);
    return false;
  }
  if (!set_config_if_present(
      *global_conf, "message.send.max.retries", config_.retries, error_message))
  {
    update_state(ProducerState::FAILED);
    return false;
  }
  if (!set_config_if_present(
      *global_conf, "retry.backoff.ms", config_.retry_backoff_ms, error_message))
  {
    update_state(ProducerState::FAILED);
    return false;
  }
  if (config_.max_pending_messages > 0 &&
    global_conf->set(
      "queue.buffering.max.messages", std::to_string(config_.max_pending_messages), errstr) !=
    RdKafka::Conf::CONF_OK)
  {
    if (error_message != nullptr) {
      *error_message = errstr;
    }
    update_state(ProducerState::FAILED);
    return false;
  }

  if (config_.enable_idempotence) {
    if (global_conf->set("enable.idempotence", "true", errstr) != RdKafka::Conf::CONF_OK) {
      if (error_message != nullptr) {
        *error_message = errstr;
      }
      update_state(ProducerState::FAILED);
      return false;
    }
  }

  delivery_cb_ = std::make_unique<DeliveryCallback>(this);
  if (global_conf->set("dr_cb", delivery_cb_.get(), errstr) != RdKafka::Conf::CONF_OK) {
    if (error_message != nullptr) {
      *error_message = errstr;
    }
    update_state(ProducerState::FAILED);
    return false;
  }

  std::unique_ptr<RdKafka::Producer> producer(RdKafka::Producer::create(global_conf.get(), errstr));
  if (!producer) {
    if (error_message != nullptr) {
      *error_message = errstr;
    }
    update_state(ProducerState::FAILED);
    return false;
  }

  {
    std::lock_guard<std::mutex> status_lock(status_mutex_);
    last_error_.clear();
  }

  // Validate connectivity if requested.
  auto next_state = ProducerState::RUNNING;
  RdKafka::Metadata * metadata{nullptr};
  const auto metadata_err = producer->metadata(false, nullptr, &metadata, kMetadataTimeoutMs);
  std::unique_ptr<RdKafka::Metadata> metadata_holder(metadata);
  if (metadata_err != RdKafka::ERR_NO_ERROR) {
    const auto metadata_error = RdKafka::err2str(metadata_err);
    {
      std::lock_guard<std::mutex> status_lock(status_mutex_);
      last_error_ = metadata_error;
    }
    if (config_.start_mode == StartMode::STRICT) {
      if (error_message != nullptr) {
        *error_message = metadata_error;
      }
      update_state(ProducerState::FAILED);
      return false;
    }
    next_state = ProducerState::DEGRADED;
  }

  producer_ = std::move(producer);
  stop_requested_.store(false, std::memory_order_release);
  poll_thread_ = std::thread([this]() {poll_loop();});
  update_state(next_state);
  return true;
}

void KafkaProducer::stop()
{
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!producer_) {
      state_.store(ProducerState::STOPPED, std::memory_order_release);
      return;
    }
    stop_requested_.store(true, std::memory_order_release);
  }

  if (poll_thread_.joinable()) {
    poll_thread_.join();
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (producer_) {
    producer_->flush(3000);
    producer_.reset();
  }
  delivery_cb_.reset();
  RdKafka::wait_destroyed(2000);
  update_state(ProducerState::STOPPED);
}

SendResult KafkaProducer::send(
  const std::string & topic,
  BufferView key,
  BufferView value,
  int64_t timestamp_ms,
  const std::vector<Header> & headers)
{
  std::lock_guard<std::mutex> lock(mutex_);
  SendResult result;
  if (!ensure_ready_locked(&result.error_message)) {
    return result;
  }

  std::unique_ptr<RdKafka::Headers> kafka_headers(RdKafka::Headers::create());
  for (const auto & header : headers) {
    kafka_headers->add(header.key, header.value);
  }

  const RdKafka::ErrorCode err = producer_->produce(
    topic,
    RdKafka::Topic::PARTITION_UA,
    RdKafka::Producer::RK_MSG_COPY,
    const_cast<uint8_t *>(value.data),
    value.size,
    key.data,
    key.size,
    timestamp_ms,
    kafka_headers.get(),
    nullptr);

  if (err == RdKafka::ERR__QUEUE_FULL) {
    kafka_headers.reset();
    dropped_queue_full_.fetch_add(1, std::memory_order_relaxed);
    result.dropped_queue_full = true;
    result.error_message = "librdkafka queue full";
    return result;
  }

  if (err != RdKafka::ERR_NO_ERROR) {
    kafka_headers.reset();
    send_errors_.fetch_add(1, std::memory_order_relaxed);
    const auto err_str = RdKafka::err2str(err);
    {
      std::lock_guard<std::mutex> status_lock(status_mutex_);
      last_error_ = err_str;
    }
    update_state(ProducerState::DEGRADED);
    result.error_message = err_str;
    return result;
  }

  kafka_headers.release();
  result.accepted = true;
  return result;
}

HealthStatus KafkaProducer::health() const
{
  HealthStatus status;
  status.state = state_.load(std::memory_order_acquire);
  status.sent_ok = sent_ok_.load(std::memory_order_acquire);
  status.dropped_queue_full = dropped_queue_full_.load(std::memory_order_acquire);
  status.send_errors = send_errors_.load(std::memory_order_acquire);
  {
    std::lock_guard<std::mutex> status_lock(status_mutex_);
    status.last_error = last_error_;
  }
  return status;
}

void KafkaProducer::update_state(ProducerState state)
{
  state_.store(state, std::memory_order_release);
}

void KafkaProducer::poll_loop()
{
  while (!stop_requested_.load(std::memory_order_acquire)) {
    RdKafka::Producer * producer = nullptr;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      producer = producer_.get();
    }
    if (producer == nullptr) {
      break;
    }
    producer->poll(kDefaultPollIntervalMs);
  }
}

void KafkaProducer::on_delivery_success()
{
  sent_ok_.fetch_add(1, std::memory_order_relaxed);
  if (state_.load(std::memory_order_acquire) == ProducerState::DEGRADED) {
    update_state(ProducerState::RUNNING);
  }
}

void KafkaProducer::on_delivery_failure(const std::string & error_message)
{
  send_errors_.fetch_add(1, std::memory_order_relaxed);
  {
    std::lock_guard<std::mutex> status_lock(status_mutex_);
    last_error_ = error_message;
  }
  update_state(ProducerState::DEGRADED);
}

}  // namespace kafka_client
