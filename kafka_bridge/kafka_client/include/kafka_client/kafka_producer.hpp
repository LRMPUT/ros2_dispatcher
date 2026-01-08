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

#ifndef KAFKA_CLIENT__KAFKA_PRODUCER_HPP_
#define KAFKA_CLIENT__KAFKA_PRODUCER_HPP_

#include <atomic>
#include <cstdint>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "kafka_client/visibility_control.hpp"

namespace RdKafka
{
class Producer;
}  // namespace RdKafka

namespace kafka_client
{

struct KafkaHeader
{
  std::string key;
  std::string value;
};

enum class StartupMode
{
  STRICT,
  TOLERANT
};

enum class ProducerStatus
{
  STOPPED,
  STARTING,
  RUNNING,
  DEGRADED,
  FAILED
};

enum class SendStatus
{
  SENT,
  QUEUE_FULL,
  PRODUCER_UNAVAILABLE,
  ERROR
};

struct KafkaProducerConfig
{
  std::string bootstrap_servers{"localhost:9092"};
  std::string client_id{"kafka_client"};
  std::string acks{"all"};

  std::optional<int> linger_ms;
  std::optional<int> batch_size;
  std::optional<int> queue_buffering_max_kbytes;
  std::optional<int> max_in_flight;
  std::optional<int> retries;
  std::optional<int> retry_backoff_ms;
  bool enable_idempotence{false};
  std::optional<int> stats_interval_ms;

  std::size_t max_queue_messages{1024};
  bool drop_when_full{true};
  StartupMode startup_mode{StartupMode::TOLERANT};

  int poll_timeout_ms{100};
  int reconnect_backoff_ms{1000};
  int flush_timeout_ms{3000};
};

struct KafkaSendResult
{
  SendStatus status{SendStatus::ERROR};
  std::string error_message;
  bool buffered{false};
};

struct ProducerHealth
{
  ProducerStatus status{ProducerStatus::STOPPED};
  std::string last_error;
  uint64_t sent_ok{0};
  uint64_t dropped_queue_full{0};
  uint64_t send_errors{0};
};

class KAFKA_CLIENT_PUBLIC KafkaProducer
{
public:
  explicit KafkaProducer(KafkaProducerConfig config);
  ~KafkaProducer();

  KafkaProducer(const KafkaProducer &) = delete;
  KafkaProducer & operator=(const KafkaProducer &) = delete;

  bool start();
  void stop();

  KafkaSendResult send(
    const std::string & topic,
    const std::vector<uint8_t> & key,
    const std::vector<uint8_t> & value,
    int64_t timestamp_ms,
    const std::vector<KafkaHeader> & headers);

  ProducerHealth health() const;

private:
  struct QueuedRecord
  {
    std::string topic;
    std::vector<uint8_t> key;
    std::vector<uint8_t> value;
    int64_t timestamp_ms{0};
    std::vector<KafkaHeader> headers;
  };

  bool create_producer_locked(std::string * error_message);
  KafkaSendResult try_produce_locked(const QueuedRecord & record);
  void poll_loop();
  void update_last_error_locked(const std::string & message);

  KafkaProducerConfig config_;

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::unique_ptr<RdKafka::Producer> producer_;
  std::unique_ptr<RdKafka::EventCb> event_cb_;
  std::deque<QueuedRecord> pending_records_;
  std::thread poll_thread_;
  std::atomic_bool running_{false};

  std::atomic<uint64_t> sent_ok_{0};
  std::atomic<uint64_t> dropped_queue_full_{0};
  std::atomic<uint64_t> send_errors_{0};

  ProducerStatus status_{ProducerStatus::STOPPED};
  std::string last_error_;
};

}  // namespace kafka_client

#endif  // KAFKA_CLIENT__KAFKA_PRODUCER_HPP_
