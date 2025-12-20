// Copyright 2025
// SPDX-License-Identifier: Apache-2.0

#ifndef KAFKA_CLIENT__KAFKA_PRODUCER_HPP_
#define KAFKA_CLIENT__KAFKA_PRODUCER_HPP_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <mutex>
#include <vector>

#include "kafka_client/visibility_control.hpp"

namespace RdKafka
{
class Producer;
class DeliveryReportCb;
class Conf;
}

namespace kafka_client
{

struct Header
{
  std::string key;
  std::string value;
};

struct BufferView
{
  const uint8_t * data{nullptr};
  std::size_t size{0};
};

enum class StartMode
{
  STRICT,
  TOLERANT
};

enum class ProducerState
{
  STOPPED,
  STARTING,
  RUNNING,
  DEGRADED,
  FAILED
};

struct ProducerConfig
{
  std::string bootstrap_servers;
  std::string client_id{"ros2-kafka-client"};
  std::string acks{"all"};
  std::optional<int> linger_ms;
  std::optional<int> batch_size;
  std::optional<int> queue_buffering_max_kbytes;
  std::optional<int> max_in_flight;
  std::optional<int> retries;
  std::optional<int> retry_backoff_ms;
  bool enable_idempotence{false};
  std::size_t max_pending_messages{0};
  StartMode start_mode{StartMode::STRICT};
};

struct SendResult
{
  bool accepted{false};
  bool dropped_queue_full{false};
  std::string error_message;
};

struct HealthStatus
{
  ProducerState state{ProducerState::STOPPED};
  std::string last_error;
  uint64_t sent_ok{0};
  uint64_t dropped_queue_full{0};
  uint64_t send_errors{0};
};

class KAFKA_CLIENT_PUBLIC KafkaProducer
{
public:
  explicit KafkaProducer(ProducerConfig config);
  ~KafkaProducer();

  KafkaProducer(const KafkaProducer &) = delete;
  KafkaProducer & operator=(const KafkaProducer &) = delete;

  bool start(std::string * error_message = nullptr);
  void stop();

  SendResult send(
    const std::string & topic,
    BufferView key,
    BufferView value,
    int64_t timestamp_ms,
    const std::vector<Header> & headers);

  HealthStatus health() const;

private:
  bool set_config_if_present(
    RdKafka::Conf & conf, const std::string & key, const std::optional<int> & value,
    std::string * error_message);
  bool ensure_ready_locked(std::string * error_message);
  void update_state(ProducerState state);
  void poll_loop();
  void on_delivery_success();
  void on_delivery_failure(const std::string & error_message);

  ProducerConfig config_;
  std::unique_ptr<RdKafka::Producer> producer_;
  std::unique_ptr<RdKafka::DeliveryReportCb> delivery_cb_;

  std::atomic<ProducerState> state_{ProducerState::STOPPED};
  std::atomic<uint64_t> sent_ok_{0};
  std::atomic<uint64_t> dropped_queue_full_{0};
  std::atomic<uint64_t> send_errors_{0};
  std::atomic<bool> stop_requested_{false};

  std::string last_error_;

  mutable std::mutex mutex_;
  mutable std::mutex status_mutex_;
  std::thread poll_thread_;
};

}  // namespace kafka_client

#endif  // KAFKA_CLIENT__KAFKA_PRODUCER_HPP_
