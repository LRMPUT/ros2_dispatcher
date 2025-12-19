#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "kafka_client/kafka_producer.hpp"

namespace
{
std::string now_header_value()
{
  using std::chrono::duration_cast;
  using std::chrono::milliseconds;
  using std::chrono::system_clock;
  const auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
  return std::to_string(now.count());
}
}

int main(int argc, char * argv[])
{
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <bootstrap_servers> <topic> [payload]" << std::endl;
    return 1;
  }

  const std::string bootstrap = argv[1];
  const std::string topic = argv[2];
  const std::string payload = (argc >= 4) ? argv[3] : "hello from kafka_client";

  kafka_client::ProducerConfig config;
  config.bootstrap_servers = bootstrap;
  config.client_id = "kafka_client_example";
  config.start_mode = kafka_client::StartMode::TOLERANT;  // keep retrying if broker is slow
  config.enable_idempotence = true;

  kafka_client::KafkaProducer producer(config);
  std::string start_error;
  if (!producer.start(&start_error)) {
    std::cerr << "Failed to start producer: " << start_error << std::endl;
    return 2;
  }

  const auto timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();

  const auto payload_bytes = reinterpret_cast<const uint8_t *>(payload.data());
  kafka_client::BufferView value{payload_bytes, payload.size()};

  std::vector<kafka_client::Header> headers;
  headers.push_back({"example", "true"});
  headers.push_back({"sent_at_ms", now_header_value()});

  auto result = producer.send(topic, kafka_client::BufferView{}, value, timestamp_ms, headers);
  if (!result.accepted) {
    std::cerr << "Send failed: " << result.error_message << std::endl;
    return 3;
  }

  // Give librdkafka time to deliver before shutdown.
  std::this_thread::sleep_for(std::chrono::seconds(1));
  producer.stop();
  std::cout << "Message enqueued successfully" << std::endl;
  return 0;
}
