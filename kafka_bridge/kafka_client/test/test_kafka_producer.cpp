#include <cstdint>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "kafka_client/kafka_producer.hpp"

namespace
{

std::vector<uint8_t> bytes_from_string(const std::string & value)
{
  return std::vector<uint8_t>(value.begin(), value.end());
}

}  // namespace

TEST(KafkaProducerTest, SendBeforeStartReturnsUnavailable)
{
  kafka_client::KafkaProducer producer(kafka_client::KafkaProducerConfig{});

  auto result = producer.send(
    "ros2.test.topic",
    bytes_from_string("key"),
    bytes_from_string("value"),
    123,
    {});

  EXPECT_EQ(result.status, kafka_client::SendStatus::PRODUCER_UNAVAILABLE);
  EXPECT_FALSE(result.buffered);
  EXPECT_EQ(producer.health().status, kafka_client::ProducerStatus::STOPPED);
}

TEST(KafkaProducerTest, StrictStartFailsForInvalidConfig)
{
  kafka_client::KafkaProducerConfig config;
  config.acks = "definitely-invalid-acks";
  config.startup_mode = kafka_client::StartupMode::STRICT;

  kafka_client::KafkaProducer producer(config);

  EXPECT_FALSE(producer.start());

  auto health = producer.health();
  EXPECT_EQ(health.status, kafka_client::ProducerStatus::FAILED);
  EXPECT_FALSE(health.last_error.empty());
}

TEST(KafkaProducerTest, TolerantStartBuffersAndDropsWhenPendingQueueFills)
{
  kafka_client::KafkaProducerConfig config;
  config.acks = "definitely-invalid-acks";
  config.startup_mode = kafka_client::StartupMode::TOLERANT;
  config.max_queue_messages = 1;
  config.drop_when_full = true;

  kafka_client::KafkaProducer producer(config);
  ASSERT_TRUE(producer.start());

  auto first = producer.send(
    "ros2.test.topic",
    bytes_from_string("key1"),
    bytes_from_string("value1"),
    123,
    {});
  EXPECT_EQ(first.status, kafka_client::SendStatus::QUEUE_FULL);
  EXPECT_TRUE(first.buffered);

  auto second = producer.send(
    "ros2.test.topic",
    bytes_from_string("key2"),
    bytes_from_string("value2"),
    456,
    {});
  EXPECT_EQ(second.status, kafka_client::SendStatus::QUEUE_FULL);
  EXPECT_FALSE(second.buffered);

  auto health = producer.health();
  EXPECT_EQ(health.status, kafka_client::ProducerStatus::DEGRADED);
  EXPECT_FALSE(health.last_error.empty());
  EXPECT_EQ(health.dropped_queue_full, 1u);

  producer.stop();
  EXPECT_EQ(producer.health().status, kafka_client::ProducerStatus::STOPPED);
}
