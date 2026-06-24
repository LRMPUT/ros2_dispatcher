#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "rclcpp/rclcpp.hpp"

#define private public
#include "dispatcher_controller/dispatcher_controller_node.hpp"
#undef private

#include "gtest/gtest.h"

namespace
{

class DispatcherControllerNodeTest : public ::testing::Test
{
protected:
  static void SetUpTestSuite()
  {
    rclcpp::init(0, nullptr);
  }

  static void TearDownTestSuite()
  {
    rclcpp::shutdown();
  }

  std::shared_ptr<dispatcher_controller::DispatcherControllerNode> make_node(
    size_t max_topics = 200)
  {
    rclcpp::NodeOptions options;
    options.parameter_overrides({
      rclcpp::Parameter("auto_apply_on_mode_change", false),
      rclcpp::Parameter("selection_mode", "gui"),
      rclcpp::Parameter("all_mode_max_topics", static_cast<int>(max_topics)),
    });
    return std::make_shared<dispatcher_controller::DispatcherControllerNode>(options);
  }
};

TEST_F(DispatcherControllerNodeTest, ParseModeRecognizesSupportedValues)
{
  auto node = make_node();
  bool valid = false;

  EXPECT_EQ(node->parse_mode("gui", valid), dispatcher_controller::SelectionMode::GUI);
  EXPECT_TRUE(valid);
  EXPECT_EQ(node->parse_mode("file", valid), dispatcher_controller::SelectionMode::FILE);
  EXPECT_TRUE(valid);
  EXPECT_EQ(node->parse_mode("all", valid), dispatcher_controller::SelectionMode::ALL);
  EXPECT_TRUE(valid);

  EXPECT_EQ(node->parse_mode("bogus", valid), dispatcher_controller::SelectionMode::GUI);
  EXPECT_FALSE(valid);
}

TEST_F(DispatcherControllerNodeTest, TopicsToYamlSerializesExpectedFields)
{
  auto node = make_node();
  introspection_manager_msgs::msg::TopicInfo topic_a;
  topic_a.name = "/demo/chatter";
  topic_a.type = "std_msgs/msg/String";
  introspection_manager_msgs::msg::TopicInfo topic_b;
  topic_b.name = "/demo/number";
  topic_b.type = "std_msgs/msg/Int32";

  auto yaml = node->topics_to_yaml({topic_a, topic_b});

  EXPECT_NE(yaml.find("topic_name: /demo/chatter"), std::string::npos);
  EXPECT_NE(yaml.find("msg_type: std_msgs/msg/String"), std::string::npos);
  EXPECT_NE(yaml.find("topic_name: /demo/number"), std::string::npos);
  EXPECT_NE(yaml.find("msg_type: std_msgs/msg/Int32"), std::string::npos);
}

TEST_F(DispatcherControllerNodeTest, LoadFileSelectionParsesTopicToolsAndThrottleAlias)
{
  auto node = make_node();
  auto file = std::filesystem::temp_directory_path() / "dispatcher_selection_test.yaml";
  std::ofstream out(file);
  out << R"(
- topic_name: /demo/cmd_vel
  msg_type: geometry_msgs/msg/Twist
  topic_tools:
    plugin: topic_tools::ThrottleNode
    output_name: throttled
    parameters:
      throttle_type: messages
      throttle_rate: 5
)";
  out.close();

  std::vector<dispatcher_controller::TopicSelection> selections;
  std::string error;
  ASSERT_TRUE(node->load_file_selection(file.string(), selections, error)) << error;
  ASSERT_EQ(selections.size(), 1u);
  ASSERT_TRUE(selections[0].topic_tools.has_value());
  EXPECT_EQ(selections[0].topic_tools->plugin_name, "topic_tools::ThrottleNode");
  EXPECT_EQ(selections[0].topic_tools->output_topic, "/throttled/demo/cmd_vel");

  std::vector<std::string> parameter_names;
  for (const auto & param : selections[0].topic_tools->parameters) {
    parameter_names.push_back(param.get_name());
  }
  EXPECT_NE(
    std::find(parameter_names.begin(), parameter_names.end(), "msgs_per_sec"),
    parameter_names.end());
  EXPECT_EQ(
    std::find(parameter_names.begin(), parameter_names.end(), "throttle_rate"),
    parameter_names.end());

  std::filesystem::remove(file);
}

TEST_F(DispatcherControllerNodeTest, LoadFileSelectionRejectsMissingTopicName)
{
  auto node = make_node();
  auto file = std::filesystem::temp_directory_path() / "dispatcher_selection_invalid.yaml";
  std::ofstream out(file);
  out << R"(
- msg_type: std_msgs/msg/String
)";
  out.close();

  std::vector<dispatcher_controller::TopicSelection> selections;
  std::string error;
  EXPECT_FALSE(node->load_file_selection(file.string(), selections, error));
  EXPECT_NE(error.find("missing topic_name"), std::string::npos);

  std::filesystem::remove(file);
}

TEST_F(DispatcherControllerNodeTest, EnsureTopicLimitsRejectsOversizedSelection)
{
  auto node = make_node(1);
  dispatcher_controller::TopicSelection first;
  first.topic.name = "/a";
  dispatcher_controller::TopicSelection second;
  second.topic.name = "/b";

  std::string error;
  EXPECT_FALSE(node->ensure_topic_limits({first, second}, error));
  EXPECT_NE(error.find("safety limit"), std::string::npos);
}

}  // namespace
