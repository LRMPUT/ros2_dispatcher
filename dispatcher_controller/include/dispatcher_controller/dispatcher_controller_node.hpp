#pragma once

#include <chrono>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "builtin_interfaces/msg/time.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp_components/register_node_macro.hpp"
#include "rclcpp_lifecycle/lifecycle_node.hpp"
#include "rcl_interfaces/srv/set_parameters.hpp"
#include "lifecycle_msgs/srv/change_state.hpp"
#include "lifecycle_msgs/srv/get_state.hpp"

#include "introspection_manager_msgs/msg/topic_info.hpp"
#include "introspection_manager_msgs/srv/get_topics.hpp"

#include "dispatcher_controller/srv/apply_selection.hpp"
#include "dispatcher_controller/srv/reload_selection.hpp"
#include "dispatcher_controller/srv/stop_streaming.hpp"
#include "dispatcher_controller/srv/get_status.hpp"
#include "dispatcher_controller/srv/set_selection_mode.hpp"

namespace dispatcher_controller
{

enum class ControllerPhase
{
  IDLE,
  BUSY,
  ERROR,
};

enum class SelectionMode
{
  GUI,
  FILE,
  ALL,
};

struct SelectionSnapshot
{
  std::vector<introspection_manager_msgs::msg::TopicInfo> topics;
  rclcpp::Time timestamp{0, 0, RCL_SYSTEM_TIME};
};

class DispatcherControllerNode : public rclcpp::Node
{
public:
  explicit DispatcherControllerNode(const rclcpp::NodeOptions & options = rclcpp::NodeOptions());

private:
  // Service handlers
  void handle_apply_selection(
    const dispatcher_controller::srv::ApplySelection::Request::SharedPtr request,
    dispatcher_controller::srv::ApplySelection::Response::SharedPtr response);
  void handle_reload_selection(
    const dispatcher_controller::srv::ReloadSelection::Request::SharedPtr request,
    dispatcher_controller::srv::ReloadSelection::Response::SharedPtr response);
  void handle_stop_streaming(
    const dispatcher_controller::srv::StopStreaming::Request::SharedPtr request,
    dispatcher_controller::srv::StopStreaming::Response::SharedPtr response);
  void handle_get_status(
    const dispatcher_controller::srv::GetStatus::Request::SharedPtr request,
    dispatcher_controller::srv::GetStatus::Response::SharedPtr response);
  void handle_set_selection_mode(
    const dispatcher_controller::srv::SetSelectionMode::Request::SharedPtr request,
    dispatcher_controller::srv::SetSelectionMode::Response::SharedPtr response);

  // Parameter callback
  rcl_interfaces::msg::SetParametersResult on_param_change(
    const std::vector<rclcpp::Parameter> & parameters);

  // Core operations
  bool switch_mode(SelectionMode new_mode, const std::string & file_path, bool apply_now,
    std::string & error_out);
  bool apply_selection(const std::vector<introspection_manager_msgs::msg::TopicInfo> & topics,
    std::string & error_out);
  bool apply_selection_to_sink(
    const std::string & sink_label,
    const std::string & sink_node_name,
    const rclcpp::Client<lifecycle_msgs::srv::ChangeState>::SharedPtr & change_state_client,
    const rclcpp::Client<lifecycle_msgs::srv::GetState>::SharedPtr & get_state_client,
    const rclcpp::Client<rcl_interfaces::srv::SetParameters>::SharedPtr & set_parameters_client,
    const std::vector<introspection_manager_msgs::msg::TopicInfo> & subs,
    std::string & error_out);
  bool deactivate_sink(
    const std::string & sink_label,
    const std::string & sink_node_name,
    const rclcpp::Client<lifecycle_msgs::srv::ChangeState>::SharedPtr & change_state_client,
    const rclcpp::Client<lifecycle_msgs::srv::GetState>::SharedPtr & get_state_client,
    std::string & error_out);
  std::optional<uint8_t> get_sink_state(
    const std::string & sink_label,
    const rclcpp::Client<lifecycle_msgs::srv::GetState>::SharedPtr & get_state_client);
  bool change_sink_state(
    const std::string & sink_label,
    const rclcpp::Client<lifecycle_msgs::srv::ChangeState>::SharedPtr & change_state_client,
    uint8_t transition_id, const std::string & action, std::string & error_out);
  bool set_sink_subscriptions_yaml(
    const std::string & sink_label,
    const rclcpp::Client<rcl_interfaces::srv::SetParameters>::SharedPtr & set_parameters_client,
    const std::vector<introspection_manager_msgs::msg::TopicInfo> & subs,
    std::string & error_out);
  bool should_skip_unavailable_sink(const std::string & sink_label, std::string & error_out) const;

  // Selection helpers
  bool load_file_selection(const std::string & path,
    std::vector<introspection_manager_msgs::msg::TopicInfo> & out, std::string & error_out);
  bool discover_all_topics(std::vector<introspection_manager_msgs::msg::TopicInfo> & out,
    std::string & error_out);
  bool infer_missing_types(
    std::vector<introspection_manager_msgs::msg::TopicInfo> & subs, std::string & error_out);
  bool ensure_topic_limits(const std::vector<introspection_manager_msgs::msg::TopicInfo> & subs,
    std::string & error_out) const;

  bool set_introspection_enabled(bool enabled, std::string & error_out);
  std::string topics_to_yaml(
    const std::vector<introspection_manager_msgs::msg::TopicInfo> & subs) const;
  SelectionMode parse_mode(const std::string & value, bool & valid) const;
  std::string mode_to_string(SelectionMode mode) const;
  std::string state_string(uint8_t state) const;

  // Members
  rclcpp::Service<dispatcher_controller::srv::ApplySelection>::SharedPtr apply_srv_;
  rclcpp::Service<dispatcher_controller::srv::ReloadSelection>::SharedPtr reload_srv_;
  rclcpp::Service<dispatcher_controller::srv::StopStreaming>::SharedPtr stop_srv_;
  rclcpp::Service<dispatcher_controller::srv::GetStatus>::SharedPtr status_srv_;
  rclcpp::Service<dispatcher_controller::srv::SetSelectionMode>::SharedPtr mode_srv_;

  rclcpp::Client<lifecycle_msgs::srv::ChangeState>::SharedPtr change_state_client_;
  rclcpp::Client<lifecycle_msgs::srv::GetState>::SharedPtr get_state_client_;
  rclcpp::Client<rcl_interfaces::srv::SetParameters>::SharedPtr set_parameters_client_;
  rclcpp::Client<lifecycle_msgs::srv::ChangeState>::SharedPtr mosquitto_change_state_client_;
  rclcpp::Client<lifecycle_msgs::srv::GetState>::SharedPtr mosquitto_get_state_client_;
  rclcpp::Client<rcl_interfaces::srv::SetParameters>::SharedPtr mosquitto_set_parameters_client_;
  rclcpp::Client<introspection_manager_msgs::srv::GetTopics>::SharedPtr introspection_client_;
  rclcpp::Client<rcl_interfaces::srv::SetParameters>::SharedPtr introspection_param_client_;

  rclcpp::CallbackGroup::SharedPtr client_cb_group_;

  OnSetParametersCallbackHandle::SharedPtr param_cb_handle_;

  // Parameters
  std::string kafka_sink_node_name_;
  std::string mosquitto_sink_node_name_;
  std::string introspection_service_name_;
  std::string introspection_node_name_;
  bool validate_topics_;
  bool disable_introspection_after_apply_;
  std::string selection_file_path_;
  bool auto_apply_on_mode_change_;
  bool allow_missing_sinks_;
  size_t all_mode_max_topics_;
  std::vector<std::string> all_mode_allowlist_;
  std::vector<std::string> all_mode_denylist_;
  bool all_mode_hide_rosout_;

  // State
  SelectionMode selection_mode_{SelectionMode::GUI};
  SelectionSnapshot last_gui_selection_;
  SelectionSnapshot last_file_selection_;
  SelectionSnapshot last_all_selection_;
  SelectionSnapshot applied_selection_;
  ControllerPhase phase_{ControllerPhase::IDLE};
  std::string last_error_;
  builtin_interfaces::msg::Time last_error_stamp_;

  std::chrono::milliseconds service_timeout_{3000};
  std::mutex mutex_;
};

}  // namespace dispatcher_controller
