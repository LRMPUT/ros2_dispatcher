#include "dispatcher_controller/dispatcher_controller_node.hpp"

#include <algorithm>
#include <fstream>
#include <set>
#include <sstream>
#include <cstdint>

#include "yaml-cpp/yaml.h"
#include <rclcpp/logging.hpp>

namespace dispatcher_controller
{

using namespace std::chrono_literals;

namespace
{

std::string sanitize_topic_segment(const std::string & topic)
{
  std::string sanitized = topic;
  std::replace(sanitized.begin(), sanitized.end(), '/', '_');
  while (!sanitized.empty() && sanitized.front() == '_') {
    sanitized.erase(sanitized.begin());
  }
  if (sanitized.empty()) {
    sanitized = "topic";
  }
  return sanitized;
}

std::string default_tool_node_name(const std::string & plugin_name, const std::string & topic_name)
{
  std::string base = plugin_name;
  auto pos = base.find_last_of("::");
  if (pos != std::string::npos && pos + 1 < base.size()) {
    base = base.substr(pos + 1);
  }
  return sanitize_topic_segment(base + "_" + topic_name);
}

std::string default_output_topic(const std::string & input_topic, const std::string & node_name)
{
  std::string prefix = node_name.empty() ? std::string("topic_tools") : node_name;
  std::string normalized = input_topic;
  if (!normalized.empty() && normalized.front() == '/') {
    normalized.erase(normalized.begin());
  }
  if (normalized.empty()) {
    return "/" + prefix;
  }
  return "/" + prefix + "/" + normalized;
}

rclcpp::Parameter parse_parameter_value(
  const std::string & name, const YAML::Node & value, std::string & error_out)
{
  if (value.IsScalar()) {
    try {
      return rclcpp::Parameter(name, value.as<bool>());
    } catch (...) {
    }
    try {
      return rclcpp::Parameter(name, value.as<int64_t>());
    } catch (...) {
    }
    try {
      return rclcpp::Parameter(name, value.as<double>());
    } catch (...) {
    }
    try {
      return rclcpp::Parameter(name, value.as<std::string>());
    } catch (...) {
    }
  } else if (value.IsSequence()) {
    try {
      std::vector<bool> vals;
      vals.reserve(value.size());
      for (const auto & item : value) {
        vals.push_back(item.as<bool>());
      }
      return rclcpp::Parameter(name, vals);
    } catch (...) {
    }

    try {
      std::vector<int64_t> vals;
      vals.reserve(value.size());
      for (const auto & item : value) {
        vals.push_back(item.as<int64_t>());
      }
      return rclcpp::Parameter(name, vals);
    } catch (...) {
    }

    try {
      std::vector<double> vals;
      vals.reserve(value.size());
      for (const auto & item : value) {
        vals.push_back(item.as<double>());
      }
      return rclcpp::Parameter(name, vals);
    } catch (...) {
    }

    try {
      std::vector<std::string> vals;
      vals.reserve(value.size());
      for (const auto & item : value) {
        vals.push_back(item.as<std::string>());
      }
      return rclcpp::Parameter(name, vals);
    } catch (...) {
    }
  }

  error_out = "Unsupported parameter value for " + name;
  throw std::runtime_error(error_out);
}

}  // namespace

DispatcherControllerNode::DispatcherControllerNode(const rclcpp::NodeOptions & options)
: rclcpp::Node("dispatcher_controller", options)
{
  client_cb_group_ = create_callback_group(rclcpp::CallbackGroupType::Reentrant);

  kafka_sink_node_name_ = declare_parameter<std::string>("kafka_sink_node_name", "/kafka_sink");
  introspection_service_name_ = declare_parameter<std::string>(
    "introspection_service_name", "/introspection_manager/get_topics");
  introspection_node_name_ =
    declare_parameter<std::string>("introspection_node_name", "/introspection_manager");
  validate_topics_ = declare_parameter<bool>("validate_topics", false);
  disable_introspection_after_apply_ =
    declare_parameter<bool>("disable_introspection_after_apply", true);
  selection_file_path_ = declare_parameter<std::string>("selection_file_path", "");
  auto_apply_on_mode_change_ = declare_parameter<bool>("auto_apply_on_mode_change", true);
  all_mode_max_topics_ = declare_parameter<int>("all_mode_max_topics", 200);
  all_mode_allowlist_ = declare_parameter<std::vector<std::string>>(
    "all_mode_allowlist", std::vector<std::string>{});
  all_mode_denylist_ = declare_parameter<std::vector<std::string>>(
    "all_mode_denylist", std::vector<std::string>{});
  all_mode_hide_rosout_ = declare_parameter<bool>("all_mode_hide_rosout", true);
  component_container_name_ = declare_parameter<std::string>(
    "component_container_name", "/ros2_kafka_dispatcher_container");

  bool valid_mode{true};
  selection_mode_ = parse_mode(
    declare_parameter<std::string>("selection_mode", "gui"), valid_mode);
  if (!valid_mode) {
    selection_mode_ = SelectionMode::GUI;
  }

  change_state_client_ = create_client<lifecycle_msgs::srv::ChangeState>(
    kafka_sink_node_name_ + "/change_state", rmw_qos_profile_services_default, client_cb_group_);
  get_state_client_ =
    create_client<lifecycle_msgs::srv::GetState>(
    kafka_sink_node_name_ + "/get_state", rmw_qos_profile_services_default, client_cb_group_);
  set_parameters_client_ = create_client<rcl_interfaces::srv::SetParameters>(
    kafka_sink_node_name_ + "/set_parameters", rmw_qos_profile_services_default, client_cb_group_);
  introspection_client_ = create_client<introspection_manager_msgs::srv::GetTopics>(
    introspection_service_name_, rmw_qos_profile_services_default, client_cb_group_);
  introspection_param_client_ = create_client<rcl_interfaces::srv::SetParameters>(
    introspection_node_name_ + "/set_parameters", rmw_qos_profile_services_default, client_cb_group_);
  load_node_client_ = create_client<composition_interfaces::srv::LoadNode>(
    component_container_name_ + "/load_node", rmw_qos_profile_services_default, client_cb_group_);
  unload_node_client_ = create_client<composition_interfaces::srv::UnloadNode>(
    component_container_name_ + "/unload_node", rmw_qos_profile_services_default, client_cb_group_);

  apply_srv_ = create_service<dispatcher_controller::srv::ApplySelection>(
    "apply_selection",
    std::bind(
      &DispatcherControllerNode::handle_apply_selection, this, std::placeholders::_1,
      std::placeholders::_2));
  reload_srv_ = create_service<dispatcher_controller::srv::ReloadSelection>(
    "reload_selection",
    std::bind(
      &DispatcherControllerNode::handle_reload_selection, this, std::placeholders::_1,
      std::placeholders::_2));
  stop_srv_ = create_service<dispatcher_controller::srv::StopStreaming>(
    "stop_streaming",
    std::bind(
      &DispatcherControllerNode::handle_stop_streaming, this, std::placeholders::_1,
      std::placeholders::_2));
  status_srv_ = create_service<dispatcher_controller::srv::GetStatus>(
    "get_status",
    std::bind(
      &DispatcherControllerNode::handle_get_status, this, std::placeholders::_1,
      std::placeholders::_2));
  mode_srv_ = create_service<dispatcher_controller::srv::SetSelectionMode>(
    "set_selection_mode",
    std::bind(
      &DispatcherControllerNode::handle_set_selection_mode, this, std::placeholders::_1,
      std::placeholders::_2));

  param_cb_handle_ = add_on_set_parameters_callback(
    std::bind(&DispatcherControllerNode::on_param_change, this, std::placeholders::_1));

  RCLCPP_INFO(
    get_logger(), "dispatcher_controller started in mode [%s], kafka_sink [%s]",
    mode_to_string(selection_mode_).c_str(), kafka_sink_node_name_.c_str());
}

rcl_interfaces::msg::SetParametersResult DispatcherControllerNode::on_param_change(
  const std::vector<rclcpp::Parameter> & parameters)
{
  std::lock_guard<std::mutex> lock(mutex_);
  auto result = rcl_interfaces::msg::SetParametersResult();
  result.successful = true;
  std::optional<SelectionMode> new_mode;
  std::string new_file_path = selection_file_path_;
  bool request_apply = auto_apply_on_mode_change_;
  std::optional<bool> new_auto_apply;

  for (const auto & param : parameters) {
    if (param.get_name() == "selection_mode") {
      bool valid{true};
      new_mode = parse_mode(param.as_string(), valid);
      if (!valid) {
        result.successful = false;
        result.reason = "Invalid selection_mode value";
        return result;
      }
    } else if (param.get_name() == "selection_file_path") {
      new_file_path = param.as_string();
    } else if (param.get_name() == "auto_apply_on_mode_change") {
      request_apply = param.as_bool();
      new_auto_apply = request_apply;
    }
  }

  if (new_mode) {
    std::string error;
    if (!switch_mode(*new_mode, new_file_path, request_apply, error)) {
      result.successful = false;
      result.reason = error;
      return result;
    }
    selection_file_path_ = new_file_path;
    if (new_auto_apply.has_value()) {
      auto_apply_on_mode_change_ = new_auto_apply.value();
    }
  }

  return result;
}

void DispatcherControllerNode::handle_set_selection_mode(
  const dispatcher_controller::srv::SetSelectionMode::Request::SharedPtr request,
  dispatcher_controller::srv::SetSelectionMode::Response::SharedPtr response)
{
  std::lock_guard<std::mutex> lock(mutex_);
  bool valid{true};
  auto mode = parse_mode(request->selection_mode, valid);
  if (!valid) {
    response->success = false;
    response->message = "Invalid selection_mode. Use gui|file|all.";
    return;
  }

  std::string error;
  if (!switch_mode(mode, request->selection_file_path, request->apply_now, error)) {
    response->success = false;
    response->message = error;
    return;
  }

  selection_file_path_ = request->selection_file_path.empty() ?
    selection_file_path_ : request->selection_file_path;
  response->success = true;
  response->message = "Mode switched to " + mode_to_string(mode);
}

void DispatcherControllerNode::handle_apply_selection(
  const dispatcher_controller::srv::ApplySelection::Request::SharedPtr request,
  dispatcher_controller::srv::ApplySelection::Response::SharedPtr response)
{
  // echo request
  RCLCPP_DEBUG(
    get_logger(), "ApplySelection request with %zu topics", request->topics.size());
  for (const auto & topic : request->topics) {
    RCLCPP_DEBUG(
      get_logger(), "  - %s (%s)", topic.name.c_str(),
      topic.type.empty() ? "unknown_type" : topic.type.c_str());
  }
  std::lock_guard<std::mutex> lock(mutex_);
  if (phase_ == ControllerPhase::BUSY) {
    response->success = false;
    response->message = "Controller busy";
    return;
  }
  if (selection_mode_ != SelectionMode::GUI) {
    response->success = false;
    response->message = "Not in gui mode";
    return;
  }

  std::vector<TopicSelection> topics;
  topics.reserve(request->topics.size());
  for (const auto & topic : request->topics) {
    TopicSelection selection;
    selection.topic = topic;
    topics.push_back(selection);
  }
  std::string error;
  if (!infer_missing_types(topics, error)) {
    response->success = false;
    response->message = "Failed to infer topic types: " + error;
    last_error_ = response->message;
    last_error_stamp_ = now();
    return;
  }
  if (!ensure_topic_limits(topics, error)) {
    response->success = false;
    response->message = error;
    last_error_ = error;
    last_error_stamp_ = now();
    return;
  }

  phase_ = ControllerPhase::BUSY;
  if (!apply_selection(topics, error)) {
    phase_ = ControllerPhase::ERROR;
    response->success = false;
    response->message = error;
    last_error_ = error;
    last_error_stamp_ = now();
    return;
  }

  last_gui_selection_.topics = topics;
  last_gui_selection_.sink_topics = applied_selection_.sink_topics;
  last_gui_selection_.timestamp = now();
  phase_ = ControllerPhase::IDLE;
  response->success = true;
  response->message = "Selection applied";
}

void DispatcherControllerNode::handle_reload_selection(
  const dispatcher_controller::srv::ReloadSelection::Request::SharedPtr request,
  dispatcher_controller::srv::ReloadSelection::Response::SharedPtr response)
{
  std::lock_guard<std::mutex> lock(mutex_);
  if (phase_ == ControllerPhase::BUSY) {
    response->success = false;
    response->message = "Controller busy";
    return;
  }

  std::vector<TopicSelection> selection;
  std::string error;
  bool apply_now = request->apply_now;
  TopicToolsPlan plan;

  if (selection_mode_ == SelectionMode::FILE) {
    std::string path = request->selection_file_path.empty() ? selection_file_path_ :
      request->selection_file_path;
    if (!load_file_selection(path, selection, error)) {
      response->success = false;
      response->message = error;
      last_error_ = error;
      last_error_stamp_ = now();
      return;
    }
    if (!infer_missing_types(selection, error)) {
      response->success = false;
      response->message = "Failed to infer types: " + error;
      last_error_ = error;
      last_error_stamp_ = now();
      return;
    }
    if (!build_topic_tools_plan(selection, plan, error)) {
      response->success = false;
      response->message = error;
      last_error_ = error;
      last_error_stamp_ = now();
      return;
    }
    last_file_selection_.topics = selection;
    last_file_selection_.sink_topics = plan.sink_topics;
    last_file_selection_.timestamp = now();
  } else if (selection_mode_ == SelectionMode::ALL) {
    if (!discover_all_topics(selection, error)) {
      response->success = false;
      response->message = error;
      last_error_ = error;
      last_error_stamp_ = now();
      return;
    }
    if (!build_topic_tools_plan(selection, plan, error)) {
      response->success = false;
      response->message = error;
      last_error_ = error;
      last_error_stamp_ = now();
      return;
    }
    last_all_selection_.topics = selection;
    last_all_selection_.sink_topics = plan.sink_topics;
    last_all_selection_.timestamp = now();
  } else {  // GUI mode
    if (last_gui_selection_.topics.empty()) {
      response->success = false;
      response->message = "No cached GUI selection to reload";
      return;
    }
    selection = last_gui_selection_.topics;
    plan.sink_topics = last_gui_selection_.sink_topics;
    if (request->selection_file_path.size() > 0) {
      RCLCPP_WARN(get_logger(), "selection_file_path ignored in gui mode reload");
    }
  }

  if (plan.sink_topics.empty() && !selection.empty()) {
    if (!build_topic_tools_plan(selection, plan, error)) {
      response->success = false;
      response->message = error;
      last_error_ = error;
      last_error_stamp_ = now();
      return;
    }
  }

  if (!ensure_topic_limits(selection, error)) {
    response->success = false;
    response->message = error;
    last_error_ = error;
    last_error_stamp_ = now();
    return;
  }

  if (apply_now) {
    phase_ = ControllerPhase::BUSY;
    if (!apply_selection(selection, error)) {
      phase_ = ControllerPhase::ERROR;
      response->success = false;
      response->message = error;
      last_error_ = error;
      last_error_stamp_ = now();
      return;
    }
    phase_ = ControllerPhase::IDLE;
  }

  response->success = true;
  response->message = apply_now ? "Selection reloaded and applied" : "Selection reloaded";
}

void DispatcherControllerNode::handle_stop_streaming(
  const dispatcher_controller::srv::StopStreaming::Request::SharedPtr request,
  dispatcher_controller::srv::StopStreaming::Response::SharedPtr response)
{
  std::lock_guard<std::mutex> lock(mutex_);
  if (phase_ == ControllerPhase::BUSY) {
    response->success = false;
    response->message = "Controller busy";
    return;
  }

  std::string error;
  if (!deactivate_kafka_sink(error)) {
    phase_ = ControllerPhase::ERROR;
    last_error_ = error;
    last_error_stamp_ = now();
    response->success = false;
    response->message = error;
    return;
  }

  if (!clear_active_topic_tools(error)) {
    phase_ = ControllerPhase::ERROR;
    last_error_ = error;
    last_error_stamp_ = now();
    response->success = false;
    response->message = error;
    return;
  }

  if (request->reset_cached) {
    last_gui_selection_ = SelectionSnapshot{};
    last_file_selection_ = SelectionSnapshot{};
    last_all_selection_ = SelectionSnapshot{};
  }

  applied_selection_ = SelectionSnapshot{};
  phase_ = ControllerPhase::IDLE;
  response->success = true;
  response->message = "Streaming stopped";
}

void DispatcherControllerNode::handle_get_status(
  const dispatcher_controller::srv::GetStatus::Request::SharedPtr /*request*/,
  dispatcher_controller::srv::GetStatus::Response::SharedPtr response)
{
  std::lock_guard<std::mutex> lock(mutex_);
  response->selection_mode = mode_to_string(selection_mode_);
  auto state = get_kafka_sink_state();
  response->kafka_sink_state = state ? (state_string(*state)) : "unknown";
  response->streaming_active = state &&
    *state == lifecycle_msgs::msg::State::PRIMARY_STATE_ACTIVE;
  response->applied_topics = applied_selection_.sink_topics.empty() ?
    to_topic_info(applied_selection_.topics) : applied_selection_.sink_topics;
  response->gui_selection_count = static_cast<uint32_t>(last_gui_selection_.topics.size());
  response->file_selection_count = static_cast<uint32_t>(last_file_selection_.topics.size());
  response->all_selection_count = static_cast<uint32_t>(last_all_selection_.topics.size());
  response->last_error = last_error_;
  response->last_error_stamp = last_error_stamp_;
  response->reconciling = phase_ == ControllerPhase::BUSY;
  response->success = true;
  response->message = "OK";
}

bool DispatcherControllerNode::switch_mode(
  SelectionMode new_mode, const std::string & file_path, bool apply_now, std::string & error_out)
{
  if (phase_ == ControllerPhase::BUSY) {
    error_out = "Controller busy";
    return false;
  }

  selection_mode_ = new_mode;
  if (!file_path.empty()) {
    selection_file_path_ = file_path;
  }

  std::vector<TopicSelection> selection;
  TopicToolsPlan plan;

  if (apply_now) {
    if (new_mode == SelectionMode::GUI) {
      if (!last_gui_selection_.topics.empty()) {
        selection = last_gui_selection_.topics;
        plan.sink_topics = last_gui_selection_.sink_topics;
      } else {
        RCLCPP_INFO(
          get_logger(), "Switched to gui mode without cached selection; idle until ApplySelection");
        return true;
      }
    } else if (new_mode == SelectionMode::FILE) {
      if (selection_file_path_.empty()) {
        error_out = "selection_file_path is empty";
        return false;
      }
      if (!load_file_selection(selection_file_path_, selection, error_out)) {
        return false;
      }
      if (!infer_missing_types(selection, error_out)) {
        return false;
      }
      if (!build_topic_tools_plan(selection, plan, error_out)) {
        return false;
      }
      last_file_selection_.topics = selection;
      last_file_selection_.sink_topics = plan.sink_topics;
      last_file_selection_.timestamp = now();
    } else {  // ALL
      if (!discover_all_topics(selection, error_out)) {
        return false;
      }
      if (!build_topic_tools_plan(selection, plan, error_out)) {
        return false;
      }
      last_all_selection_.topics = selection;
      last_all_selection_.sink_topics = plan.sink_topics;
      last_all_selection_.timestamp = now();
    }

    if (plan.sink_topics.empty() && !selection.empty()) {
      if (!build_topic_tools_plan(selection, plan, error_out)) {
        return false;
      }
    }

    if (!ensure_topic_limits(selection, error_out)) {
      return false;
    }

    phase_ = ControllerPhase::BUSY;
    if (!apply_selection(selection, error_out)) {
      phase_ = ControllerPhase::ERROR;
      return false;
    }
    phase_ = ControllerPhase::IDLE;
  }

  RCLCPP_INFO(
    get_logger(), "Switched to mode [%s]%s", mode_to_string(new_mode).c_str(),
    apply_now ? " and applied selection" : "");
  return true;
}

bool DispatcherControllerNode::apply_selection(
  const std::vector<TopicSelection> & topics, std::string & error_out)
{
  if (topics.empty()) {
    error_out = "No topics to apply";
    return false;
  }

  auto state = get_kafka_sink_state();
  if (!state) {
    error_out = "Unable to get kafka_sink state";
    return false;
  }

  TopicToolsPlan plan;
  std::vector<TopicSelection> validated_topics = topics;
  if (validate_topics_) {
    if (!infer_missing_types(validated_topics, error_out)) {
      return false;
    }
  }

  if (!build_topic_tools_plan(validated_topics, plan, error_out)) {
    return false;
  }

  if (*state == lifecycle_msgs::msg::State::PRIMARY_STATE_ACTIVE) {
    if (!change_kafka_sink_state(
        lifecycle_msgs::msg::Transition::TRANSITION_DEACTIVATE, "deactivate", error_out))
    {
      return false;
    }
    state = get_kafka_sink_state();
    if (!state) {
      error_out = "Failed to confirm kafka_sink state after deactivate";
      return false;
    }
  }

  if (*state == lifecycle_msgs::msg::State::PRIMARY_STATE_UNCONFIGURED) {
    if (!change_kafka_sink_state(
        lifecycle_msgs::msg::Transition::TRANSITION_CONFIGURE, "configure", error_out))
    {
      return false;
    }
  }

  if (!reconcile_topic_tools(plan, error_out)) {
    return false;
  }

  if (!set_kafka_sink_subscriptions_yaml(plan.sink_topics, error_out)) {
    return false;
  }

  if (!change_kafka_sink_state(
      lifecycle_msgs::msg::Transition::TRANSITION_ACTIVATE, "activate", error_out))
  {
    return false;
  }

  applied_selection_.topics = validated_topics;
  applied_selection_.sink_topics = plan.sink_topics;
  applied_selection_.timestamp = now();

  if (disable_introspection_after_apply_) {
    std::string warn;
    if (!set_introspection_enabled(false, warn)) {
      RCLCPP_WARN(get_logger(), "Failed to disable introspection_manager: %s", warn.c_str());
    }
  }

  return true;
}

bool DispatcherControllerNode::deactivate_kafka_sink(std::string & error_out)
{
  auto state = get_kafka_sink_state();
  if (!state) {
    error_out = "Unable to get kafka_sink state";
    return false;
  }
  if (*state == lifecycle_msgs::msg::State::PRIMARY_STATE_ACTIVE) {
    return change_kafka_sink_state(
      lifecycle_msgs::msg::Transition::TRANSITION_DEACTIVATE, "deactivate", error_out);
  }
  return true;
}

std::optional<uint8_t> DispatcherControllerNode::get_kafka_sink_state()
{
  if (!get_state_client_->wait_for_service(service_timeout_)) {
    RCLCPP_WARN(get_logger(), "get_state service not available");
    return std::nullopt;
  }

  auto request = std::make_shared<lifecycle_msgs::srv::GetState::Request>();
  auto future = get_state_client_->async_send_request(request);
  auto status = future.wait_for(service_timeout_);
  if (status != std::future_status::ready) {
    RCLCPP_WARN(get_logger(), "Timeout waiting for kafka_sink state");
    return std::nullopt;
  }
  return future.get()->current_state.id;
}

bool DispatcherControllerNode::change_kafka_sink_state(
  uint8_t transition_id, const std::string & action, std::string & error_out)
{
  if (!change_state_client_->wait_for_service(service_timeout_)) {
    error_out = "change_state service not available";
    return false;
  }

  auto request = std::make_shared<lifecycle_msgs::srv::ChangeState::Request>();
  request->transition.id = transition_id;
  auto future = change_state_client_->async_send_request(request);
  if (future.wait_for(service_timeout_) != std::future_status::ready) {
    error_out = "Timeout during kafka_sink " + action;
    return false;
  }
  auto response = future.get();
  if (!response->success) {
    error_out = "kafka_sink rejected transition " + action;
    return false;
  }
  RCLCPP_INFO(get_logger(), "kafka_sink %s succeeded", action.c_str());
  return true;
}

bool DispatcherControllerNode::set_kafka_sink_subscriptions_yaml(
  const std::vector<introspection_manager_msgs::msg::TopicInfo> & subs, std::string & error_out)
{
  if (!set_parameters_client_->wait_for_service(service_timeout_)) {
    error_out = "set_parameters service not available";
    return false;
  }
  auto request = std::make_shared<rcl_interfaces::srv::SetParameters::Request>();
  rclcpp::Parameter param("subscriptions_yaml", topics_to_yaml(subs));
  request->parameters.push_back(param.to_parameter_msg());
  auto future = set_parameters_client_->async_send_request(request);
  if (future.wait_for(service_timeout_) != std::future_status::ready) {
    error_out = "Timeout setting subscriptions_yaml";
    return false;
  }
  auto response = future.get();
  for (const auto & result : response->results) {
    if (!result.successful) {
      error_out = "Failed to set subscriptions_yaml: " + result.reason;
      return false;
    }
  }
  RCLCPP_INFO(get_logger(), "Updated kafka_sink subscriptions_yaml with %zu entries", subs.size());
  return true;
}

bool DispatcherControllerNode::build_topic_tools_plan(
  const std::vector<TopicSelection> & selection, TopicToolsPlan & plan, std::string & error_out)
{
  plan.sink_topics.clear();
  plan.tools.clear();

  for (const auto & entry : selection) {
    if (entry.topic.name.empty()) {
      error_out = "Selection entry missing topic_name";
      return false;
    }

    introspection_manager_msgs::msg::TopicInfo sink_topic = entry.topic;

    if (entry.topic_tools && entry.topic_tools->enabled) {
      const auto & tools_cfg = *entry.topic_tools;
      if (tools_cfg.plugin_name.empty()) {
        error_out = "topic_tools.plugin is required when topic_tools is enabled";
        return false;
      }

      std::string node_name = tools_cfg.node_name.empty() ?
        default_tool_node_name(tools_cfg.plugin_name, entry.topic.name) : tools_cfg.node_name;
      std::string output_topic = tools_cfg.output_topic.empty() ?
        default_output_topic(entry.topic.name, node_name) : tools_cfg.output_topic;
      std::string output_type = tools_cfg.output_type.empty() ? entry.topic.type : tools_cfg.output_type;

      std::vector<rclcpp::Parameter> parameters = tools_cfg.parameters;
      bool has_input{false};
      bool has_output{false};
      for (const auto & param : parameters) {
        if (param.get_name() == "input_topic") {
          has_input = true;
        } else if (param.get_name() == "output_topic") {
          has_output = true;
        }
      }
      if (!has_input) {
        parameters.emplace_back("input_topic", entry.topic.name);
      }
      if (!has_output) {
        parameters.emplace_back("output_topic", output_topic);
      }

      ResolvedTopicTool tool;
      tool.input_topic = entry.topic.name;
      tool.output_topic = output_topic;
      tool.package_name = tools_cfg.package_name;
      tool.plugin_name = tools_cfg.plugin_name;
      tool.node_name = node_name;
      tool.parameters = parameters;
      tool.output_type = output_type;
      plan.tools.push_back(tool);

      sink_topic.name = output_topic;
      sink_topic.type = output_type.empty() ? entry.topic.type : output_type;
    }

    plan.sink_topics.push_back(sink_topic);
  }

  return true;
}

bool DispatcherControllerNode::reconcile_topic_tools(const TopicToolsPlan & plan, std::string & error_out)
{
  if (!clear_active_topic_tools(error_out)) {
    return false;
  }

  for (const auto & tool : plan.tools) {
    ActiveTopicTool active;
    if (!load_and_activate_topic_tool(tool, active, error_out)) {
      return false;
    }
    active_topic_tools_[tool.input_topic] = active;
  }

  return true;
}

bool DispatcherControllerNode::clear_active_topic_tools(std::string & error_out)
{
  for (const auto & entry : active_topic_tools_) {
    std::string local_error;
    if (!deactivate_and_unload_tool(entry.second, local_error)) {
      error_out = local_error;
      return false;
    }
  }
  active_topic_tools_.clear();
  return true;
}

bool DispatcherControllerNode::load_and_activate_topic_tool(
  const ResolvedTopicTool & tool, ActiveTopicTool & out, std::string & error_out)
{
  if (!load_node_client_->wait_for_service(service_timeout_)) {
    error_out = "load_node service not available for component container";
    return false;
  }

  auto request = std::make_shared<composition_interfaces::srv::LoadNode::Request>();
  request->package_name = tool.package_name;
  request->plugin_name = tool.plugin_name;
  request->node_name = tool.node_name;
  request->node_namespace = this->get_namespace();
  for (const auto & param : tool.parameters) {
    request->parameters.push_back(param.to_parameter_msg());
  }

  auto future = load_node_client_->async_send_request(request);
  if (future.wait_for(service_timeout_) != std::future_status::ready) {
    error_out = "Timeout loading topic_tools component";
    return false;
  }
  auto response = future.get();
  if (!response->success) {
    error_out = "Failed to load topic_tools component " + tool.node_name;
    return false;
  }

  ActiveTopicTool active;
  active.unique_id = response->unique_id;
  active.full_node_name = response->full_node_name;
  active.config = tool;

  if (!change_lifecycle_state(
      active.full_node_name, lifecycle_msgs::msg::Transition::TRANSITION_CONFIGURE,
      "configure topic_tools", error_out))
  {
    deactivate_and_unload_tool(active, error_out);
    return false;
  }

  if (!change_lifecycle_state(
      active.full_node_name, lifecycle_msgs::msg::Transition::TRANSITION_ACTIVATE,
      "activate topic_tools", error_out))
  {
    deactivate_and_unload_tool(active, error_out);
    return false;
  }

  out = active;
  return true;
}

bool DispatcherControllerNode::deactivate_and_unload_tool(
  const ActiveTopicTool & tool, std::string & error_out)
{
  auto state = get_lifecycle_state(tool.full_node_name);
  if (state) {
    if (*state == lifecycle_msgs::msg::State::PRIMARY_STATE_ACTIVE) {
      change_lifecycle_state(
        tool.full_node_name, lifecycle_msgs::msg::Transition::TRANSITION_DEACTIVATE,
        "deactivate topic_tools", error_out);
    }
    change_lifecycle_state(
      tool.full_node_name, lifecycle_msgs::msg::Transition::TRANSITION_CLEANUP,
      "cleanup topic_tools", error_out);
  }

  if (!unload_node_client_->wait_for_service(service_timeout_)) {
    error_out = "unload_node service not available for component container";
    return false;
  }

  auto request = std::make_shared<composition_interfaces::srv::UnloadNode::Request>();
  request->unique_id = tool.unique_id;
  auto future = unload_node_client_->async_send_request(request);
  if (future.wait_for(service_timeout_) != std::future_status::ready) {
    error_out = "Timeout unloading topic_tools component";
    return false;
  }
  auto response = future.get();
  if (!response->success) {
    error_out = "Failed to unload topic_tools component";
    return false;
  }

  return true;
}

std::optional<uint8_t> DispatcherControllerNode::get_lifecycle_state(const std::string & node_name)
{
  auto client = create_client<lifecycle_msgs::srv::GetState>(
    node_name + "/get_state", rmw_qos_profile_services_default, client_cb_group_);
  if (!client->wait_for_service(service_timeout_)) {
    return std::nullopt;
  }

  auto request = std::make_shared<lifecycle_msgs::srv::GetState::Request>();
  auto future = client->async_send_request(request);
  if (future.wait_for(service_timeout_) != std::future_status::ready) {
    return std::nullopt;
  }
  return future.get()->current_state.id;
}

bool DispatcherControllerNode::change_lifecycle_state(
  const std::string & node_name, uint8_t transition_id, const std::string & action,
  std::string & error_out)
{
  auto client = create_client<lifecycle_msgs::srv::ChangeState>(
    node_name + "/change_state", rmw_qos_profile_services_default, client_cb_group_);
  if (!client->wait_for_service(service_timeout_)) {
    error_out = action + " service not available";
    return false;
  }

  auto request = std::make_shared<lifecycle_msgs::srv::ChangeState::Request>();
  request->transition.id = transition_id;
  auto future = client->async_send_request(request);
  if (future.wait_for(service_timeout_) != std::future_status::ready) {
    error_out = "Timeout during " + action;
    return false;
  }
  auto response = future.get();
  if (!response->success) {
    error_out = "Lifecycle transition rejected for " + node_name;
    return false;
  }
  return true;
}

bool DispatcherControllerNode::load_file_selection(
  const std::string & path, std::vector<TopicSelection> & out, std::string & error_out)
{
  if (path.empty()) {
    error_out = "selection_file_path is empty";
    return false;
  }

  YAML::Node root;
  try {
    root = YAML::LoadFile(path);
  } catch (const YAML::Exception & e) {
    error_out = "Failed to load selection file: " + std::string(e.what());
    return false;
  }

  if (!root.IsSequence()) {
    error_out = "Selection file must be a YAML sequence";
    return false;
  }

  out.clear();
  for (const auto & node : root) {
    TopicSelection selection;
    if (node["topic_name"]) {
      selection.topic.name = node["topic_name"].as<std::string>();
    } else if (node["name"]) {
      selection.topic.name = node["name"].as<std::string>();
    }
    if (node["msg_type"]) {
      selection.topic.type = node["msg_type"].as<std::string>();
    } else if (node["type"]) {
      selection.topic.type = node["type"].as<std::string>();
    }

    if (selection.topic.name.empty()) {
      error_out = "Selection file entry missing topic_name";
      return false;
    }

    if (node["topic_tools"]) {
      auto cfg_node = node["topic_tools"];
      if (!cfg_node.IsMap()) {
        error_out = "topic_tools must be a map";
        return false;
      }

      TopicToolsConfig cfg;
      cfg.enabled = cfg_node["enabled"] ? cfg_node["enabled"].as<bool>() : true;
      if (cfg.enabled) {
        if (cfg_node["package"]) {
          cfg.package_name = cfg_node["package"].as<std::string>();
        }
        if (cfg_node["plugin"]) {
          cfg.plugin_name = cfg_node["plugin"].as<std::string>();
        }
        if (cfg_node["name"]) {
          cfg.node_name = cfg_node["name"].as<std::string>();
        }
        if (cfg_node["output_topic"]) {
          cfg.output_topic = cfg_node["output_topic"].as<std::string>();
        }
        if (cfg_node["output_name"] && cfg.output_topic.empty()) {
          cfg.output_topic = default_output_topic(
            selection.topic.name, cfg_node["output_name"].as<std::string>());
        }
        if (cfg_node["output_type"]) {
          cfg.output_type = cfg_node["output_type"].as<std::string>();
        }
        if (cfg_node["parameters"]) {
          auto params = cfg_node["parameters"];
          if (!params.IsMap()) {
            error_out = "topic_tools.parameters must be a map";
            return false;
          }
          for (auto it = params.begin(); it != params.end(); ++it) {
            auto name = it->first.as<std::string>();
            try {
              cfg.parameters.push_back(parse_parameter_value(name, it->second, error_out));
            } catch (const std::exception & ex) {
              error_out = ex.what();
              return false;
            }
          }
        }

        if (cfg.plugin_name.empty()) {
          error_out = "topic_tools.plugin is required";
          return false;
        }
        selection.topic_tools = cfg;
      }
    }

    out.push_back(selection);
  }
  return true;
}

bool DispatcherControllerNode::discover_all_topics(
  std::vector<TopicSelection> & out, std::string & error_out)
{
  if (!introspection_client_->wait_for_service(service_timeout_)) {
    error_out = "Introspection service not available";
    return false;
  }
  if (!set_introspection_enabled(true, error_out)) {
    RCLCPP_WARN(get_logger(), "Failed to ensure introspection enabled: %s", error_out.c_str());
  }

  auto request = std::make_shared<introspection_manager_msgs::srv::GetTopics::Request>();
  auto future = introspection_client_->async_send_request(request);
  if (future.wait_for(service_timeout_) != std::future_status::ready) {
    error_out = "Timeout querying introspection_manager";
    return false;
  }
  out.clear();
  auto resp = future.get();
  for (const auto & info : resp->topics) {
    if (all_mode_hide_rosout_ && info.name == "/rosout") {
      continue;
    }
    if (!all_mode_allowlist_.empty()) {
      if (std::find(all_mode_allowlist_.begin(), all_mode_allowlist_.end(), info.name) ==
        all_mode_allowlist_.end())
      {
        continue;
      }
    }
    if (std::find(all_mode_denylist_.begin(), all_mode_denylist_.end(), info.name) !=
      all_mode_denylist_.end())
    {
      continue;
    }
    if (!info.name.empty() && info.name[0] == '_') {
      continue;
    }
    TopicSelection selection;
    selection.topic = info;
    out.push_back(selection);
  }

  if (!ensure_topic_limits(out, error_out)) {
    return false;
  }
  return true;
}

bool DispatcherControllerNode::infer_missing_types(
  std::vector<TopicSelection> & subs, std::string & error_out)
{
  bool needs_lookup = false;
  for (const auto & sub : subs) {
    if (sub.topic.type.empty()) {
      needs_lookup = true;
      break;
    }
  }
  if (!needs_lookup) {
    return true;
  }
  if (!introspection_client_->wait_for_service(service_timeout_)) {
    error_out = "Introspection service not available for type inference";
    return false;
  }

  auto request = std::make_shared<introspection_manager_msgs::srv::GetTopics::Request>();
  auto future = introspection_client_->async_send_request(request);
  if (future.wait_for(service_timeout_) != std::future_status::ready) {
    error_out = "Timeout querying introspection_manager for types";
    return false;
  }
  std::map<std::string, std::string> topic_types;
  for (const auto & info : future.get()->topics) {
    topic_types[info.name] = info.type;
  }

  std::vector<std::string> missing_topics;
  std::vector<std::string> available_topics;

  for (const auto & entry : topic_types) {
    available_topics.push_back(entry.first);
  }

  for (auto & sub : subs) {
    if (sub.topic.type.empty()) {
      auto it = topic_types.find(sub.topic.name);
      if (it == topic_types.end() || it->second.empty()) {
        missing_topics.push_back(sub.topic.name);
        RCLCPP_WARN(
          get_logger(), "Cannot infer type for topic: %s (not found in introspection)",
          sub.topic.name.c_str());
      } else {
        sub.topic.type = it->second;
        RCLCPP_DEBUG(
          get_logger(), "Inferred type for topic %s: %s", sub.topic.name.c_str(),
          sub.topic.type.c_str());
      }
    }
  }

  if (!missing_topics.empty()) {
    std::ostringstream oss;
    oss << "Cannot infer types for " << missing_topics.size() << " topic(s): ";
    for (size_t i = 0; i < missing_topics.size(); ++i) {
      if (i > 0) oss << ", ";
      oss << missing_topics[i];
    }
    oss << ". Available topics from introspection: " << available_topics.size();
    error_out = oss.str();

    RCLCPP_ERROR(get_logger(), "%s", error_out.c_str());
    if (!available_topics.empty() && available_topics.size() <= 20) {
      std::ostringstream avail;
      avail << "  Available topics: ";
      for (size_t i = 0; i < available_topics.size(); ++i) {
        if (i > 0) avail << ", ";
        avail << available_topics[i];
      }
      RCLCPP_ERROR(get_logger(), "%s", avail.str().c_str());
    } else if (available_topics.size() > 20) {
      RCLCPP_ERROR(
        get_logger(), "  Available topics (first 20 of %zu): ", available_topics.size());
      for (size_t i = 0; i < 20; ++i) {
        RCLCPP_ERROR(get_logger(), "    - %s", available_topics[i].c_str());
      }
    }
    return false;
  }

  return true;
}

bool DispatcherControllerNode::ensure_topic_limits(
  const std::vector<TopicSelection> & subs, std::string & error_out) const
{
  if (subs.size() > all_mode_max_topics_) {
    std::ostringstream oss;
    oss << "Selection exceeds safety limit (" << subs.size() << " > " << all_mode_max_topics_
        << ")";
    error_out = oss.str();
    return false;
  }
  return true;
}

std::vector<introspection_manager_msgs::msg::TopicInfo> DispatcherControllerNode::to_topic_info(
  const std::vector<TopicSelection> & topics) const
{
  std::vector<introspection_manager_msgs::msg::TopicInfo> out;
  out.reserve(topics.size());
  for (const auto & topic : topics) {
    out.push_back(topic.topic);
  }
  return out;
}

bool DispatcherControllerNode::set_introspection_enabled(bool enabled, std::string & error_out)
{
  if (!introspection_param_client_->wait_for_service(service_timeout_)) {
    error_out = "introspection_manager set_parameters unavailable";
    return false;
  }
  auto request = std::make_shared<rcl_interfaces::srv::SetParameters::Request>();
  rclcpp::Parameter param("introspection_enabled", enabled);
  request->parameters.push_back(param.to_parameter_msg());
  auto future = introspection_param_client_->async_send_request(request);
  if (future.wait_for(service_timeout_) != std::future_status::ready) {
    error_out = "Timeout toggling introspection_enabled";
    return false;
  }
  auto response = future.get();
  for (const auto & result : response->results) {
    if (!result.successful) {
      error_out = "Failed to toggle introspection_enabled: " + result.reason;
      return false;
    }
  }
  return true;
}

std::string DispatcherControllerNode::topics_to_yaml(
  const std::vector<introspection_manager_msgs::msg::TopicInfo> & subs) const
{
  YAML::Emitter emitter;
  emitter << YAML::BeginSeq;
  for (const auto & sub : subs) {
    emitter << YAML::BeginMap;
    emitter << YAML::Key << "topic_name" << YAML::Value << sub.name;
    emitter << YAML::Key << "msg_type" << YAML::Value << sub.type;
    emitter << YAML::EndMap;
  }
  emitter << YAML::EndSeq;
  return std::string(emitter.c_str());
}

SelectionMode DispatcherControllerNode::parse_mode(const std::string & value, bool & valid) const
{
  if (value == "gui") {
    valid = true;
    return SelectionMode::GUI;
  }
  if (value == "file") {
    valid = true;
    return SelectionMode::FILE;
  }
  if (value == "all") {
    valid = true;
    return SelectionMode::ALL;
  }
  valid = false;
  return SelectionMode::GUI;
}

std::string DispatcherControllerNode::mode_to_string(SelectionMode mode) const
{
  switch (mode) {
    case SelectionMode::GUI:
      return "gui";
    case SelectionMode::FILE:
      return "file";
    case SelectionMode::ALL:
      return "all";
    default:
      return "unknown";
  }
}

std::string DispatcherControllerNode::state_string(uint8_t state) const
{
  switch (state) {
    case lifecycle_msgs::msg::State::PRIMARY_STATE_UNCONFIGURED:
      return "unconfigured";
    case lifecycle_msgs::msg::State::PRIMARY_STATE_INACTIVE:
      return "inactive";
    case lifecycle_msgs::msg::State::PRIMARY_STATE_ACTIVE:
      return "active";
    case lifecycle_msgs::msg::State::PRIMARY_STATE_FINALIZED:
      return "finalized";
    default:
      return "unknown";
  }
}

}  // namespace dispatcher_controller

RCLCPP_COMPONENTS_REGISTER_NODE(dispatcher_controller::DispatcherControllerNode)
