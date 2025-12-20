#include "dispatcher_controller/dispatcher_controller_node.hpp"

#include <algorithm>
#include <fstream>
#include <set>
#include <sstream>

#include "yaml-cpp/yaml.h"

namespace dispatcher_controller
{

using namespace std::chrono_literals;

DispatcherControllerNode::DispatcherControllerNode(const rclcpp::NodeOptions & options)
: rclcpp::Node("dispatcher_controller", options)
{
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

  bool valid_mode{true};
  selection_mode_ = parse_mode(
    declare_parameter<std::string>("selection_mode", "gui"), valid_mode);
  if (!valid_mode) {
    selection_mode_ = SelectionMode::GUI;
  }

  change_state_client_ = create_client<lifecycle_msgs::srv::ChangeState>(
    kafka_sink_node_name_ + "/change_state");
  get_state_client_ =
    create_client<lifecycle_msgs::srv::GetState>(kafka_sink_node_name_ + "/get_state");
  set_parameters_client_ = create_client<rcl_interfaces::srv::SetParameters>(
    kafka_sink_node_name_ + "/set_parameters");
  introspection_client_ = create_client<introspection_manager_msgs::srv::GetTopics>(
    introspection_service_name_);
  introspection_param_client_ = create_client<rcl_interfaces::srv::SetParameters>(
    introspection_node_name_ + "/set_parameters");

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

  auto topics = request->topics;
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

  std::vector<introspection_manager_msgs::msg::TopicInfo> selection;
  std::string error;
  bool apply_now = request->apply_now;

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
    last_file_selection_.topics = selection;
    last_file_selection_.timestamp = now();
  } else if (selection_mode_ == SelectionMode::ALL) {
    if (!discover_all_topics(selection, error)) {
      response->success = false;
      response->message = error;
      last_error_ = error;
      last_error_stamp_ = now();
      return;
    }
    last_all_selection_.topics = selection;
    last_all_selection_.timestamp = now();
  } else {  // GUI mode
    if (last_gui_selection_.topics.empty()) {
      response->success = false;
      response->message = "No cached GUI selection to reload";
      return;
    }
    selection = last_gui_selection_.topics;
    if (request->selection_file_path.size() > 0) {
      RCLCPP_WARN(get_logger(), "selection_file_path ignored in gui mode reload");
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
  response->applied_topics = applied_selection_.topics;
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

  std::vector<introspection_manager_msgs::msg::TopicInfo> selection;

  if (apply_now) {
    if (new_mode == SelectionMode::GUI) {
      if (!last_gui_selection_.topics.empty()) {
        selection = last_gui_selection_.topics;
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
      last_file_selection_.topics = selection;
      last_file_selection_.timestamp = now();
    } else {  // ALL
      if (!discover_all_topics(selection, error_out)) {
        return false;
      }
      last_all_selection_.topics = selection;
      last_all_selection_.timestamp = now();
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
  const std::vector<introspection_manager_msgs::msg::TopicInfo> & topics, std::string & error_out)
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

  if (validate_topics_) {
    auto copy = topics;
    if (!infer_missing_types(copy, error_out)) {
      return false;
    }
  }

  if (!set_kafka_sink_subscriptions_yaml(topics, error_out)) {
    return false;
  }

  if (!change_kafka_sink_state(
      lifecycle_msgs::msg::Transition::TRANSITION_ACTIVATE, "activate", error_out))
  {
    return false;
  }

  applied_selection_.topics = topics;
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
  auto status = rclcpp::spin_until_future_complete(
    get_node_base_interface(), future, service_timeout_);
  if (status != rclcpp::FutureReturnCode::SUCCESS) {
    RCLCPP_WARN(
      get_logger(), "Failed waiting for kafka_sink state: %s",
      status == rclcpp::FutureReturnCode::TIMEOUT ? "timeout" : "interrupted");
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
  auto status = rclcpp::spin_until_future_complete(
    get_node_base_interface(), future, service_timeout_);
  if (status != rclcpp::FutureReturnCode::SUCCESS) {
    error_out = (status == rclcpp::FutureReturnCode::TIMEOUT ? "Timeout" : "Interrupted") +
      std::string(" during kafka_sink ") + action;
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
  auto status = rclcpp::spin_until_future_complete(
    get_node_base_interface(), future, service_timeout_);
  if (status != rclcpp::FutureReturnCode::SUCCESS) {
    error_out = status == rclcpp::FutureReturnCode::TIMEOUT ?
      "Timeout setting subscriptions_yaml" : "Interrupted while setting subscriptions_yaml";
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

bool DispatcherControllerNode::load_file_selection(
  const std::string & path, std::vector<introspection_manager_msgs::msg::TopicInfo> & out,
  std::string & error_out)
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
    introspection_manager_msgs::msg::TopicInfo info;
    if (node["topic_name"]) {
      info.name = node["topic_name"].as<std::string>();
    } else if (node["name"]) {
      info.name = node["name"].as<std::string>();
    }
    if (node["msg_type"]) {
      info.type = node["msg_type"].as<std::string>();
    } else if (node["type"]) {
      info.type = node["type"].as<std::string>();
    }
    if (info.name.empty()) {
      error_out = "Selection file entry missing topic_name";
      return false;
    }
    out.push_back(info);
  }
  return true;
}

bool DispatcherControllerNode::discover_all_topics(
  std::vector<introspection_manager_msgs::msg::TopicInfo> & out, std::string & error_out)
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
  auto status = rclcpp::spin_until_future_complete(
    get_node_base_interface(), future, service_timeout_);
  if (status != rclcpp::FutureReturnCode::SUCCESS) {
    error_out = status == rclcpp::FutureReturnCode::TIMEOUT ?
      "Timeout querying introspection_manager" : "Interrupted while querying introspection_manager";
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
    out.push_back(info);
  }

  if (!ensure_topic_limits(out, error_out)) {
    return false;
  }
  return true;
}

bool DispatcherControllerNode::infer_missing_types(
  std::vector<introspection_manager_msgs::msg::TopicInfo> & subs, std::string & error_out)
{
  bool needs_lookup = false;
  for (const auto & sub : subs) {
    if (sub.type.empty()) {
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
  auto status = rclcpp::spin_until_future_complete(
    get_node_base_interface(), future, service_timeout_);
  if (status != rclcpp::FutureReturnCode::SUCCESS) {
    error_out = status == rclcpp::FutureReturnCode::TIMEOUT ?
      "Timeout querying introspection_manager for types" :
      "Interrupted while querying introspection_manager for types";
    return false;
  }
  std::map<std::string, std::string> topic_types;
  for (const auto & info : future.get()->topics) {
    topic_types[info.name] = info.type;
  }

  for (auto & sub : subs) {
    if (sub.type.empty()) {
      auto it = topic_types.find(sub.name);
      if (it == topic_types.end() || it->second.empty()) {
        error_out = "Missing type for topic " + sub.name;
        return false;
      }
      sub.type = it->second;
    }
  }
  return true;
}

bool DispatcherControllerNode::ensure_topic_limits(
  const std::vector<introspection_manager_msgs::msg::TopicInfo> & subs, std::string & error_out) const
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
  auto status = rclcpp::spin_until_future_complete(
    get_node_base_interface(), future, service_timeout_);
  if (status != rclcpp::FutureReturnCode::SUCCESS) {
    error_out = status == rclcpp::FutureReturnCode::TIMEOUT ?
      "Timeout toggling introspection_enabled" : "Interrupted while toggling introspection_enabled";
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
