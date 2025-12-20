#include <rclcpp/rclcpp.hpp>
#include "dispatcher_controller/dispatcher_controller_node.hpp"

int main(int argc, char * argv[])
{
  rclcpp::init(argc, argv);
  rclcpp::executors::MultiThreadedExecutor executor;
  auto node = std::make_shared<dispatcher_controller::DispatcherControllerNode>();
  executor.add_node(node);
  executor.spin();
  rclcpp::shutdown();
  return 0;
}
