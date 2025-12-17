# GUI Client

RQt plugin that provides a thin GUI for controlling the `introspection_manager` node.
The plugin uses ROS 2 services only, leaving all heavy logic to the backend node.

## Features

- Configure the namespace of the introspection manager instance.
- Start or stop introspection by toggling the `introspection_enabled` parameter.
- Refresh and display the list of discovered topics via the `get_topics` service.
- Placeholder "Load selected topics" action to mimic a future plugin loader hook.

## Usage

Install the workspace and run `rqt` with the package in your environment.
The plugin appears as **IntrospectionGuiPlugin** in the RQt plugin list.
