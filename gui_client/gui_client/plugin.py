# Copyright 2025 Maciej Krupka
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""RQt plugin for interacting with the introspection manager."""

from functools import partial
from typing import Callable, List, Tuple

from python_qt_binding import QtCore, QtWidgets
import rclpy
from rcl_interfaces.srv import SetParameters
from rcl_interfaces.msg import Parameter, ParameterValue
from rclpy.task import Future
from rqt_gui_py.plugin import Plugin

from introspection_manager_msgs.srv import GetTopics


class IntrospectionGuiPlugin(Plugin):
    """Thin GUI client that delegates work to the introspection manager services."""

    def __init__(self, context):
        super().__init__(context)
        self.setObjectName('IntrospectionGuiPlugin')
        self._node = context.node

        self._pending_futures: List[Tuple[Future, Callable]] = []

        self._namespace_edit = QtWidgets.QLineEdit('/introspection_manager')
        self._status_label = QtWidgets.QLabel('Idle')
        self._topics_widget = QtWidgets.QTreeWidget()
        self._topics_widget.setHeaderLabels(['Topic', 'Type'])
        self._topics_widget.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)

        self._start_button = QtWidgets.QPushButton('Start introspection')
        self._stop_button = QtWidgets.QPushButton('Stop introspection')
        self._refresh_button = QtWidgets.QPushButton('Refresh topics')
        self._load_button = QtWidgets.QPushButton('Load selected topics (placeholder)')

        self._start_button.clicked.connect(partial(self._set_introspection_state, True))
        self._stop_button.clicked.connect(partial(self._set_introspection_state, False))
        self._refresh_button.clicked.connect(self._request_topics)
        self._load_button.clicked.connect(self._simulate_plugin_load)

        self._timer = QtCore.QTimer()
        self._timer.timeout.connect(self._process_futures)
        self._timer.start(200)

        widget = self._build_ui()
        if context.serial_number() > 1:
            widget.setWindowTitle(f'{widget.windowTitle()} ({context.serial_number()})')
        context.add_widget(widget)

    def _build_ui(self) -> QtWidgets.QWidget:
        root = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout()

        namespace_layout = QtWidgets.QHBoxLayout()
        namespace_layout.addWidget(QtWidgets.QLabel('Manager namespace:'))
        namespace_layout.addWidget(self._namespace_edit)
        layout.addLayout(namespace_layout)

        control_layout = QtWidgets.QHBoxLayout()
        control_layout.addWidget(self._start_button)
        control_layout.addWidget(self._stop_button)
        control_layout.addWidget(self._refresh_button)
        layout.addLayout(control_layout)

        layout.addWidget(self._topics_widget)
        layout.addWidget(self._load_button)

        status_layout = QtWidgets.QHBoxLayout()
        status_layout.addWidget(QtWidgets.QLabel('Status:'))
        status_layout.addWidget(self._status_label)
        layout.addLayout(status_layout)

        root.setLayout(layout)
        root.setWindowTitle('Introspection Manager GUI')
        return root

    def _process_futures(self):
        for future, handler in list(self._pending_futures):
            if future.done():
                try:
                    handler(future)
                finally:
                    self._pending_futures.remove((future, handler))

    def _queue_future(self, future: Future, handler: Callable) -> None:
        self._pending_futures.append((future, handler))

    def _service_name(self, suffix: str) -> str:
        namespace = self._namespace_edit.text().strip() or '/introspection_manager'
        if not namespace.startswith('/'):
            namespace = '/' + namespace
        return f'{namespace.rstrip("/")}/{suffix.lstrip("/")}'

    def _set_status(self, message: str) -> None:
        self._status_label.setText(message)
        self._node.get_logger().info(message)

    def _set_introspection_state(self, enable: bool) -> None:
        client = self._node.create_client(SetParameters, self._service_name('set_parameters'))
        if not client.wait_for_service(timeout_sec=1.0):
            self._set_status('set_parameters service not available')
            return

        param_value = ParameterValue(type=ParameterType.PARAMETER_BOOL, bool_value=enable)
        parameter = Parameter(name='introspection_enabled', value=param_value)
        request = SetParameters.Request(parameters=[parameter])
        future = client.call_async(request)
        self._queue_future(future, partial(self._handle_set_parameters_response, enable))
        self._set_status('Sending introspection request...')

    def _handle_set_parameters_response(self, enable: bool, future: Future) -> None:
        try:
            response = future.result()
            successful = all(result.successful for result in response.results)
        except Exception as exc:  # noqa: BLE001
            self._set_status(f'Failed to set parameter: {exc}')
            return

        if successful:
            state = 'enabled' if enable else 'disabled'
            self._set_status(f'Introspection {state}.')
        else:
            self._set_status('Parameter change rejected.')

    def _request_topics(self) -> None:
        client = self._node.create_client(GetTopics, self._service_name('get_topics'))
        if not client.wait_for_service(timeout_sec=1.0):
            self._set_status('get_topics service not available')
            return

        request = GetTopics.Request()
        future = client.call_async(request)
        self._queue_future(future, self._handle_topics_response)
        self._set_status('Requesting topics...')

    def _handle_topics_response(self, future: Future) -> None:
        try:
            response = future.result()
        except Exception as exc:  # noqa: BLE001
            self._set_status(f'Failed to get topics: {exc}')
            return

        self._topics_widget.clear()
        for info in response.topics:
            item = QtWidgets.QTreeWidgetItem([info.name, info.type])
            self._topics_widget.addTopLevelItem(item)

        self._topics_widget.sortItems(0, QtCore.Qt.AscendingOrder)
        self._set_status(f'Received {len(response.topics)} topics.')

    def _simulate_plugin_load(self) -> None:
        selected = self._topics_widget.selectedItems()
        if not selected:
            self._set_status('No topics selected for loading (placeholder).')
            return

        topics = ', '.join({item.text(0) for item in selected})
        self._set_status(f'Plugin loader placeholder invoked for: {topics}')

    def shutdown_plugin(self):
        self._timer.stop()
        self._pending_futures.clear()

    def save_settings(self, plugin_settings, instance_settings):
        instance_settings.set_value('namespace', self._namespace_edit.text())

    def restore_settings(self, plugin_settings, instance_settings):
        namespace = instance_settings.value('namespace')
        if namespace:
            self._namespace_edit.setText(namespace)


# rcl_interfaces.msg.ParameterType is not imported by default in Foxy, so guard it.
try:
    from rcl_interfaces.msg import ParameterType
except ImportError:  # pragma: no cover - compatibility shim
    class ParameterType:
        PARAMETER_BOOL = 1
