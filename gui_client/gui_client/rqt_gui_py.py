from rqt_gui_py.plugin import Plugin
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QListWidget, QLineEdit, QLabel, QMessageBox,
    QStyledItemDelegate, QComboBox, QFileDialog, QCheckBox, QTableWidget, QTableWidgetItem
)
from PyQt5.QtGui import QColor, QPainter, QBrush
from PyQt5.QtCore import QTimer, QRect, Qt, QMetaObject, Q_ARG
import yaml
from pathlib import Path
from datetime import datetime
import rclpy.task

# Import the service types from introspection_manager package
from introspection_manager_msgs.srv import GetTopics
from introspection_manager_msgs.msg import TopicInfo
from dispatcher_controller.srv import ApplySelection, ReloadSelection, SetSelectionMode, StopStreaming
from dispatcher_controller.srv import GetStatus as GetControllerStatus

class CustomItemDelegate(QStyledItemDelegate):
    """Custom delegate to preserve item colors even when selected."""
    def paint(self, painter, option, index):
        painter.save()
        
        # Get the background color set for the item
        bg_color = index.data(Qt.BackgroundRole)
        if bg_color:
            painter.fillRect(option.rect, bg_color)
        
        # Get text color
        text_color = index.data(Qt.ForegroundRole)
        if text_color:
            painter.setPen(text_color)
        
        # Draw text
        text = index.data(Qt.DisplayRole)
        painter.drawText(option.rect, 0, text)
        
        painter.restore()

class IntrospectionPlugin(Plugin):
    # Color constants
    COLOR_SAVED = QColor(0, 128, 0)  # Green background for saved items
    COLOR_DEFAULT_BG = QColor(255, 255, 255)  # White background
    COLOR_SAVED_TEXT = QColor(255, 255, 255)  # White text for saved items
    COLOR_DEFAULT_TEXT = QColor(0, 0, 0)  # Black text for default items
    
    def __init__(self, context):
        super().__init__(context)
        self.version_info = '0.0.8'
        self.setObjectName('IntrospectionPlugin')
        self._node = context.node  # Use the shared rclpy node provided by RQt

        # --- Build GUI layout ---
        self._widget = QWidget()
        self._widget.setWindowTitle(f'Introspection Manager GUI v{self.version_info}')
        layout = QVBoxLayout(self._widget)
        
        # First row: Save button | Start button | Unselect button
        row1 = QHBoxLayout()
        self.btn_save = QPushButton('Save Selected Topics')
        self.btn_start = QPushButton('Start Introspection')
        self.btn_unselect = QPushButton('Unselect Saved')
        row1.addWidget(self.btn_save)
        row1.addWidget(self.btn_start)
        row1.addWidget(self.btn_unselect)
        layout.addLayout(row1)
        
        # Second row: Mode controls
        row2 = QHBoxLayout()
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(['gui', 'file', 'all'])
        self.mode_combo.currentTextChanged.connect(self._on_mode_changed)
        self.selection_file_edit = QLineEdit()
        self.selection_file_edit.setPlaceholderText('selection_file_path (file mode)')
        self.btn_browse_file = QPushButton('Browse...')
        self.apply_now_checkbox = QCheckBox('Apply now')
        self.apply_now_checkbox.setChecked(True)
        self.btn_set_mode = QPushButton('Set Mode')
        row2.addWidget(QLabel('Mode:'))
        row2.addWidget(self.mode_combo)
        row2.addWidget(self.selection_file_edit)
        row2.addWidget(self.btn_browse_file)
        row2.addWidget(self.apply_now_checkbox)
        row2.addWidget(self.btn_set_mode)
        layout.addLayout(row2)
        
        # Third row: Reload / Stop controls
        row3 = QHBoxLayout()
        self.reload_apply_checkbox = QCheckBox('Apply after reload')
        self.reload_apply_checkbox.setChecked(False)
        self.btn_reload = QPushButton('Reload Selection')
        self.reset_cached_checkbox = QCheckBox('Reset cached')
        self.reset_cached_checkbox.setChecked(False)
        self.btn_stop = QPushButton('Stop Streaming')
        row3.addWidget(self.btn_reload)
        row3.addWidget(self.reload_apply_checkbox)
        row3.addWidget(self.btn_stop)
        row3.addWidget(self.reset_cached_checkbox)
        layout.addLayout(row3)
        
        # Topic filter and list
        self.filter_edit = QLineEdit()
        self.filter_edit.setPlaceholderText('Filter topics...')
        layout.addWidget(self.filter_edit)
        
        # Update button
        self.btn_update = QPushButton('Update Topics')
        layout.addWidget(self.btn_update)
        
        self.topic_list = QListWidget()
        self.topic_list.setSelectionMode(QListWidget.MultiSelection)  # Enable multi-select
        layout.addWidget(self.topic_list)
        
        # Status and plugin actions
        status_row = QHBoxLayout()
        self.btn_refresh_status = QPushButton('Refresh Status')
        self.btn_plugin = QPushButton('Simulate Plugin Load/Unload')
        status_row.addWidget(self.btn_refresh_status)
        status_row.addWidget(self.btn_plugin)
        layout.addLayout(status_row)

        self.status_label = QLabel('Status: not requested yet')
        self.status_label.setWordWrap(True)
        layout.addWidget(self.status_label)

        # Colored status message area (single place to show result of operations)
        self.status_result_label = QLabel('')
        self.status_result_label.setWordWrap(True)
        self.status_result_label.setStyleSheet(
            'QLabel { color: #555; background-color: #f5f5f5; padding: 6px; border-radius: 4px; }'
        )
        layout.addWidget(self.status_result_label)

        # Last vs Current status comparison table
        self.status_table = QTableWidget(0, 3)
        self.status_table.setHorizontalHeaderLabels(["Field", "Last", "Current"])
        self.status_table.setEditTriggers(self.status_table.NoEditTriggers)
        self.status_table.setSelectionMode(self.status_table.NoSelection)
        self.status_table.verticalHeader().setVisible(False)
        self.status_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.status_table)

        # Initialize table rows for known fields
        self._status_fields_order = [
            ("Selection mode", "selection_mode"),
            ("Kafka sink state", "kafka_sink_state"),
            ("Streaming active", "streaming_active"),
            ("Reconciling", "reconciling"),
            ("Applied topics", "applied_topics_count"),
        ]
        self.status_table.setRowCount(len(self._status_fields_order))
        for row, (label, _) in enumerate(self._status_fields_order):
            self.status_table.setItem(row, 0, QTableWidgetItem(label))
            self.status_table.setItem(row, 1, QTableWidgetItem("-"))
            self.status_table.setItem(row, 2, QTableWidgetItem("-"))

        # Keep last snapshot for comparison
        self._last_status_snapshot = None

        # Finalize layout
        self._widget.setLayout(layout)
        context.add_widget(self._widget)  # Add the widget to the RQt UI dock
        
        # --- ROS 2 service clients ---
        SERVICE_NAME = '/introspection_manager_node/get_topics'
        self.get_topics_cli = self._node.create_client(GetTopics, SERVICE_NAME)
        self.apply_selection_cli = self._node.create_client(ApplySelection, '/apply_selection')
        self.reload_selection_cli = self._node.create_client(ReloadSelection, '/reload_selection')
        self.set_mode_cli = self._node.create_client(SetSelectionMode, '/set_selection_mode')
        self.stop_streaming_cli = self._node.create_client(StopStreaming, '/stop_streaming')
        self.get_status_cli = self._node.create_client(GetControllerStatus, '/get_status')
        
        # Connect button signals to handlers
        self.btn_update.clicked.connect(self.update_topic_list)
        self.btn_save.clicked.connect(self.handle_save_selection)
        self.btn_unselect.clicked.connect(self.handle_unselect_saved)
        self.btn_plugin.clicked.connect(self.handle_plugin_action)
        self.btn_start.clicked.connect(self.handle_apply_selection)
        self.btn_reload.clicked.connect(self.handle_reload_selection)
        self.btn_set_mode.clicked.connect(self.handle_set_selection_mode)
        self.btn_stop.clicked.connect(self.handle_stop_streaming)
        self.btn_browse_file.clicked.connect(self._browse_selection_file)
        self.btn_refresh_status.clicked.connect(self.refresh_status)
        # Filter text -> update list
        self.filter_edit.textChanged.connect(self.filter_topics)

        # Store all topics for filtering
        self.all_topics = []
        # Store saved topic names to preserve green coloring
        self.saved_topics = set()

        # Periodic status refresh
        self.status_timer = QTimer(self._widget)
        self.status_timer.timeout.connect(self.refresh_status)
        self.status_timer.start(5000)
        self._on_mode_changed(self.mode_combo.currentText())
        
        # Initial status refresh (delayed to allow service discovery)
        QTimer.singleShot(2000, self.refresh_status)
    
    def _extract_topic_name(self, item_text):
        """Extract topic name from item text format 'name [type]'."""
        if '[' in item_text:
            return item_text[:item_text.rfind('[')].strip()
        return item_text.strip()
    
    def _extract_topic_info(self, item_text):
        """Extract topic name and type from item text format 'name [type]'."""
        if '[' not in item_text or ']' not in item_text:
            return (item_text.strip(), 'unknown')
        
        topic_name = item_text[:item_text.rfind('[')].strip()
        topic_type = item_text[item_text.rfind('[') + 1:item_text.rfind(']')].strip()
        return (topic_name, topic_type)

    def handle_apply_selection(self):
        """Send selected topics to dispatcher_controller/apply_selection service."""
        selected_items = self.topic_list.selectedItems()
        if not selected_items:
            QMessageBox.information(self._widget, "Info", "No topics selected to apply")
            return

        topics = []
        for item in selected_items:
            topic_name, topic_type = self._extract_topic_info(item.text())
            topic_msg = TopicInfo()
            topic_msg.name = topic_name
            # Let dispatcher_controller infer the type when it's unknown/empty
            if topic_type and topic_type.lower() not in {'unknown', 'unknown_type'}:
                topic_msg.type = topic_type
            topics.append(topic_msg)

        if not self.apply_selection_cli.wait_for_service(timeout_sec=2.5):
            QMessageBox.warning(
                self._widget,
                "Dispatcher Controller",
                "apply_selection service not available",
            )
            self._node.get_logger().warning("apply_selection service not available")
            return

        request = ApplySelection.Request()
        request.topics = topics
        future = self.apply_selection_cli.call_async(request)
        future.add_done_callback(self._on_apply_selection_response)

    def _on_mode_changed(self, mode_text):
        """Enable or disable file selection controls based on mode."""
        is_file_mode = mode_text == 'file'
        self.selection_file_edit.setEnabled(is_file_mode)
        self.btn_browse_file.setEnabled(is_file_mode)

    def _browse_selection_file(self):
        """Open a file dialog to select a YAML selection file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self._widget, "Select selection_file_path", "", "YAML Files (*.yaml *.yml);;All Files (*)")
        if file_path:
            self.selection_file_edit.setText(file_path)

    def handle_set_selection_mode(self):
        """Call dispatcher_controller/set_selection_mode with the chosen mode."""
        mode = self.mode_combo.currentText()
        if mode not in {'gui', 'file', 'all'}:
            QMessageBox.warning(self._widget, "Dispatcher Controller", "Invalid mode selected")
            return

        selection_file_path = self.selection_file_edit.text().strip()
        if mode == 'file' and not selection_file_path:
            QMessageBox.warning(
                self._widget, "Dispatcher Controller", "selection_file_path is required in file mode")
            return

        if not self.set_mode_cli.wait_for_service(timeout_sec=2.5):
            QMessageBox.warning(self._widget, "Dispatcher Controller", "set_selection_mode not available")
            self._node.get_logger().warning("set_selection_mode service not available")
            return

        request = SetSelectionMode.Request()
        request.selection_mode = mode
        request.selection_file_path = selection_file_path
        request.apply_now = self.apply_now_checkbox.isChecked()
        future = self.set_mode_cli.call_async(request)
        future.add_done_callback(self._on_set_mode_response)

    def handle_reload_selection(self):
        """Invoke dispatcher_controller/reload_selection."""
        if not self.reload_selection_cli.wait_for_service(timeout_sec=2.5):
            QMessageBox.warning(self._widget, "Dispatcher Controller", "reload_selection not available")
            self._node.get_logger().warning("reload_selection service not available")
            return

        request = ReloadSelection.Request()
        request.selection_file_path = self.selection_file_edit.text().strip()
        request.apply_now = self.reload_apply_checkbox.isChecked()
        future = self.reload_selection_cli.call_async(request)
        future.add_done_callback(self._on_reload_response)

    def handle_stop_streaming(self):
        """Call dispatcher_controller/stop_streaming."""
        if not self.stop_streaming_cli.wait_for_service(timeout_sec=2.5):
            QMessageBox.warning(self._widget, "Dispatcher Controller", "stop_streaming not available")
            self._node.get_logger().warning("stop_streaming service not available")
            return

        request = StopStreaming.Request()
        request.reset_cached = self.reset_cached_checkbox.isChecked()
        future = self.stop_streaming_cli.call_async(request)
        future.add_done_callback(self._on_stop_streaming_response)

    def refresh_status(self):
        """Fetch current dispatcher_controller status."""
        if not self.get_status_cli.wait_for_service(timeout_sec=0.5):
            self.status_label.setText("Status: get_status service not available")
            self._node.get_logger().warning("get_status service not available")
            return

        request = GetControllerStatus.Request()
        future = self.get_status_cli.call_async(request)
        future.add_done_callback(self._on_status_response)

    def update_topic_list(self):
        """Fetch topics from introspection manager and update the list."""
        if not self.get_topics_cli.wait_for_service(timeout_sec=2.5):
            # Don't show error dialog for periodic updates, just log
            self._node.get_logger().warning("get_topics service not available")
            return
        
        request = GetTopics.Request()
        future = self.get_topics_cli.call_async(request)
        future.add_done_callback(self._on_get_topics_response)
    
    def _on_get_topics_response(self, future):
        """Callback for get_topics service response."""
        try:
            response = future.result()
            # Store all topics
            self.all_topics = [(topic.name, topic.type) for topic in response.topics]
            # Apply current filter (keeps selection intact)
            self.filter_topics()
        except rclpy.task.Future._EXCEPTION_TYPES as e:
            self._node.get_logger().error(f"Service call failed: {e}")
        except Exception as e:
            self._node.get_logger().error(f"Get topics failed with unexpected error: {e}")
    
    def filter_topics(self):
        """Filter the topic list based on the filter text."""
        filter_text = self.filter_edit.text().lower()
        self.topic_list.clear()
        
        for topic_name, topic_type in self.all_topics:
            if filter_text in topic_name.lower() or filter_text in topic_type.lower():
                item_text = f"{topic_name} [{topic_type}]"
                self.topic_list.addItem(item_text)
                # Restore green color if this topic was saved
                if topic_name in self.saved_topics:
                    item = self.topic_list.item(self.topic_list.count() - 1)
                    item.setBackground(self.COLOR_SAVED)
                    item.setForeground(self.COLOR_SAVED_TEXT)
    
    def handle_save_selection(self):
        """Save selected topics to a YAML configuration file."""
        selected_items = self.topic_list.selectedItems()
        if not selected_items:
            QMessageBox.information(self._widget, "Info", "No topics selected")
            return
        
        # Parse topic name and type from the format "topic_name [topic_type]"
        topics_config = []
        for item in selected_items:
            topic_name, topic_type = self._extract_topic_info(item.text())
            topics_config.append({
                'name': topic_name,
                'type': topic_type
            })
        
        # Create config directory if it doesn't exist
        config_dir = Path.home() / '.ros' / 'gui_client'
        config_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename with timestamp
        now = datetime.now()
        timestamp = now.strftime('%Y%m%d_%H%M%S')
        config_file = config_dir / f'selected_topics_{timestamp}.yaml'
        
        # Prepare configuration data
        config_data = {
            'selected_topics': topics_config,
            'timestamp': now.isoformat(),
            'count': len(topics_config)
        }
        
        try:
            # Write to YAML file
            with open(config_file, 'w') as f:
                yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)
            
            # Success message
            msg = f"Successfully saved {len(topics_config)} topic(s) to:\n{config_file}\n\n"
            msg += "Topics:\n" + "\n".join([f"  - {t['name']}" for t in topics_config[:5]])
            if len(topics_config) > 5:
                msg += f"\n  ... and {len(topics_config) - 5} more"
            
            QMessageBox.information(self._widget, "Topics Saved", msg)
            self._node.get_logger().info(f"Saved {len(topics_config)} topics to {config_file}")
            
            selected_texts = {item.text() for item in selected_items}
            for i in range(self.topic_list.count()):
                item = self.topic_list.item(i)
                if item.text() in selected_texts:
                    item.setBackground(self.COLOR_SAVED)
                    item.setForeground(self.COLOR_SAVED_TEXT)
                    # Extract topic name and add to saved set
                    topic_name = self._extract_topic_name(item.text())
                    self.saved_topics.add(topic_name)
            
            # Auto-clear selection after save
            self.topic_list.clearSelection()
            
        except (IOError, OSError, yaml.YAMLError) as e:
            error_msg = f"Failed to save topics configuration:\n{str(e)}"
            QMessageBox.critical(self._widget, "Error", error_msg)
            self._node.get_logger().error(f"Failed to save topics: {e}")
        except Exception as e:
            error_msg = f"Unexpected error while saving topics:\n{str(e)}"
            QMessageBox.critical(self._widget, "Error", error_msg)
            self._node.get_logger().error(f"Unexpected error during save: {e}")

    def _on_apply_selection_response(self, future):
        """Handle response from dispatcher_controller apply_selection service."""
        try:
            response = future.result()
        except rclpy.task.Future._EXCEPTION_TYPES as e:
            self._node.get_logger().error(f"apply_selection service call failed: {e}")
            QTimer.singleShot(0, lambda: QMessageBox.critical(
                self._widget,
                "Dispatcher Controller",
                f"Service call failed: {e}",
            ))
        except Exception as e:
            self._node.get_logger().error(f"Unexpected error calling apply_selection: {e}")
            QTimer.singleShot(0, lambda: QMessageBox.critical(
                self._widget,
                "Dispatcher Controller",
                f"Unexpected error: {e}",
            ))
        else:
            title = "Dispatcher Controller"
            if response.success:
                QTimer.singleShot(0, lambda: QMessageBox.information(
                    self._widget, title, response.message or "Selection applied"))
            else:
                QTimer.singleShot(0, lambda: QMessageBox.warning(
                    self._widget,
                    title,
                    response.message or "Failed to apply selection",
                ))
            # Update consolidated status area
            QTimer.singleShot(0, lambda: self._set_status_message(
                response.message or ("Selection applied" if response.success else "Failed to apply selection"),
                response.success
            ))
            QTimer.singleShot(0, self.refresh_status)

    def _on_set_mode_response(self, future):
        """Handle response from set_selection_mode."""
        try:
            response = future.result()
        except rclpy.task.Future._EXCEPTION_TYPES as e:
            self._node.get_logger().error(f"set_selection_mode failed: {e}")
            QTimer.singleShot(0, lambda: QMessageBox.critical(
                self._widget, "Dispatcher Controller", f"set_selection_mode failed: {e}"))
            return
        except Exception as e:
            self._node.get_logger().error(f"Unexpected set_selection_mode error: {e}")
            QTimer.singleShot(0, lambda: QMessageBox.critical(
                self._widget, "Dispatcher Controller", f"Unexpected error: {e}"))
            return

        if response.success:
            QTimer.singleShot(0, lambda: QMessageBox.information(
                self._widget, "Dispatcher Controller", response.message or "Mode set"))
        else:
            QTimer.singleShot(0, lambda: QMessageBox.warning(
                self._widget, "Dispatcher Controller", response.message or "Failed to set mode"))
        QTimer.singleShot(0, lambda: self._set_status_message(
            response.message or ("Mode set" if response.success else "Failed to set mode"),
            response.success
        ))
        QTimer.singleShot(0, self.refresh_status)

    def _on_reload_response(self, future):
        """Handle response from reload_selection."""
        try:
            response = future.result()
        except rclpy.task.Future._EXCEPTION_TYPES as e:
            self._node.get_logger().error(f"reload_selection failed: {e}")
            QTimer.singleShot(0, lambda: QMessageBox.critical(
                self._widget, "Dispatcher Controller", f"reload_selection failed: {e}"))
            return
        except Exception as e:
            self._node.get_logger().error(f"Unexpected reload_selection error: {e}")
            QTimer.singleShot(0, lambda: QMessageBox.critical(
                self._widget, "Dispatcher Controller", f"Unexpected error: {e}"))
            return

        if response.success:
            QTimer.singleShot(0, lambda: QMessageBox.information(
                self._widget, "Dispatcher Controller", response.message or "Selection reloaded"))
        else:
            QTimer.singleShot(0, lambda: QMessageBox.warning(
                self._widget, "Dispatcher Controller", response.message or "Failed to reload"))
        QTimer.singleShot(0, lambda: self._set_status_message(
            response.message or ("Selection reloaded" if response.success else "Failed to reload"),
            response.success
        ))
        QTimer.singleShot(0, self.refresh_status)

    def _on_stop_streaming_response(self, future):
        """Handle response from stop_streaming."""
        try:
            response = future.result()
        except rclpy.task.Future._EXCEPTION_TYPES as e:
            self._node.get_logger().error(f"stop_streaming failed: {e}")
            QTimer.singleShot(0, lambda: QMessageBox.critical(
                self._widget, "Dispatcher Controller", f"stop_streaming failed: {e}"))
            return
        except Exception as e:
            self._node.get_logger().error(f"Unexpected stop_streaming error: {e}")
            QTimer.singleShot(0, lambda: QMessageBox.critical(
                self._widget, "Dispatcher Controller", f"Unexpected error: {e}"))
            return

        if response.success:
            QTimer.singleShot(0, lambda: QMessageBox.information(
                self._widget, "Dispatcher Controller", response.message or "Streaming stopped"))
        else:
            QTimer.singleShot(0, lambda: QMessageBox.warning(
                self._widget, "Dispatcher Controller", response.message or "Failed to stop"))
        QTimer.singleShot(0, lambda: self._set_status_message(
            response.message or ("Streaming stopped" if response.success else "Failed to stop"),
            response.success
        ))
        QTimer.singleShot(0, self.refresh_status)

    def _on_status_response(self, future):
        """Update status label with dispatcher_controller status."""
        try:
            response = future.result()
        except rclpy.task.Future._EXCEPTION_TYPES as e:
            self._node.get_logger().error(f"get_status failed: {e}")
            QTimer.singleShot(0, lambda: self._set_status_message(f"Status error: {e}", False))
            QTimer.singleShot(0, lambda: self.status_label.setText(f"Status error: {e}"))
            return
        except Exception as e:
            self._node.get_logger().error(f"Unexpected get_status error: {e}")
            QTimer.singleShot(0, lambda: self._set_status_message(f"Unexpected status error: {e}", False))
            QTimer.singleShot(0, lambda: self.status_label.setText(f"Status unexpected error: {e}"))
            return

        # Build current snapshot used for table and summary (robust to missing fields)
        try:
            applied_topics = getattr(response, 'applied_topics', []) or []
            current_snapshot = {
                "selection_mode": str(getattr(response, 'selection_mode', '-')),
                "kafka_sink_state": str(getattr(response, 'kafka_sink_state', '-')),
                "streaming_active": str(bool(getattr(response, 'streaming_active', False))),
                "reconciling": str(bool(getattr(response, 'reconciling', False))),
                "applied_topics_count": str(len(applied_topics)),
            }
        except Exception as e:
            self._node.get_logger().error(f"Failed to build status snapshot: {e}")
            QTimer.singleShot(0, lambda: self._set_status_message("Failed to parse status", False))
            return

        # Update comparison table in UI thread
        def _update_table():
            last = self._last_status_snapshot or {k: "-" for _, k in self._status_fields_order}
            for row, (label, key) in enumerate(self._status_fields_order):
                # Last value
                last_val = last.get(key, "-")
                self.status_table.setItem(row, 1, QTableWidgetItem(last_val))
                # Current value
                curr_val = current_snapshot.get(key, "-")
                curr_item = QTableWidgetItem(curr_val)
                # Color coding: same -> light green, different -> light orange
                if last_val == curr_val and last_val != "-":
                    curr_item.setBackground(QColor("#e8f5e9"))
                    curr_item.setForeground(QColor("#2e7d32"))
                else:
                    curr_item.setBackground(QColor("#fff3e0"))
                    curr_item.setForeground(QColor("#e65100"))
                self.status_table.setItem(row, 2, curr_item)

            # Save snapshot for next comparison
            self._last_status_snapshot = dict(current_snapshot)

        QTimer.singleShot(0, _update_table)

        # Compose human-readable status text
        def _update_text_and_banner():
            status_lines = [
                f"Selection mode: {getattr(response, 'selection_mode', '-')}",
                f"kafka_sink_state: {getattr(response, 'kafka_sink_state', '-')}",
                f"Streaming active: {getattr(response, 'streaming_active', False)}",
                f"Reconciling: {getattr(response, 'reconciling', False)}",
                f"GUI cache count: {getattr(response, 'gui_selection_count', 0)}",
                f"File cache count: {getattr(response, 'file_selection_count', 0)}",
                f"All cache count: {getattr(response, 'all_selection_count', 0)}",
                f"Applied topics: {len(applied_topics)}",
            ]
            last_error = getattr(response, 'last_error', '')
            if last_error:
                stamp = self._format_time(getattr(response, 'last_error_stamp', None))
                status_lines.append(f"Last error @ {stamp}: {last_error}")

            success = bool(getattr(response, 'success', False))
            message = getattr(response, 'message', '')
            self._set_status_message(message or ("OK" if success else "Error"), success)
            if success:
                status_lines.append(f"Message: {message}")
            else:
                status_lines.append(f"Status call unsuccessful: {message}")
            self.status_label.setText("\n".join(status_lines))

        QTimer.singleShot(0, _update_text_and_banner)

    def _format_time(self, time_msg):
        """Convert builtin_interfaces/Time to human-readable string."""
        if time_msg is None:
            return "n/a"
        # Use seconds + nanoseconds; avoid datetime conversions to keep dependencies minimal
        return f"{time_msg.sec}.{str(time_msg.nanosec).zfill(9)}"

    def _set_status_message(self, text: str, success: bool):
        """Update colored status message area with success/failure styling."""
        if text is None:
            text = ''
        if success:
            color = '#2e7d32'      # green text
            bg = '#e8f5e9'         # light green background
        else:
            color = '#c62828'      # red text
            bg = '#ffebee'         # light red background
        self.status_result_label.setStyleSheet(
            f"QLabel {{ color: {color}; background-color: {bg}; padding: 6px; border-radius: 4px; }}"
        )
        self.status_result_label.setText(text)
    
    def handle_plugin_action(self):
        """Simulate plugin load/unload (placeholder)."""
        QMessageBox.information(self._widget, "Plugin Action", "Plugin load/unload simulation (not implemented)")
    
    def handle_unselect_saved(self):
        """Clear green highlighting only from selected items."""
        selected_items = self.topic_list.selectedItems()
        for item in selected_items:
            # Check if item has green background
            if item.background().color() == self.COLOR_SAVED:
                item.setBackground(self.COLOR_DEFAULT_BG)
                item.setForeground(self.COLOR_DEFAULT_TEXT)
                # Remove from saved set
                topic_name = self._extract_topic_name(item.text())
                self.saved_topics.discard(topic_name)
    
    def shutdown_plugin(self):
        """Called when the plugin is being shut down."""
        # Destroy service clients
        self.status_timer.stop()
        self._node.destroy_client(self.get_topics_cli)
        self._node.destroy_client(self.apply_selection_cli)
        self._node.destroy_client(self.reload_selection_cli)
        self._node.destroy_client(self.set_mode_cli)
        self._node.destroy_client(self.stop_streaming_cli)
        self._node.destroy_client(self.get_status_cli)
    
    def save_settings(self, plugin_settings, instance_settings):
        """Save plugin settings (optional)."""
        pass
    
    def restore_settings(self, plugin_settings, instance_settings):
        """Restore plugin settings (optional)."""
        pass
