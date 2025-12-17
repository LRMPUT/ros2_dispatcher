from rqt_gui_py.plugin import Plugin
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QListWidget, QLineEdit, QLabel, QMessageBox, QStyledItemDelegate
from PyQt5.QtGui import QColor, QPainter, QBrush
from PyQt5.QtCore import QTimer, QRect
import yaml
from pathlib import Path
from datetime import datetime

# Import the service types from introspection_manager package
from introspection_manager_msgs.srv import GetTopics

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
    def __init__(self, context):
        super().__init__(context)
        self.version_info = '0.0.1'
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
        
        # Second row: Plugin button | Stop button
        row2 = QHBoxLayout()
        self.btn_plugin = QPushButton('Simulate Plugin Load/Unload')
        self.btn_stop = QPushButton('Stop Introspection')
        row2.addWidget(self.btn_plugin)
        row2.addWidget(self.btn_stop)
        layout.addLayout(row2)
        
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
        
        # Finalize layout
        self._widget.setLayout(layout)
        context.add_widget(self._widget)  # Add the widget to the RQt UI dock
        
        # --- ROS 2 service clients ---
        self.get_topics_cli = self._node.create_client(GetTopics, '/introspection_manager_node/get_topics')
        
        # Connect button signals to handlers
        self.btn_update.clicked.connect(self.update_topic_list)
        self.btn_save.clicked.connect(self.handle_save_selection)
        self.btn_unselect.clicked.connect(self.handle_unselect_saved)
        self.btn_plugin.clicked.connect(self.handle_plugin_action)
        # Filter text -> update list
        self.filter_edit.textChanged.connect(self.filter_topics)
        
        # Store all topics for filtering
        self.all_topics = []
        # Store saved topic names to preserve green coloring
        self.saved_topics = set()
    
    def update_topic_list(self):
        """Fetch topics from introspection manager and update the list."""
        if not self.get_topics_cli.wait_for_service(timeout_sec=0.5):
            # Don't show error dialog for periodic updates, just log
            self._node.get_logger().warn("get_topics service not available")
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
        except Exception as e:
            self._node.get_logger().error(f"Get topics failed: {e}")
    
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
                    item.setBackground(QColor(0, 128, 0))  # Green
                    item.setForeground(QColor(255, 255, 255))  # White text
    
    def handle_save_selection(self):
        """Save selected topics to a YAML configuration file."""
        selected_items = self.topic_list.selectedItems()
        if not selected_items:
            QMessageBox.information(self._widget, "Info", "No topics selected")
            return
        
        # Parse topic name and type from the format "topic_name [topic_type]"
        topics_config = []
        for item in selected_items:
            text = item.text()
            # Extract topic name and type
            if '[' in text and ']' in text:
                topic_name = text[:text.rfind('[')].strip()
                topic_type = text[text.rfind('[')+1:text.rfind(']')].strip()
                topics_config.append({
                    'name': topic_name,
                    'type': topic_type
                })
            else:
                # Fallback if format is unexpected
                topics_config.append({'name': text, 'type': 'unknown'})
        
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
            
            # Color saved topics green
            for i in range(self.topic_list.count()):
                item = self.topic_list.item(i)
                for selected_item in selected_items:
                    if item.text() == selected_item.text():
                        item.setBackground(QColor(0, 128, 0))  # Green
                        item.setForeground(QColor(255, 255, 255))  # White text
                        # Extract topic name and add to saved set
                        topic_text = item.text()
                        topic_name = topic_text[:topic_text.rfind('[')].strip()
                        self.saved_topics.add(topic_name)
                        break
            
            # Auto-clear selection after save
            self.topic_list.clearSelection()
            
        except Exception as e:
            error_msg = f"Failed to save topics configuration:\n{str(e)}"
            QMessageBox.critical(self._widget, "Error", error_msg)
            self._node.get_logger().error(f"Failed to save topics: {e}")
    
    def handle_plugin_action(self):
        """Simulate plugin load/unload (placeholder)."""
        QMessageBox.information(self._widget, "Plugin Action", "Plugin load/unload simulation (not implemented)")
    
    def handle_unselect_saved(self):
        """Clear green highlighting only from selected items."""
        selected_items = self.topic_list.selectedItems()
        for item in selected_items:
            # Check if item has green background
            if item.background().color() == QColor(0, 128, 0):
                item.setBackground(QColor(255, 255, 255))  # Reset to white
                item.setForeground(QColor(0, 0, 0))  # Reset to black text
                # Remove from saved set
                topic_text = item.text()
                topic_name = topic_text[:topic_text.rfind('[')].strip()
                self.saved_topics.discard(topic_name)
    
    def shutdown_plugin(self):
        """Called when the plugin is being shut down."""
        # Destroy service clients
        self._node.destroy_client(self.get_topics_cli)
    
    def save_settings(self, plugin_settings, instance_settings):
        """Save plugin settings (optional)."""
        pass
    
    def restore_settings(self, plugin_settings, instance_settings):
        """Restore plugin settings (optional)."""
        pass