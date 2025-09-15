"""
Enhanced GUI Components
Improved pane manager and strategy engine with better integration
"""

# ============== Enhanced Pane Manager ==============

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTextBrowser, QSplitter,
    QTabWidget, QStackedWidget, QPushButton, QLabel, QComboBox,
    QToolBar, QAction, QDockWidget, QMainWindow, QLineEdit
)
from PyQt5.QtCore import Qt, pyqtSignal, QPropertyAnimation, QRect, QEasingCurve
from PyQt5.QtGui import QIcon, QFont
from datetime import datetime
from typing import Dict, Optional, List, Any
from enum import Enum

class PaneType(Enum):
    """Types of panes"""
    TEXT = "text"
    TABLE = "table"
    CHART = "chart"
    LOG = "log"
    CUSTOM = "custom"

class PanePosition(Enum):
    """Pane positions"""
    CENTER = "center"
    LEFT = "left"
    RIGHT = "right"
    TOP = "top"
    BOTTOM = "bottom"

class EnhancedMultiPaneManager(QWidget):
    """Enhanced multi-pane manager with advanced features"""
    
    content_changed = pyqtSignal(str, str)  # Pane ID, Content
    pane_activated = pyqtSignal(str)  # Pane ID
    pane_closed = pyqtSignal(str)  # Pane ID
    layout_changed = pyqtSignal()
    
    def __init__(self, parent=None, use_tabs: bool = False, allow_floating: bool = False):
        super().__init__(parent)
        self.panes: Dict[str, Dict[str, Any]] = {}
        self.active_pane: Optional[str] = None
        self.use_tabs = use_tabs
        self.allow_floating = allow_floating
        self.layout_mode = "stacked"  # stacked, tabbed, split
        
        # Animation support
        self.animations: Dict[str, QPropertyAnimation] = {}
        
        self._setup_ui()
    
    def _setup_ui(self):
        """Initialize the UI layout"""
        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(main_layout)
        
        # Toolbar
        self._create_toolbar(main_layout)
        
        # Content area
        if self.use_tabs:
            self.container = QTabWidget()
            self.container.currentChanged.connect(self._on_tab_changed)
            self.container.tabCloseRequested.connect(self._on_tab_close_requested)
            self.container.setTabsClosable(True)
            self.container.setMovable(True)
        else:
            self.container = QStackedWidget()
        
        main_layout.addWidget(self.container)
        
        # Status bar
        self._create_status_bar(main_layout)
    
    def _create_toolbar(self, parent_layout):
        """Create toolbar with pane controls"""
        toolbar_layout = QHBoxLayout()
        
        # Layout mode selector
        self.layout_selector = QComboBox()
        self.layout_selector.addItems(["Stacked", "Tabbed", "Split Horizontal", "Split Vertical"])
        self.layout_selector.currentTextChanged.connect(self._change_layout_mode)
        toolbar_layout.addWidget(QLabel("Layout:"))
        toolbar_layout.addWidget(self.layout_selector)
        
        # Pane selector
        self.pane_selector = QComboBox()
        self.pane_selector.currentTextChanged.connect(self._on_pane_selected)
        toolbar_layout.addWidget(QLabel("Pane:"))
        toolbar_layout.addWidget(self.pane_selector)
        
        toolbar_layout.addStretch()
        
        # Action buttons
        self.btn_add = QPushButton("Add Pane")
        self.btn_remove = QPushButton("Remove")
        self.btn_clear = QPushButton("Clear All")
        
        self.btn_add.clicked.connect(self._add_default_pane)
        self.btn_remove.clicked.connect(self._remove_current_pane)
        self.btn_clear.clicked.connect(self.clear_all_panes)
        
        toolbar_layout.addWidget(self.btn_add)
        toolbar_layout.addWidget(self.btn_remove)
        toolbar_layout.addWidget(self.btn_clear)
        
        parent_layout.addLayout(toolbar_layout)
    
    def _create_status_bar(self, parent_layout):
        """Create status bar"""
        status_layout = QHBoxLayout()
        
        self.status_label = QLabel("Ready")
        self.pane_count_label = QLabel("Panes: 0")
        
        status_layout.addWidget(self.status_label)
        status_layout.addStretch()
        status_layout.addWidget(self.pane_count_label)
        
        parent_layout.addLayout(status_layout)
    
    def add_pane(
        self,
        pane_id: str,
        widget: Optional[QWidget] = None,
        pane_type: PaneType = PaneType.TEXT,
        title: Optional[str] = None,
        position: PanePosition = PanePosition.CENTER,
        closable: bool = True
    ) -> bool:
        """Add a new pane with enhanced options"""
        if pane_id in self.panes:
            self.status_label.setText(f"Pane '{pane_id}' already exists")
            return False
        
        if widget is None:
            widget = self._create_default_widget(pane_id, pane_type)
        
        # Store pane information
        self.panes[pane_id] = {
            'widget': widget,
            'type': pane_type,
            'title': title or pane_id,
            'position': position,
            'closable': closable,
            'created': datetime.now()
        }
        
        # Add to container
        if self.use_tabs:
            index = self.container.addTab(widget, title or pane_id)
            if closable:
                self.container.tabBar().tabButton(index, 1)  # Close button
        else:
            self.container.addWidget(widget)
        
        # Update selector
        self.pane_selector.addItem(title or pane_id, pane_id)
        
        # Animate addition if visible
        if widget.isVisible():
            self._animate_pane_addition(widget)
        
        self._update_status()
        self.status_label.setText(f"Added pane: {pane_id}")
        return True
    
    def _create_default_widget(self, pane_id: str, pane_type: PaneType) -> QWidget:
        """Create default widget based on type"""
        if pane_type == PaneType.TEXT:
            widget = QTextBrowser()
            widget.setPlainText(f"Pane: {pane_id}\nCreated: {datetime.now()}")
        elif pane_type == PaneType.LOG:
            widget = QTextBrowser()
            widget.setPlainText(f"Log for {pane_id}")
            widget.setFont(QFont("Monospace", 9))
        else:
            widget = QWidget()
            layout = QVBoxLayout()
            layout.addWidget(QLabel(f"Custom pane: {pane_id}"))
            widget.setLayout(layout)
        
        return widget
    
    def show_pane(self, pane_id: str, animate: bool = True) -> bool:
        """Show a specific pane with optional animation"""
        if pane_id not in self.panes:
            self.status_label.setText(f"Pane '{pane_id}' not found")
            return False
        
        widget = self.panes[pane_id]['widget']
        
        if self.use_tabs:
            # Find and activate tab
            for i in range(self.container.count()):
                if self.container.widget(i) == widget:
                    self.container.setCurrentIndex(i)
                    break
        else:
            # Show in stack
            self.container.setCurrentWidget(widget)
            
            if animate:
                self._animate_pane_transition(widget)
        
        self.active_pane = pane_id
        self.pane_activated.emit(pane_id)
        self.status_label.setText(f"Showing: {pane_id}")
        return True
    
    def _animate_pane_transition(self, widget: QWidget):
        """Animate pane transition"""
        if widget in self.animations:
            self.animations[widget].stop()
        
        animation = QPropertyAnimation(widget, b"geometry")
        animation.setDuration(300)
        animation.setEasingCurve(QEasingCurve.OutCubic)
        
        # Slide in from right
        start = QRect(widget.width(), 0, widget.width(), widget.height())
        end = widget.geometry()
        
        animation.setStartValue(start)
        animation.setEndValue(end)
        animation.start()
        
        self.animations[widget] = animation
    
    def _animate_pane_addition(self, widget: QWidget):
        """Animate new pane addition"""
        widget.setWindowOpacity(0)
        
        animation = QPropertyAnimation(widget, b"windowOpacity")
        animation.setDuration(500)
        animation.setStartValue(0)
        animation.setEndValue(1)
        animation.start()
        
        self.animations[widget] = animation
    
    def update_pane_content(self, pane_id: str, content: Any) -> bool:
        """Update content of a specific pane"""
        if pane_id not in self.panes:
            self.status_label.setText(f"Pane '{pane_id}' not found")
            return False
        
        widget = self.panes[pane_id]['widget']
        pane_type = self.panes[pane_id]['type']
        
        # Update based on widget type
        if hasattr(widget, 'setText'):
            widget.setText(str(content))
        elif hasattr(widget, 'setPlainText'):
            widget.setPlainText(str(content))
        elif hasattr(widget, 'setHtml'):
            widget.setHtml(str(content))
        elif hasattr(widget, 'update_content'):
            widget.update_content(content)
        else:
            self.status_label.setText(f"Cannot update content for {pane_id}")
            return False
        
        self.content_changed.emit(pane_id, str(content))
        return True
    
    def remove_pane(self, pane_id: str) -> bool:
        """Remove a pane with animation"""
        if pane_id not in self.panes:
            self.status_label.setText(f"Pane '{pane_id}' not found")
            return False
        
        widget = self.panes[pane_id]['widget']
        
        # Animate removal
        animation = QPropertyAnimation(widget, b"windowOpacity")
        animation.setDuration(300)
        animation.setStartValue(1)
        animation.setEndValue(0)
        animation.finished.connect(lambda: self._finish_remove_pane(pane_id))
        animation.start()
        
        return True
    
    def _finish_remove_pane(self, pane_id: str):
        """Complete pane removal after animation"""
        if pane_id not in self.panes:
            return
        
        widget = self.panes[pane_id]['widget']
        
        # Remove from container
        if self.use_tabs:
            for i in range(self.container.count()):
                if self.container.widget(i) == widget:
                    self.container.removeTab(i)
                    break
        else:
            self.container.removeWidget(widget)
        
        # Remove from selector
        index = self.pane_selector.findData(pane_id)
        if index >= 0:
            self.pane_selector.removeItem(index)
        
        widget.deleteLater()
        del self.panes[pane_id]
        
        if self.active_pane == pane_id:
            self.active_pane = None
        
        self.pane_closed.emit(pane_id)
        self._update_status()
        self.status_label.setText(f"Removed: {pane_id}")
    
    def _change_layout_mode(self, mode: str):
        """Change the layout mode"""
        # This would implement different layout modes
        self.layout_mode = mode.lower()
        self.layout_changed.emit()
        self.status_label.setText(f"Layout: {mode}")
    
    def _on_pane_selected(self, text: str):
        """Handle pane selection from combo box"""
        pane_id = self.pane_selector.currentData()
        if pane_id:
            self.show_pane(pane_id)
    
    def _on_tab_changed(self, index: int):
        """Handle tab change"""
        if index >= 0:
            widget = self.container.widget(index)
            for pane_id, info in self.panes.items():
                if info['widget'] == widget:
                    self.active_pane = pane_id
                    self.pane_activated.emit(pane_id)
                    break
    
    def _on_tab_close_requested(self, index: int):
        """Handle tab close request"""
        widget = self.container.widget(index)
        for pane_id, info in self.panes.items():
            if info['widget'] == widget and info['closable']:
                self.remove_pane(pane_id)
                break
    
    def _add_default_pane(self):
        """Add a default pane"""
        pane_id = f"pane_{len(self.panes) + 1}"
        self.add_pane(pane_id, pane_type=PaneType.TEXT)
    
    def _remove_current_pane(self):
        """Remove the current pane"""
        if self.active_pane:
            self.remove_pane(self.active_pane)
    
    def _update_status(self):
        """Update status displays"""
        self.pane_count_label.setText(f"Panes: {len(self.panes)}")
    
    def clear_all_panes(self):
        """Remove all panes"""
        pane_ids = list(self.panes.keys())
        for pane_id in pane_ids:
            self.remove_pane(pane_id)
        self.status_label.setText("All panes cleared")

# ============== Enhanced Strategy Engine Tab ==============

import sys
import logging
from datetime import datetime
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QHBoxLayout, QPushButton, QTextEdit,
    QTableWidget, QTableWidgetItem, QHeaderView, QCheckBox, QProgressBar,
    QGroupBox, QSpinBox, QComboBox
)
from PyQt5.QtCore import Qt, QProcess, pyqtSlot, QTimer, pyqtSignal
from PyQt5.QtGui import QColor, QFont, QIcon
from collections import deque, defaultdict

logger = logging.getLogger(__name__)

class EnhancedStrategyEngineTab(QWidget):
    """Enhanced strategy engine control with monitoring and analytics"""
    
    signal_received = pyqtSignal(dict)
    status_changed = pyqtSignal(str)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Process management
        self.proc: Optional[QProcess] = None
        self.auto_restart = False
        self.restart_count = 0
        self.max_restarts = 3
        
        # Statistics
        self.stats = {
            'total_signals': 0,
            'by_type': defaultdict(int),
            'by_ticker': defaultdict(int),
            'start_time': None,
            'errors': 0
        }
        
        # Signal history
        self.signal_history = deque(maxlen=1000)
        
        # Build UI
        self._build_ui()
        
        # Setup monitoring
        self._setup_monitoring()
        
        self._append_log("Strategy Engine initialized", "INFO")
    
    def _build_ui(self):
        """Build enhanced UI"""
        main_layout = QVBoxLayout()
        self.setLayout(main_layout)
        
        # Title
        self.title = QLabel("Strategy Engine Control Center")
        self.title.setAlignment(Qt.AlignCenter)
        self.title.setFont(QFont("Arial", 14, QFont.Bold))
        main_layout.addWidget(self.title)
        
        # Control panel
        control_group = QGroupBox("Controls")
        control_layout = QHBoxLayout()
        
        # Auto-start checkbox
        self.auto_start = QCheckBox("Auto-start on launch")
        control_layout.addWidget(self.auto_start)
        
        # Auto-restart checkbox
        self.auto_restart_check = QCheckBox("Auto-restart on failure")
        self.auto_restart_check.toggled.connect(self._toggle_auto_restart)
        control_layout.addWidget(self.auto_restart_check)
        
        # Manual ticker input
        ticker_label = QLabel("Manual Ticker:")
        control_layout.addWidget(ticker_label)
        
        self.ticker_input = QLineEdit()
        self.ticker_input.setPlaceholderText("Enter ticker (e.g., NVDA)")
        self.ticker_input.setMaximumWidth(120)
        self.ticker_input.returnPressed.connect(self._send_manual_ticker)
        control_layout.addWidget(self.ticker_input)
        
        self.btn_add_ticker = QPushButton("Add Ticker")
        self.btn_add_ticker.clicked.connect(self._send_manual_ticker)
        self.btn_add_ticker.setMaximumWidth(90)
        control_layout.addWidget(self.btn_add_ticker)
        
        control_layout.addStretch()
        
        # Control buttons
        self.btn_start = QPushButton("Launch Engine")
        self.btn_start.clicked.connect(self.start_engine)
        control_layout.addWidget(self.btn_start)
        
        self.btn_stop = QPushButton("Stop Engine")
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self.stop_engine)
        control_layout.addWidget(self.btn_stop)
        
        self.btn_restart = QPushButton("Restart")
        self.btn_restart.clicked.connect(self.restart_engine)
        control_layout.addWidget(self.btn_restart)
        
        control_group.setLayout(control_layout)
        main_layout.addWidget(control_group)
        
        # Status panel
        status_group = QGroupBox("Status")
        status_layout = QHBoxLayout()
        
        self.status_indicator = QLabel("STOPPED")
        self.status_indicator.setAlignment(Qt.AlignCenter)
        self.status_indicator.setStyleSheet("""
            QLabel {
                background-color: #FF0000;
                color: white;
                padding: 5px;
                border-radius: 3px;
                font-weight: bold;
            }
        """)
        status_layout.addWidget(self.status_indicator)
        
        # CPU usage
        self.cpu_label = QLabel("CPU: 0%")
        status_layout.addWidget(self.cpu_label)
        
        # Memory usage
        self.mem_label = QLabel("MEM: 0 MB")
        status_layout.addWidget(self.mem_label)
        
        # Uptime
        self.uptime_label = QLabel("Uptime: 00:00:00")
        status_layout.addWidget(self.uptime_label)
        
        status_layout.addStretch()
        
        # Statistics
        self.signals_label = QLabel("Signals: 0")
        status_layout.addWidget(self.signals_label)
        
        self.rate_label = QLabel("Rate: 0/min")
        status_layout.addWidget(self.rate_label)
        
        status_group.setLayout(status_layout)
        main_layout.addWidget(status_group)
        
        # Signals table
        signals_group = QGroupBox("Trading Signals")
        signals_layout = QVBoxLayout()
        
        # Table controls
        table_controls = QHBoxLayout()
        
        self.filter_combo = QComboBox()
        self.filter_combo.addItems(["All", "REVERSAL", "BOTTOM", "BREAKOUT", "MOMENTUM"])
        self.filter_combo.currentTextChanged.connect(self._apply_filter)
        table_controls.addWidget(QLabel("Filter:"))
        table_controls.addWidget(self.filter_combo)
        
        self.max_rows_spin = QSpinBox()
        self.max_rows_spin.setRange(10, 1000)
        self.max_rows_spin.setValue(100)
        self.max_rows_spin.valueChanged.connect(self._update_max_rows)
        table_controls.addWidget(QLabel("Max rows:"))
        table_controls.addWidget(self.max_rows_spin)
        
        table_controls.addStretch()
        
        self.btn_clear_table = QPushButton("Clear")
        self.btn_clear_table.clicked.connect(self._clear_table)
        table_controls.addWidget(self.btn_clear_table)
        
        self.btn_export = QPushButton("Export")
        self.btn_export.clicked.connect(self._export_signals)
        table_controls.addWidget(self.btn_export)
        
        signals_layout.addLayout(table_controls)
        
        # Signals table
        self.table = QTableWidget(0, 7)
        self.table.setHorizontalHeaderLabels([
            "Time", "Ticker", "Price", "Type", "Confidence", "Action", "Details"
        ])
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.Fixed)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(6, QHeaderView.Stretch)
        
        self.table.setColumnWidth(1, 80)  # Ticker
        self.table.setAlternatingRowColors(True)
        self.table.setSortingEnabled(True)
        
        signals_layout.addWidget(self.table)
        signals_group.setLayout(signals_layout)
        main_layout.addWidget(signals_group)
        
        # Log panel
        log_group = QGroupBox("Engine Log")
        log_layout = QVBoxLayout()
        
        # Log controls
        log_controls = QHBoxLayout()
        
        self.log_level_combo = QComboBox()
        self.log_level_combo.addItems(["ALL", "INFO", "WARNING", "ERROR"])
        self.log_level_combo.currentTextChanged.connect(self._filter_logs)
        log_controls.addWidget(QLabel("Level:"))
        log_controls.addWidget(self.log_level_combo)
        
        log_controls.addStretch()
        
        self.btn_clear_log = QPushButton("Clear Log")
        self.btn_clear_log.clicked.connect(self._clear_log)
        log_controls.addWidget(self.btn_clear_log)
        
        log_layout.addLayout(log_controls)
        
        # Log text
        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.log.setMaximumHeight(200)
        self.log.setFont(QFont("Monospace", 9))
        log_layout.addWidget(self.log)
        
        log_group.setLayout(log_layout)
        main_layout.addWidget(log_group)
    
    def _setup_monitoring(self):
        """Setup monitoring timers"""
        # Update timer
        self.update_timer = QTimer()
        self.update_timer.timeout.connect(self._update_status)
        self.update_timer.start(1000)  # Update every second
        
        # Process monitor
        self.monitor_timer = QTimer()
        self.monitor_timer.timeout.connect(self._monitor_process)
        self.monitor_timer.start(5000)  # Check every 5 seconds
    
    def start_engine(self):
        """Start the strategy engine process"""
        if self.proc:
            self._append_log("Engine already running", "WARNING")
            return
        
        try:
            self.proc = QProcess(self)
            self.proc.setProgram(sys.executable)
            self.proc.setArguments(["-m", "legacy.strategies.strategy_consumer"])
            self.proc.setProcessChannelMode(QProcess.MergedChannels)
            
            # Connect signals
            self.proc.readyReadStandardOutput.connect(self._read_output)
            self.proc.finished.connect(self._on_finished)
            self.proc.errorOccurred.connect(self._on_error)
            
            # Start process
            self.proc.start()
            
            # Update UI
            self._set_running(True)
            self.stats['start_time'] = datetime.now()
            self._append_log("Launching strategy engine...", "INFO")
            
        except Exception as e:
            logger.exception("Failed to start engine")
            self._append_log(f"Failed to start engine: {e}", "ERROR")
    
    def stop_engine(self):
        """Stop the strategy engine process"""
        if not self.proc:
            self._append_log("No engine running", "WARNING")
            return
        
        try:
            # Try graceful shutdown first
            self.proc.terminate()
            if not self.proc.waitForFinished(3000):
                # Force kill if needed
                self.proc.kill()
                self.proc.waitForFinished(5000)
            
            self._append_log("Strategy engine stopped", "INFO")
            
        except Exception as e:
            logger.exception("Error stopping engine")
            self._append_log(f"Error stopping engine: {e}", "ERROR")
        finally:
            self.proc = None
            self._set_running(False)
    
    def restart_engine(self):
        """Restart the strategy engine"""
        self._append_log("Restarting engine...", "INFO")
        self.stop_engine()
        QTimer.singleShot(1000, self.start_engine)
    
    def _on_finished(self, exit_code, exit_status):
        """Handle process finish"""
        self._append_log(f"Process exited with code {exit_code}", "WARNING")
        self.proc = None
        self._set_running(False)
        
        # Auto-restart if enabled and under limit
        if self.auto_restart and self.restart_count < self.max_restarts:
            self.restart_count += 1
            self._append_log(f"Auto-restarting (attempt {self.restart_count}/{self.max_restarts})", "INFO")
            QTimer.singleShot(2000, self.start_engine)
    
    def _on_error(self, error):
        """Handle process error"""
        error_map = {
            QProcess.FailedToStart: "Failed to start",
            QProcess.Crashed: "Process crashed",
            QProcess.Timedout: "Process timed out",
            QProcess.WriteError: "Write error",
            QProcess.ReadError: "Read error",
            QProcess.UnknownError: "Unknown error"
        }
        error_msg = error_map.get(error, "Unknown error")
        self._append_log(f"Process error: {error_msg}", "ERROR")
        self.stats['errors'] += 1
    
    def _read_output(self):
        """Read process output"""
        if not self.proc:
            return
        
        output = bytes(self.proc.readAllStandardOutput()).decode("utf-8", errors="ignore")
        
        for line in output.strip().splitlines():
            # Parse for signals
            if "SIGNAL:" in line or "signal" in line.lower():
                self._parse_signal_from_output(line)
            
            # Add to log
            self._append_log(line, "OUTPUT")
    
    def _parse_signal_from_output(self, line: str):
        """Try to parse a signal from output line"""
        # This would parse signals from the process output
        # For now, just log it
        self._append_log(f"Signal detected: {line}", "INFO")
    
    @pyqtSlot(dict)
    def on_signal(self, sig: dict):
        """Handle incoming trading signal"""
        # Update statistics
        self.stats['total_signals'] += 1
        sig_type = sig.get('type', 'UNKNOWN')
        self.stats['by_type'][sig_type] += 1
        ticker = sig.get('ticker', 'UNKNOWN')
        self.stats['by_ticker'][ticker] += 1
        
        # Add to history
        sig['received_at'] = datetime.now()
        self.signal_history.append(sig)
        
        # Add to table
        self._add_signal_to_table(sig)
        
        # Emit for other components
        self.signal_received.emit(sig)
        
        # Update UI
        self._update_statistics()
    
    def _add_signal_to_table(self, sig: dict):
        """Add signal to table"""
        # Check max rows
        if self.table.rowCount() >= self.max_rows_spin.value():
            self.table.removeRow(0)
        
        row = self.table.rowCount()
        self.table.insertRow(row)
        
        # Time
        time_str = datetime.now().strftime("%H:%M:%S")
        self.table.setItem(row, 0, QTableWidgetItem(time_str))
        
        # Ticker
        ticker = sig.get('ticker', 'UNKNOWN')
        ticker_item = QTableWidgetItem(ticker)
        ticker_item.setFont(QFont("Monospace", 10, QFont.Bold))
        self.table.setItem(row, 1, ticker_item)
        
        # Price
        price = sig.get('price', 0)
        price_str = f"${price:.2f}" if isinstance(price, (int, float)) else str(price)
        self.table.setItem(row, 2, QTableWidgetItem(price_str))
        
        # Type
        sig_type = sig.get('type', 'UNKNOWN')
        type_item = QTableWidgetItem(sig_type)
        type_item.setForeground(self._get_type_color(sig_type))
        self.table.setItem(row, 3, type_item)
        
        # Confidence
        confidence = sig.get('confidence', 0)
        conf_str = f"{confidence:.1f}%" if isinstance(confidence, (int, float)) else "-"
        conf_item = QTableWidgetItem(conf_str)
        if confidence >= 80:
            conf_item.setForeground(QColor(0, 255, 0))
        elif confidence >= 60:
            conf_item.setForeground(QColor(255, 165, 0))
        self.table.setItem(row, 4, conf_item)
        
        # Action
        action = sig.get('action', '-')
        self.table.setItem(row, 5, QTableWidgetItem(action))
        
        # Details
        details = sig.get('metadata', {})
        detail_str = ', '.join(f"{k}={v}" for k, v in details.items())[:100]
        self.table.setItem(row, 6, QTableWidgetItem(detail_str))
        
        # Scroll to bottom
        self.table.scrollToBottom()
    
    def _get_type_color(self, sig_type: str) -> QColor:
        """Get color for signal type"""
        colors = {
            "REVERSAL": QColor(0, 255, 0),
            "BOTTOM": QColor(255, 165, 0),
            "BREAKOUT": QColor(0, 128, 255),
            "MOMENTUM": QColor(255, 0, 255),
            "THRESHOLD": QColor(255, 255, 0),
            "UNKNOWN": QColor(192, 192, 192)
        }
        return colors.get(sig_type, QColor(255, 255, 255))
    
    def _set_running(self, running: bool):
        """Update UI for running state"""
        self.btn_start.setEnabled(not running)
        self.btn_stop.setEnabled(running)
        
        if running:
            self.status_indicator.setText("RUNNING")
            self.status_indicator.setStyleSheet("""
                QLabel {
                    background-color: #00FF00;
                    color: black;
                    padding: 5px;
                    border-radius: 3px;
                    font-weight: bold;
                }
            """)
        else:
            self.status_indicator.setText("STOPPED")
            self.status_indicator.setStyleSheet("""
                QLabel {
                    background-color: #FF0000;
                    color: white;
                    padding: 5px;
                    border-radius: 3px;
                    font-weight: bold;
                }
            """)
        
        self.status_changed.emit("RUNNING" if running else "STOPPED")
    
    def _update_status(self):
        """Update status displays"""
        if self.proc:
            # Update uptime
            if self.stats['start_time']:
                uptime = datetime.now() - self.stats['start_time']
                hours, remainder = divmod(uptime.seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                self.uptime_label.setText(f"Uptime: {hours:02d}:{minutes:02d}:{seconds:02d}")
            
            # Update signal rate
            if self.stats['start_time'] and self.stats['total_signals'] > 0:
                elapsed = (datetime.now() - self.stats['start_time']).seconds
                if elapsed > 0:
                    rate = (self.stats['total_signals'] / elapsed) * 60
                    self.rate_label.setText(f"Rate: {rate:.1f}/min")
        
        # Update signal count
        self.signals_label.setText(f"Signals: {self.stats['total_signals']}")
    
    def _update_statistics(self):
        """Update statistics displays"""
        self._update_status()
    
    def _monitor_process(self):
        """Monitor process health"""
        if self.proc:
            # Check if process is still running
            if self.proc.state() == QProcess.NotRunning:
                self._append_log("Process not running", "WARNING")
                self._set_running(False)
    
    def _append_log(self, message: str, level: str = "INFO"):
        """Append message to log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        # Color code by level
        colors = {
            "INFO": "black",
            "WARNING": "orange",
            "ERROR": "red",
            "OUTPUT": "blue"
        }
        color = colors.get(level, "black")
        
        # Format message
        formatted = f'<span style="color: {color}">[{timestamp}] {level}: {message}</span>'
        
        # Add to log
        self.log.append(formatted)
        
        # Auto-scroll
        self.log.verticalScrollBar().setValue(
            self.log.verticalScrollBar().maximum()
        )
    
    def _toggle_auto_restart(self, checked: bool):
        """Toggle auto-restart"""
        self.auto_restart = checked
        if checked:
            self._append_log("Auto-restart enabled", "INFO")
        else:
            self._append_log("Auto-restart disabled", "INFO")
            self.restart_count = 0
    
    def _apply_filter(self, filter_text: str):
        """Apply signal type filter to table"""
        for row in range(self.table.rowCount()):
            type_item = self.table.item(row, 3)
            if type_item:
                if filter_text == "All" or type_item.text() == filter_text:
                    self.table.setRowHidden(row, False)
                else:
                    self.table.setRowHidden(row, True)
    
    def _update_max_rows(self, value: int):
        """Update maximum table rows"""
        while self.table.rowCount() > value:
            self.table.removeRow(0)
    
    def _clear_table(self):
        """Clear signals table"""
        self.table.setRowCount(0)
        self._append_log("Signals table cleared", "INFO")
    
    def _clear_log(self):
        """Clear log"""
        self.log.clear()
    
    def _filter_logs(self, level: str):
        """Filter logs by level"""
        # This would implement log filtering
        pass
    
    def _export_signals(self):
        """Export signals to file"""
        from PyQt5.QtWidgets import QFileDialog
        import json
        
        filename, _ = QFileDialog.getSaveFileName(
            self, "Export Signals", "", "JSON Files (*.json)"
        )
        
        if filename:
            try:
                # Convert deque to list for JSON serialization
                signals = list(self.signal_history)
                
                # Convert datetime objects to strings
                for sig in signals:
                    if 'received_at' in sig and hasattr(sig['received_at'], 'isoformat'):
                        sig['received_at'] = sig['received_at'].isoformat()
                
                with open(filename, 'w') as f:
                    json.dump(signals, f, indent=2)
                
                self._append_log(f"Exported {len(signals)} signals to {filename}", "INFO")
                
            except Exception as e:
                self._append_log(f"Export failed: {e}", "ERROR")
    
    def _send_manual_ticker(self):
        """Send manual ticker to strategy engine for immediate processing"""
        ticker_text = self.ticker_input.text().strip().upper()
        
        if not ticker_text:
            self._append_log("Please enter a ticker symbol", "WARNING")
            return
        
        if len(ticker_text) > 10:  # Basic validation
            self._append_log(f"Invalid ticker: {ticker_text} (too long)", "ERROR")
            return
        
        try:
            # Send a manual price request message to RabbitMQ
            import json
            import time
            from datetime import datetime
            
            # Create a manual price message
            manual_message = {
                'ticker': ticker_text,
                'price': 0.0,  # Will be filled by data fetcher
                'volume': 0,
                'manual_request': True,
                'timestamp': datetime.now().isoformat(),
                'source': 'manual_input'
            }
            
            # Send to main application for processing
            if hasattr(self.parent(), 'handle_manual_ticker'):
                self.parent().handle_manual_ticker(ticker_text)
                self._append_log(f"📈 Manual ticker request sent: {ticker_text}", "INFO")
            else:
                # Try to fetch price directly
                self._fetch_manual_price(ticker_text)
            
            # Clear input
            self.ticker_input.clear()
            
        except Exception as e:
            self._append_log(f"Failed to send manual ticker: {e}", "ERROR")
    
    def _fetch_manual_price(self, ticker: str):
        """Fetch price for manual ticker using available data source"""
        try:
            # Try to get current price and send it through the system
            import yfinance as yf
            from datetime import datetime
            
            # Quick price fetch
            stock = yf.Ticker(ticker)
            info = stock.info
            current_price = info.get('currentPrice') or info.get('regularMarketPrice', 0)
            
            if current_price > 0:
                # Create price message
                price_data = {
                    'ticker': ticker,
                    'price': current_price,
                    'volume': info.get('regularMarketVolume', 0),
                    'timestamp': datetime.now().isoformat(),
                    'source': 'manual_yf'
                }
                
                # Try to send through main app if available
                if hasattr(self.parent(), 'update_ui_with_data'):
                    self.parent().update_ui_with_data(price_data)
                
                self._append_log(f"💰 {ticker}: ${current_price:.2f}", "INFO")
            else:
                self._append_log(f"Could not fetch price for {ticker}", "WARNING")
                
        except Exception as e:
            self._append_log(f"Price fetch failed for {ticker}: {e}", "ERROR")
    
    def append_signal(self, signal_data: dict):
        """Append signal to strategy engine (called by main app)"""
        try:
            # Forward to the existing on_signal method
            self.on_signal(signal_data)
        except Exception as e:
            self._append_log(f"Signal processing error: {e}", "ERROR")