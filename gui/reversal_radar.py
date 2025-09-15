"""
Enhanced Reversal Radar Tab
Advanced signal visualization with charts, filtering, and export capabilities
"""

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, QTableWidgetItem,
    QLabel, QHeaderView, QPushButton, QComboBox, QSpinBox, QLineEdit,
    QGroupBox, QSplitter, QTextEdit, QFileDialog, QMenu, QAction,
    QProgressBar, QCheckBox, QStyledItemDelegate, QStyleOptionViewItem
)
from PyQt5.QtCore import Qt, pyqtSignal, QTimer, QDateTime, QPropertyAnimation, QEasingCurve
from PyQt5.QtGui import QColor, QBrush, QFont, QPalette, QIcon, QPainter, QPen
import pyqtgraph as pg
import numpy as np
import datetime
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from collections import deque, defaultdict
import json
import csv

class ColoredItemDelegate(QStyledItemDelegate):
    """Custom delegate to force text colors"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.type_colors = {
            "REVERSAL": QColor(0, 255, 0),      # Green
            "BOTTOM": QColor(255, 165, 0),      # Orange  
            "BREAKOUT": QColor(0, 128, 255),    # Blue
            "MOMENTUM": QColor(255, 0, 255),    # Magenta
            "THRESHOLD": QColor(255, 255, 0),   # Yellow
            "PRICE_UPDATE": QColor(0, 255, 255), # Cyan
            "VOLUME_SPIKE": QColor(255, 128, 0), # Orange
            "UNKNOWN": QColor(192, 192, 192)    # Gray
        }
    
    def paint(self, painter, option, index):
        """Custom paint method to force colors"""
        if index.column() == 3:  # Type column
            text = index.data(Qt.DisplayRole)
            color = self.type_colors.get(text, QColor(255, 255, 255))
            
            # Fill entire cell with the signal color (semi-transparent)
            bg_color = QColor(color.red(), color.green(), color.blue(), 100)
            painter.fillRect(option.rect, bg_color)
            
            # Draw a colored border for definition
            painter.setPen(color)
            painter.drawRect(option.rect.adjusted(1, 1, -1, -1))
            
            # Draw text in the signal color for maximum visibility
            painter.setPen(color)
            font = painter.font()
            font.setBold(True)
            font.setPointSize(font.pointSize() + 1)  # Slightly larger
            painter.setFont(font)
            painter.drawText(option.rect, Qt.AlignCenter, text)
            
            return
        
        # Default painting for other columns
        super().paint(painter, option, index)

@dataclass
class SignalData:
    """Signal data structure"""
    ticker: str
    price: float
    volume: int
    signal_type: str
    progress: int
    timestamp: datetime.datetime
    confidence: float = 0.0
    rsi: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'ticker': self.ticker,
            'price': self.price,
            'volume': self.volume,
            'signal_type': self.signal_type,
            'progress': self.progress,
            'timestamp': self.timestamp.isoformat(),
            'confidence': self.confidence,
            'rsi': self.rsi,
            'metadata': self.metadata
        }

class SignalTableWidget(QTableWidget):
    """Enhanced table widget with custom features"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_context_menu()
    
    def setup_context_menu(self):
        """Setup right-click context menu"""
        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self.show_context_menu)
    
    def show_context_menu(self, position):
        """Show context menu on right-click"""
        menu = QMenu(self)
        
        # Copy action
        copy_action = QAction("Copy", self)
        copy_action.triggered.connect(self.copy_selection)
        menu.addAction(copy_action)
        
        # Export action
        export_action = QAction("Export Selected", self)
        export_action.triggered.connect(self.export_selected)
        menu.addAction(export_action)
        
        menu.exec_(self.mapToGlobal(position))
    
    def copy_selection(self):
        """Copy selected cells to clipboard"""
        selection = self.selectedRanges()
        if not selection:
            return
        
        # Get selected data
        text = ""
        for range in selection:
            for row in range(range.topRow(), range.bottomRow() + 1):
                row_data = []
                for col in range(range.leftColumn(), range.rightColumn() + 1):
                    item = self.item(row, col)
                    row_data.append(item.text() if item else "")
                text += "\t".join(row_data) + "\n"
        
        # Copy to clipboard
        from PyQt5.QtWidgets import QApplication
        QApplication.clipboard().setText(text)
    
    def export_selected(self):
        """Export selected rows to file"""
        # Implementation would go here
        pass

class ReversalRadarTab(QWidget):
    """Enhanced Reversal Radar Dashboard"""
    
    update_signal = pyqtSignal(dict)
    alert_signal = pyqtSignal(str, str)  # ticker, message
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Data storage
        self.signals_data: Dict[str, SignalData] = {}
        self.signal_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.ticker_row_map: Dict[str, int] = {}
        self.current_ticker: Optional[str] = None
        
        # Single-ticker chart data
        self.focused_ticker: Optional[str] = None
        self.current_selected_ticker: Optional[str] = None  # Currently selected ticker for chart
        self.chart_data: Dict[str, Dict] = {}  # Chart data for pyqtgraph focused chart
        self.ticker_price_data: Dict[str, Dict[str, list]] = defaultdict(lambda: {'timestamps': [], 'prices': []})
        self.ticker_signals: Dict[str, List[Dict]] = defaultdict(list)  # Store buy/sell signals per ticker
        self.available_tickers: set = set()  # Track all tickers we've seen
        
        # Filtering
        self.signal_filters = {
            "REVERSAL": True,
            "BOTTOM": True,
            "BREAKOUT": True,
            "MOMENTUM": True,
            "THRESHOLD": True,
            "PRICE_UPDATE": True,
            "VOLUME_SPIKE": True,
            "UNKNOWN": True
        }
        
        # Statistics
        self.stats = {
            'total_signals': 0,
            'by_type': defaultdict(int),
            'by_ticker': defaultdict(int),
            'high_confidence': 0
        }
        
        # UI setup
        self._setup_ui()
        self._setup_connections()
        self._setup_animations()
    
    def _setup_ui(self):
        """Initialize enhanced UI"""
        main_layout = QVBoxLayout()
        self.setLayout(main_layout)
        
        # Title and stats bar
        self._create_header_section(main_layout)
        
        # Control panel
        self._create_control_panel(main_layout)
        
        # Main content area with splitter
        splitter = QSplitter(Qt.Vertical)
        
        # Table section
        table_widget = self._create_table_section()
        splitter.addWidget(table_widget)
        
        # Chart section
        chart_widget = self._create_chart_section()
        splitter.addWidget(chart_widget)
        
        splitter.setStretchFactor(0, 2)  # Table gets 2/3
        splitter.setStretchFactor(1, 1)  # Chart gets 1/3
        
        main_layout.addWidget(splitter)
        
        # Status bar
        self._create_status_bar(main_layout)
    
    def _create_header_section(self, parent_layout):
        """Create header with title and stats"""
        header_layout = QHBoxLayout()
        
        # Title
        self.title = QLabel("📊 Live Reversal Radar Dashboard")
        self.title.setAlignment(Qt.AlignCenter)
        font = QFont()
        font.setPointSize(12)
        font.setBold(True)
        self.title.setFont(font)
        header_layout.addWidget(self.title)
        
        # Stats display
        stats_group = QGroupBox("Statistics")
        stats_layout = QHBoxLayout()
        
        self.total_signals_label = QLabel("Total: 0")
        self.high_confidence_label = QLabel("High Conf: 0")
        self.active_tickers_label = QLabel("Active: 0")
        
        stats_layout.addWidget(self.total_signals_label)
        stats_layout.addWidget(self.high_confidence_label)
        stats_layout.addWidget(self.active_tickers_label)
        
        stats_group.setLayout(stats_layout)
        header_layout.addWidget(stats_group)
        
        parent_layout.addLayout(header_layout)
    
    def _create_control_panel(self, parent_layout):
        """Create control panel with filters and actions"""
        control_group = QGroupBox("Controls")
        control_layout = QHBoxLayout()
        
        # Signal type filters
        filter_group = QGroupBox("Signal Filters")
        filter_layout = QHBoxLayout()
        
        for signal_type in self.signal_filters:
            btn = QPushButton(signal_type)
            btn.setCheckable(True)
            btn.setChecked(True)
            btn.setMaximumWidth(100)
            btn.clicked.connect(self._make_filter_toggle(signal_type, btn))
            
            # Color code buttons
            colors = {
                "REVERSAL": "#00FF00",
                "BOTTOM": "#FFA500", 
                "BREAKOUT": "#0080FF",
                "MOMENTUM": "#FF00FF",
                "THRESHOLD": "#FFFF00",
                "PRICE_UPDATE": "#00FFFF",
                "VOLUME_SPIKE": "#FF8000",
                "UNKNOWN": "#C0C0C0"
            }
            if signal_type in colors:
                btn.setStyleSheet(f"""
                    QPushButton {{
                        background-color: {colors[signal_type]};
                        color: black;
                        border-radius: 3px;
                        padding: 3px;
                    }}
                    QPushButton:checked {{
                        background-color: {colors[signal_type]};
                        border: 2px solid black;
                    }}
                    QPushButton:!checked {{
                        background-color: #808080;
                        color: white;
                    }}
                """)
            
            filter_layout.addWidget(btn)
        
        filter_group.setLayout(filter_layout)
        control_layout.addWidget(filter_group)
        
        # Confidence filter
        conf_group = QGroupBox("Min Confidence")
        conf_layout = QHBoxLayout()
        
        self.confidence_filter = QSpinBox()
        self.confidence_filter.setRange(0, 100)
        self.confidence_filter.setValue(0)
        self.confidence_filter.setSuffix("%")
        self.confidence_filter.valueChanged.connect(self._apply_filters)
        conf_layout.addWidget(self.confidence_filter)
        
        conf_group.setLayout(conf_layout)
        control_layout.addWidget(conf_group)
        
        # Search box
        search_group = QGroupBox("Search")
        search_layout = QHBoxLayout()
        
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Ticker symbol...")
        self.search_box.textChanged.connect(self._apply_filters)
        search_layout.addWidget(self.search_box)
        
        search_group.setLayout(search_layout)
        control_layout.addWidget(search_group)
        
        # Action buttons
        action_group = QGroupBox("Actions")
        action_layout = QHBoxLayout()
        
        self.export_btn = QPushButton("Export")
        self.export_btn.clicked.connect(self.export_data)
        action_layout.addWidget(self.export_btn)
        
        self.clear_btn = QPushButton("Clear")
        self.clear_btn.clicked.connect(self.clear_table)
        action_layout.addWidget(self.clear_btn)
        
        self.auto_scroll_check = QCheckBox("Auto-scroll")
        self.auto_scroll_check.setChecked(True)
        action_layout.addWidget(self.auto_scroll_check)
        
        # Reset scroll button
        self.reset_scroll_btn = QPushButton("Reset Scroll")
        self.reset_scroll_btn.clicked.connect(self._reset_scroll_behavior)
        action_layout.addWidget(self.reset_scroll_btn)
        
        action_group.setLayout(action_layout)
        control_layout.addWidget(action_group)
        
        control_group.setLayout(control_layout)
        parent_layout.addWidget(control_group)
    
    def _create_table_section(self):
        """Create enhanced table widget"""
        table_group = QGroupBox("Signals")
        table_layout = QVBoxLayout()
        
        # Create custom table
        self.table = SignalTableWidget()
        self.table.setRowCount(0)
        self.table.setColumnCount(9)
        self.table.setHorizontalHeaderLabels([
            "Ticker", "Price", "Volume", "Type", "Progress", 
            "Confidence", "RSI", "Time", "Alert"
        ])
        
        # Configure table
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Fixed)  # Ticker
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)  # Price
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)  # Volume
        header.setSectionResizeMode(3, QHeaderView.Fixed)  # Type
        header.setSectionResizeMode(4, QHeaderView.Fixed)  # Progress
        header.setSectionResizeMode(5, QHeaderView.Fixed)  # Confidence
        header.setSectionResizeMode(6, QHeaderView.Fixed)  # RSI
        header.setSectionResizeMode(7, QHeaderView.Stretch)  # Time
        header.setSectionResizeMode(8, QHeaderView.Fixed)  # Alert
        
        self.table.setColumnWidth(0, 80)   # Ticker
        self.table.setColumnWidth(3, 100)  # Type
        self.table.setColumnWidth(4, 100)  # Progress
        self.table.setColumnWidth(5, 100)  # Confidence
        self.table.setColumnWidth(6, 80)   # RSI
        self.table.setColumnWidth(8, 50)   # Alert
        
        self.table.setSortingEnabled(True)
        self.table.setAlternatingRowColors(False)  # Disable alternating colors that may interfere
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        
        # Connect row click to ticker selection for chart
        self.table.cellClicked.connect(self._on_table_cell_clicked)
        
        # Custom delegate removed - using standard item coloring
        
        # Minimal neon theme - no item-level styling to allow color overrides
        self.table.setStyleSheet("""
            QTableWidget {
                background-color: #001f3f;
                gridline-color: #00c8ff;
                border: 1px solid #00c8ff;
            }
            QHeaderView::section {
                background-color: #001f3f;
                color: #00c8ff;
                padding: 5px;
                border: 1px solid #00c8ff;
            }
        """)
        
        table_layout.addWidget(self.table)
        table_group.setLayout(table_layout)
        
        return table_group
    
    def _create_chart_section(self):
        """Create focused single-ticker chart using pyqtgraph"""
        chart_group = QGroupBox("Ticker Focus Mode")
        chart_layout = QVBoxLayout()
        
        # Ticker selection dropdown
        selection_layout = QHBoxLayout()
        selection_layout.addWidget(QLabel("Focus Ticker:"))
        self.ticker_dropdown = QComboBox()
        self.ticker_dropdown.addItem("Select a ticker...")
        self.ticker_dropdown.currentTextChanged.connect(self._on_ticker_selected)
        selection_layout.addWidget(self.ticker_dropdown)
        selection_layout.addStretch()
        chart_layout.addLayout(selection_layout)
        
        # Create pyqtgraph plot widget
        self.chart_widget = pg.PlotWidget()
        self.chart_widget.setLabel('left', 'Price', units='$')
        self.chart_widget.setLabel('bottom', 'Time')
        self.chart_widget.showGrid(x=True, y=True, alpha=0.3)
        
        # Set dark theme for pyqtgraph
        self.chart_widget.setBackground('#000814')
        
        # Initialize empty plot
        self.price_curve = None
        self.buy_markers = None
        self.sell_markers = None
        
        # Placeholder text
        self.placeholder_text = pg.TextItem('Select a ticker to view price history', 
                                          color='#00c8ff', anchor=(0.5, 0.5))
        self.chart_widget.addItem(self.placeholder_text)
        self.placeholder_text.setPos(0.5, 0.5)
        
        chart_layout.addWidget(self.chart_widget)
        chart_group.setLayout(chart_layout)
        
        return chart_group
    
    def _create_status_bar(self, parent_layout):
        """Create status bar"""
        status_layout = QHBoxLayout()
        
        self.status_label = QLabel("Ready")
        self.status_label.setStyleSheet("QLabel { color: #00c8ff; }")
        status_layout.addWidget(self.status_label)
        
        status_layout.addStretch()
        
        self.last_update_label = QLabel("Last update: Never")
        status_layout.addWidget(self.last_update_label)
        
        parent_layout.addLayout(status_layout)
    
    def _setup_connections(self):
        """Setup signal connections"""
        # User interaction tracking
        self.user_has_scrolled = False
        self.user_has_selected = False
        self.last_user_interaction = 0
        
        self.update_signal.connect(self.update_table)
        
        # Connect table signals to track user interaction
        self.table.itemSelectionChanged.connect(self._on_user_selection)
        self.table.verticalScrollBar().valueChanged.connect(self._on_user_scroll)
        
        # Update timer
        self.update_timer = QTimer(self)
        self.update_timer.timeout.connect(self.periodic_update)
        self.update_timer.start(1000)  # Update every second
        
        # Alert timer
        self.alert_timer = QTimer(self)
        self.alert_timer.timeout.connect(self.check_alerts)
        self.alert_timer.start(5000)  # Check every 5 seconds
    
    def _setup_animations(self):
        """Setup UI animations"""
        # Add fade animation for new rows
        self.fade_animation = QPropertyAnimation(self, b"windowOpacity")
        self.fade_animation.setDuration(500)
        self.fade_animation.setEasingCurve(QEasingCurve.InOutQuad)
    
    def update_table(self, data: Dict[str, Any]):
        """Update table with new signal data"""
        try:
            # Parse and validate data
            signal = self._parse_signal_data(data)
            if not signal:
                return
            
            # Store signal
            self.signals_data[signal.ticker] = signal
            self.signal_history[signal.ticker].append(signal)
            
            # Add to chart data
            self._update_focused_chart(signal)
            
            # Add ticker to dropdown if not already present
            if self.ticker_dropdown.findText(signal.ticker) == -1:
                self.ticker_dropdown.addItem(signal.ticker)
                print(f"DEBUG: Added {signal.ticker} to ticker dropdown")
            
            # Update statistics
            self._update_statistics(signal)
            
            # Update table
            row = self._ensure_row_exists(signal.ticker)
            self._update_row(row, signal)
            
            # Auto-select first ticker if none selected
            if self.current_ticker is None:
                self.set_ticker(signal.ticker)
            
            # Apply filters
            self._apply_filters()
            
            # Auto-scroll if enabled and user hasn't manually scrolled/selected
            # Only auto-scroll if checkbox is checked AND user allows it
            if self.auto_scroll_check.isChecked() and self._should_auto_scroll():
                self.table.scrollToBottom()
            
            # Update status
            self.last_update_label.setText(f"Last update: {datetime.datetime.now().strftime('%H:%M:%S')}")
            
        except Exception as e:
            print(f"Error updating table: {e}")
    
    def _parse_signal_data(self, data: Dict[str, Any]) -> Optional[SignalData]:
        """Parse raw data into SignalData object"""
        try:
            ticker = str(data.get('ticker', '')).strip().upper()
            if not ticker:
                return None
            
            # Log raw data for debugging
            print(f"DEBUG: Reversal tab received data: {data}")
            
            # Parse timestamp - check multiple possible fields
            timestamp_str = data.get('timestamp') or data.get('Time') or data.get('time')
            if timestamp_str:
                if isinstance(timestamp_str, str):
                    try:
                        # Try ISO format first
                        timestamp = datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    except ValueError:
                        try:
                            # Try other common formats
                            timestamp = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            print(f"DEBUG: Could not parse timestamp '{timestamp_str}', using current time")
                            timestamp = datetime.datetime.now()
                elif isinstance(timestamp_str, (int, float)):
                    # Unix timestamp
                    timestamp = datetime.datetime.fromtimestamp(timestamp_str)
                else:
                    timestamp = datetime.datetime.now()
            else:
                # If no timestamp provided, ensure timestamps progress for each ticker
                current_timestamp = time.time()
                if ticker in self.last_timestamp_per_ticker:
                    # Ensure each new signal for this ticker is at least 1 second after the last
                    min_timestamp = self.last_timestamp_per_ticker[ticker] + 1.0
                    current_timestamp = max(current_timestamp, min_timestamp)
                
                self.last_timestamp_per_ticker[ticker] = current_timestamp
                timestamp = datetime.datetime.fromtimestamp(current_timestamp)
                
            print(f"DEBUG: Parsed timestamp: {timestamp} from raw: {timestamp_str}")
            
            # Calculate progress if not provided
            progress = data.get('progress')
            if progress is None:
                # Use RSI if available, otherwise calculate from confidence
                rsi = data.get('rsi')
                confidence = float(data.get('confidence', 0))
                
                if rsi is not None:
                    progress = int(max(0, min(100, float(rsi))))
                elif confidence > 0:
                    progress = int(confidence)
                else:
                    progress = 50  # Default middle value
            
            # Handle both signal data and basic price data
            signal_type = str(data.get('type', 'PRICE_UPDATE')).upper()
            confidence = float(data.get('confidence', 0))
            volume = int(float(data.get('volume', 0))) if data.get('volume') else 0
            price = float(data.get('price', 0))
            
            # If this is just a price update, try to classify it intelligently
            if signal_type == 'PRICE_UPDATE' and confidence == 0:
                # Get price movement if we have history
                if ticker in getattr(self, '_price_history', {}):
                    history = self._price_history[ticker]
                    if len(history) >= 3:
                        recent_prices = list(history)[-3:]
                        price_change = (price - recent_prices[0]) / recent_prices[0] if recent_prices[0] > 0 else 0
                        
                        # Classify based on price movement and volume
                        if abs(price_change) > 0.02:  # 2% price move
                            if price_change > 0:
                                signal_type = 'BREAKOUT'
                                confidence = min(85, 60 + abs(price_change) * 1000)
                            else:
                                signal_type = 'BOTTOM'
                                confidence = min(85, 60 + abs(price_change) * 1000)
                        elif volume > 1000:  # High volume
                            signal_type = 'VOLUME_SPIKE'
                            confidence = min(80, 50 + (volume / 50))
                        elif volume > 500:
                            signal_type = 'MOMENTUM'
                            confidence = 65
                        else:
                            # Check for reversal patterns using RSI if available
                            rsi_val = data.get('rsi')
                            if rsi_val:
                                if rsi_val < 30:
                                    signal_type = 'REVERSAL'
                                    confidence = 90 - rsi_val
                                elif rsi_val > 70:
                                    signal_type = 'REVERSAL' 
                                    confidence = rsi_val - 10
                                else:
                                    confidence = 50
                            else:
                                confidence = 50
                    else:
                        confidence = 50
                else:
                    # Initialize price history tracking
                    if not hasattr(self, '_price_history'):
                        self._price_history = {}
                    if ticker not in self._price_history:
                        self._price_history[ticker] = deque(maxlen=20)
                    self._price_history[ticker].append(price)
                    confidence = 50
            
            # Debug output
            print(f"DEBUG: Classified {ticker} as {signal_type} with confidence {confidence:.1f}% (price: ${price:.2f}, volume: {volume})")
            
            return SignalData(
                ticker=ticker,
                price=float(data.get('price', 0)),
                volume=volume,
                signal_type=signal_type,
                progress=int(progress),
                timestamp=timestamp,
                confidence=confidence,
                rsi=data.get('rsi'),
                metadata=data.get('metadata', {})
            )
            
        except Exception as e:
            print(f"Error parsing signal data: {e}")
            return None
    
    def _update_row(self, row: int, signal: SignalData):
        """Update a table row with signal data"""
        # Ticker - White and bold
        ticker_item = QTableWidgetItem(signal.ticker)
        ticker_item.setFont(QFont("Monospace", 10, QFont.Bold))
        color = QColor(255, 255, 255)
        ticker_item.setForeground(color)
        ticker_item.setData(Qt.ForegroundRole, color)
        ticker_item.setData(Qt.TextColorRole, color)
        self.table.setItem(row, 0, ticker_item)
        
        # Price - Green and bold
        price_item = QTableWidgetItem(f"${signal.price:.2f}")
        price_item.setTextAlignment(Qt.AlignRight)
        price_item.setFont(QFont("Monospace", 9, QFont.Bold))
        color = QColor(0, 255, 0)
        price_item.setForeground(color)
        price_item.setData(Qt.ForegroundRole, color)
        price_item.setData(Qt.TextColorRole, color)
        self.table.setItem(row, 1, price_item)
        
        # Volume - Light blue
        volume_item = QTableWidgetItem(f"{signal.volume:,}")
        volume_item.setTextAlignment(Qt.AlignRight)
        color = QColor(200, 200, 255)
        volume_item.setForeground(color)
        volume_item.setData(Qt.ForegroundRole, color)
        volume_item.setData(Qt.TextColorRole, color)
        font = QFont()
        font.setBold(True)
        volume_item.setFont(font)
        self.table.setItem(row, 2, volume_item)
        
        # Type with color - REVERT TO ORIGINAL
        type_item = QTableWidgetItem(signal.signal_type)
        type_colors = {
            "REVERSAL": QColor(0, 255, 0),
            "BOTTOM": QColor(255, 165, 0),
            "BREAKOUT": QColor(0, 128, 255),
            "MOMENTUM": QColor(255, 0, 255),
            "THRESHOLD": QColor(255, 255, 0),
            "PRICE_UPDATE": QColor(0, 255, 255),
            "VOLUME_SPIKE": QColor(255, 128, 0),
            "UNKNOWN": QColor(192, 192, 192)
        }
        color = type_colors.get(signal.signal_type, QColor(255, 255, 255))
        type_item.setForeground(color)
        type_item.setData(Qt.ForegroundRole, color)
        type_item.setData(Qt.TextColorRole, color)
        print(f"DEBUG: Type {signal.signal_type} color set to {color.name()}")
        self.table.setItem(row, 3, type_item)
        
        # Progress bar widget with proper coloring
        progress_item = self._create_progress_item(signal.progress)
        self.table.setItem(row, 4, progress_item)
        
        # Confidence
        conf_item = self._create_confidence_item(signal.confidence)
        self.table.setItem(row, 5, conf_item)
        
        # RSI
        if signal.rsi is not None:
            rsi_item = self._create_rsi_item(signal.rsi)
            self.table.setItem(row, 6, rsi_item)
        else:
            self.table.setItem(row, 6, QTableWidgetItem("-"))
        
        # Timestamp - Light gray
        time_item = QTableWidgetItem(signal.timestamp.strftime("%H:%M:%S"))
        color = QColor(200, 200, 200)
        time_item.setForeground(color)
        time_item.setData(Qt.ForegroundRole, color)
        time_item.setData(Qt.TextColorRole, color)
        font = QFont()
        font.setBold(True)
        time_item.setFont(font)
        self.table.setItem(row, 7, time_item)
        
        # Alert indicator
        alert_item = QTableWidgetItem("")
        if signal.confidence >= 90 or signal.progress >= 90:
            alert_item.setText("🔥")
        elif signal.confidence >= 75:
            alert_item.setText("⚠️")
        self.table.setItem(row, 8, alert_item)
    
    def _ensure_row_exists(self, ticker: str) -> int:
        """Ensure a row exists for the ticker"""
        for row in range(self.table.rowCount()):
            item = self.table.item(row, 0)
            if item and item.text() == ticker:
                return row
        
        # Add new row
        row = self.table.rowCount()
        self.table.insertRow(row)
        return row
    
    def _apply_filters(self):
        """Apply all active filters"""
        min_confidence = self.confidence_filter.value()
        search_text = self.search_box.text().upper()
        
        for row in range(self.table.rowCount()):
            show_row = True
            
            # Signal type filter
            type_item = self.table.item(row, 3)
            if type_item:
                signal_type = type_item.text()
                if not self.signal_filters.get(signal_type, True):
                    show_row = False
            
            # Confidence filter
            conf_item = self.table.item(row, 5)
            if conf_item and show_row:
                try:
                    confidence = float(conf_item.text().rstrip('%'))
                    if confidence < min_confidence:
                        show_row = False
                except:
                    pass
            
            # Search filter
            if search_text and show_row:
                ticker_item = self.table.item(row, 0)
                if ticker_item and search_text not in ticker_item.text():
                    show_row = False
            
            self.table.setRowHidden(row, not show_row)
    
    def _update_statistics(self, signal: SignalData):
        """Update statistics"""
        self.stats['total_signals'] += 1
        self.stats['by_type'][signal.signal_type] += 1
        self.stats['by_ticker'][signal.ticker] += 1
        
        if signal.confidence >= 80:
            self.stats['high_confidence'] += 1
        
        # Update UI
        self.total_signals_label.setText(f"Total: {self.stats['total_signals']}")
        self.high_confidence_label.setText(f"High Conf: {self.stats['high_confidence']}")
        self.active_tickers_label.setText(f"Active: {len(self.stats['by_ticker'])}")
    
    def _update_chart(self, ticker: str):
        """Focus chart on specific ticker (highlight in legend, update axes)"""
        print(f"DEBUG: Focusing chart on ticker: {ticker}")
        
        # Make selected ticker series more prominent
        for t, series in self.chart_series.items():
            if t == ticker:
                # Highlight selected ticker with thicker line
                color = self._get_color_for_ticker(t)
                series.setPen(QPen(QColor(color), 4))  # Thicker line for selected
                print(f"DEBUG: Highlighted {ticker} series with thick {color} line")
            else:
                # Keep other series normal but slightly dimmed
                color = self._get_color_for_ticker(t)
                dimmed_color = QColor(color)
                dimmed_color.setAlpha(150)  # Make semi-transparent
                series.setPen(QPen(dimmed_color, 2))
        
        # Update chart title to show focused ticker
        self.chart.setTitle(f"Signal Price History - Focused on {ticker}")
        
        # Chart axes are auto-handled by pyqtgraph
    
    def periodic_update(self):
        """Periodic UI updates"""
        # Update time displays
        for row in range(self.table.rowCount()):
            ticker_item = self.table.item(row, 0)
            if ticker_item:
                ticker = ticker_item.text()
                if ticker in self.signals_data:
                    signal = self.signals_data[ticker]
                    age = (datetime.datetime.now() - signal.timestamp).seconds
                    
                    # Fade old signals
                    if age > 60:
                        for col in range(self.table.columnCount()):
                            item = self.table.item(row, col)
                            if item:
                                color = item.foreground().color()
                                color.setAlpha(max(50, 255 - age * 2))
                                item.setForeground(QBrush(color))
    
    def check_alerts(self):
        """Check for alert conditions"""
        for ticker, signal in self.signals_data.items():
            # High confidence alert
            if signal.confidence >= 90:
                self.alert_signal.emit(ticker, f"High confidence signal: {signal.confidence:.1f}%")
            
            # RSI extremes
            if signal.rsi:
                if signal.rsi < 20:
                    self.alert_signal.emit(ticker, f"Extremely oversold: RSI {signal.rsi:.1f}")
                elif signal.rsi > 80:
                    self.alert_signal.emit(ticker, f"Extremely overbought: RSI {signal.rsi:.1f}")
    
    def export_data(self):
        """Export table data to file"""
        filename, _ = QFileDialog.getSaveFileName(
            self, "Export Data", "", 
            "CSV Files (*.csv);;JSON Files (*.json)"
        )
        
        if not filename:
            return
        
        try:
            if filename.endswith('.csv'):
                self._export_csv(filename)
            else:
                self._export_json(filename)
            
            self.status_label.setText(f"Exported to {filename}")
            
        except Exception as e:
            self.status_label.setText(f"Export failed: {e}")
    
    def _export_csv(self, filename: str):
        """Export to CSV"""
        with open(filename, 'w', newline='') as file:
            writer = csv.writer(file)
            
            # Header
            headers = []
            for col in range(self.table.columnCount()):
                headers.append(self.table.horizontalHeaderItem(col).text())
            writer.writerow(headers)
            
            # Data
            for row in range(self.table.rowCount()):
                if not self.table.isRowHidden(row):
                    row_data = []
                    for col in range(self.table.columnCount()):
                        item = self.table.item(row, col)
                        row_data.append(item.text() if item else "")
                    writer.writerow(row_data)
    
    def _export_json(self, filename: str):
        """Export to JSON"""
        data = []
        for signal in self.signals_data.values():
            data.append(signal.to_dict())
        
        with open(filename, 'w') as file:
            json.dump(data, file, indent=2)
    
    def _make_filter_toggle(self, signal_type: str, button: QPushButton):
        """Create filter toggle function"""
        def toggle():
            self.signal_filters[signal_type] = button.isChecked()
            print(f"DEBUG: Filter {signal_type} set to {button.isChecked()}")
            self._apply_filters()
            # Show filter results
            visible_rows = sum(1 for row in range(self.table.rowCount()) if not self.table.isRowHidden(row))
            print(f"DEBUG: {visible_rows}/{self.table.rowCount()} rows visible after filtering")
        return toggle
    
    def _get_color_for_ticker(self, ticker: str) -> str:
        """Get a unique color for each ticker"""
        colors = ['#00ff00', '#ff9500', '#ff0080', '#00ffcc', '#ffff00', '#ff4500', '#8000ff', '#ff6b35']
        return colors[hash(ticker) % len(colors)]
    
    def _update_focused_chart(self, signal: SignalData):
        """Update the focused single-ticker chart with new signal data"""
        ticker = signal.ticker
        timestamp = signal.timestamp.timestamp()  # Unix timestamp
        price = signal.price
        signal_type = signal.type
        
        # Initialize data storage for ticker if it doesn't exist
        if ticker not in self.chart_data:
            self.chart_data[ticker] = {
                'timestamps': [],
                'prices': [],
                'buy_signals': {'timestamps': [], 'prices': []},
                'sell_signals': {'timestamps': [], 'prices': []}
            }
        
        # Add price data point
        self.chart_data[ticker]['timestamps'].append(timestamp)
        self.chart_data[ticker]['prices'].append(price)
        
        # Add signal markers based on type
        if signal_type in ['BOTTOM', 'REVERSAL', 'BREAKOUT']:
            # These are BUY signals
            self.chart_data[ticker]['buy_signals']['timestamps'].append(timestamp)
            self.chart_data[ticker]['buy_signals']['prices'].append(price)
        elif signal_type in ['MOMENTUM', 'THRESHOLD']:
            # These could be SELL signals
            self.chart_data[ticker]['sell_signals']['timestamps'].append(timestamp)
            self.chart_data[ticker]['sell_signals']['prices'].append(price)
        
        # Limit data points to prevent memory issues
        max_points = 200
        for data_key in ['timestamps', 'prices']:
            if len(self.chart_data[ticker][data_key]) > max_points:
                self.chart_data[ticker][data_key] = self.chart_data[ticker][data_key][-max_points:]
        
        # Limit signal markers too
        for signal_data in [self.chart_data[ticker]['buy_signals'], self.chart_data[ticker]['sell_signals']]:
            for data_key in ['timestamps', 'prices']:
                if len(signal_data[data_key]) > max_points // 4:  # Fewer signal markers
                    signal_data[data_key] = signal_data[data_key][-(max_points // 4):]
        
        # Update chart if this ticker is currently selected
        if ticker == self.current_selected_ticker:
            self._refresh_chart_display()
    
    def _refresh_chart_display(self):
        """Refresh the chart display for the currently selected ticker"""
        if not self.current_selected_ticker or self.current_selected_ticker not in self.chart_data:
            self.chart_widget.clear()
            return
        
        try:
            ticker = self.current_selected_ticker
            data = self.chart_data[ticker]
            
            if not data['timestamps'] or not data['prices']:
                return
            
            # Clear previous plots
            self.chart_widget.clear()
            
            # Plot price line
            price_curve = self.chart_widget.plot(
                x=data['timestamps'], 
                y=data['prices'],
                pen=pg.mkPen(color='#00c8ff', width=2),
                name=f'{ticker} Price'
            )
            
            # Add BUY signal markers (green)
            if data['buy_signals']['timestamps']:
                buy_scatter = self.chart_widget.plot(
                    x=data['buy_signals']['timestamps'],
                    y=data['buy_signals']['prices'],
                    pen=None,
                    symbol='o',
                    symbolBrush=pg.mkBrush(color='#00ff00'),  # Green
                    symbolSize=10,
                    name='BUY Signals'
                )
            
            # Add SELL signal markers (red)
            if data['sell_signals']['timestamps']:
                sell_scatter = self.chart_widget.plot(
                    x=data['sell_signals']['timestamps'],
                    y=data['sell_signals']['prices'],
                    pen=None,
                    symbol='s',  # Square symbol for sell
                    symbolBrush=pg.mkBrush(color='#ff0000'),  # Red
                    symbolSize=10,
                    name='SELL Signals'
                )
            
            # Set chart title
            self.chart_widget.setTitle(f'{ticker} - Focused Chart', color='#ffffff', size='12pt')
            
        except Exception as e:
            print(f"ERROR: Chart refresh failed: {e}")
    
    def _on_ticker_selected(self, ticker: str):
        """Handle ticker selection from dropdown"""
        if not ticker or ticker == "Select ticker...":
            self.current_selected_ticker = None
            self.chart_widget.clear()
            self.chart_widget.setTitle('Select a ticker to view chart', color='#888888', size='12pt')
            return
        
        self.current_selected_ticker = ticker.strip().upper()
        print(f"DEBUG: Ticker selected for chart: {self.current_selected_ticker}")
        
        # Refresh chart display
        self._refresh_chart_display()
    
    
    def _on_table_cell_clicked(self, row: int, column: int):
        """Handle table cell click to select ticker for chart"""
        try:
            ticker_item = self.table.item(row, 0)  # Ticker is in column 0
            if ticker_item:
                ticker = ticker_item.text().strip()
                if ticker:
                    # Update dropdown selection to match
                    index = self.ticker_dropdown.findText(ticker)
                    if index >= 0:
                        self.ticker_dropdown.setCurrentIndex(index)
                    else:
                        # Add ticker to dropdown if not present
                        self.ticker_dropdown.addItem(ticker)
                        self.ticker_dropdown.setCurrentText(ticker)
                    
                    # Trigger chart update
                    self._on_ticker_selected(ticker)
        except Exception as e:
            print(f"Error handling table click: {e}")
    
    def set_ticker(self, ticker: str):
        """Set current ticker and update display"""
        if not ticker:
            return
        
        ticker = ticker.strip().upper()
        self.current_ticker = ticker
        self.title.setText(f"📊 Live Reversal Radar — {ticker}")
        
        # Update chart
        self._update_chart(ticker)
        
        # Highlight row
        for row in range(self.table.rowCount()):
            item = self.table.item(row, 0)
            if item:
                if item.text() == ticker:
                    self.table.selectRow(row)
                    self.table.scrollToItem(item)
                    break
    
    def update_data_row(self, data: dict):
        """Compatibility wrapper for main window"""
        if not isinstance(data, dict):
            return
        if "ticker" not in data:
            return
        self.update_table(data)
    
    def toggle_view(self, show: bool = True):
        """Toggle visibility"""
        self.setVisible(bool(show))
    
    def clear_table(self):
        """Clear all data"""
        self.table.setRowCount(0)
        self.signals_data.clear()
        self.signal_history.clear()
        self.chart.removeAllSeries()
        self.chart_series.clear()
        self.chart_data.clear()
        
        # Reset stats
        self.stats = {
            'total_signals': 0,
            'by_type': defaultdict(int),
            'by_ticker': defaultdict(int),
            'high_confidence': 0
        }
        self._update_statistics(None)
    
    def check_updates(self):
        """Placeholder for compatibility"""
        pass
    
    def _on_user_selection(self):
        """Track when user selects items"""
        self.user_has_selected = True
        self.last_user_interaction = time.time()
    
    def _on_user_scroll(self, value):
        """Track when user scrolls manually"""
        scrollbar = self.table.verticalScrollBar()
        
        # Only count as user scroll if not at the very bottom
        # Use larger margin to be more user-friendly
        if value < scrollbar.maximum() - 15:
            self.user_has_scrolled = True
            self.last_user_interaction = time.time()
            print(f"DEBUG: User scrolled to {value}/{scrollbar.maximum()}, disabling auto-scroll")
    
    def _should_auto_scroll(self):
        """Determine if auto-scroll should happen"""
        current_time = time.time()
        
        # If user interacted recently (within 3 seconds), don't auto-scroll
        if current_time - self.last_user_interaction < 3:
            return False
        
        # If user has manually scrolled away from bottom, don't auto-scroll
        if self.user_has_scrolled:
            scrollbar = self.table.verticalScrollBar()
            # Use larger margin to be more user-friendly
            if scrollbar.value() < scrollbar.maximum() - 20:
                return False
            else:
                # User scrolled back near bottom, allow auto-scroll again
                self.user_has_scrolled = False
                return True
        
        # Default: allow auto-scroll
        return True
    
    def _user_is_interacting(self):
        """Legacy method - delegate to _should_auto_scroll"""
        return not self._should_auto_scroll()
    
    def _reset_scroll_behavior(self):
        """Reset scroll behavior and scroll to bottom"""
        self.user_has_scrolled = False
        self.user_has_selected = False
        self.last_user_interaction = 0
        self.auto_scroll_check.setChecked(True)
        self.table.scrollToBottom()
        print("DEBUG: Reset scroll behavior - auto-scroll re-enabled")
    
    def _create_progress_item(self, progress: int) -> QTableWidgetItem:
        """Create progress item with dynamic color based on value"""
        progress_item = QTableWidgetItem(f"{progress}%")
        progress_item.setTextAlignment(Qt.AlignCenter)
        
        # Color based on progress value - use proper Qt roles for override
        if progress < 40:
            color = QColor(255, 0, 0)  # Red
        elif progress < 70:
            color = QColor(255, 165, 0)  # Orange
        else:
            color = QColor(0, 255, 0)  # Green
        
        # Set color using multiple methods to ensure override
        progress_item.setForeground(color)
        progress_item.setData(Qt.ForegroundRole, color)
        progress_item.setData(Qt.TextColorRole, color)
        
        # Make text bold for visibility
        font = QFont()
        font.setBold(True)
        progress_item.setFont(font)
        
        return progress_item
    
    def _create_confidence_item(self, confidence: float) -> QTableWidgetItem:
        """Create confidence item with dynamic color based on value"""
        conf_item = QTableWidgetItem(f"{confidence:.1f}%")
        conf_item.setTextAlignment(Qt.AlignCenter)
        
        # Color based on confidence level
        if confidence >= 80:
            color = QColor(0, 255, 0)  # Green
        elif confidence >= 60:
            color = QColor(255, 165, 0)  # Orange
        else:
            color = QColor(255, 0, 0)  # Red
        
        # Set color using multiple methods to ensure override
        conf_item.setForeground(color)
        conf_item.setData(Qt.ForegroundRole, color)
        conf_item.setData(Qt.TextColorRole, color)
        
        # Make text bold for visibility
        font = QFont()
        font.setBold(True)
        conf_item.setFont(font)
        
        return conf_item
    
    def _create_rsi_item(self, rsi_value: float) -> QTableWidgetItem:
        """Create RSI item with dynamic color based on value"""
        rsi_item = QTableWidgetItem(f"{rsi_value:.1f}")
        rsi_item.setTextAlignment(Qt.AlignCenter)
        
        # Color based on RSI level
        if rsi_value < 30:
            color = QColor(0, 255, 0)  # Green - Oversold
        elif rsi_value > 70:
            color = QColor(255, 0, 0)  # Red - Overbought  
        else:
            color = QColor(255, 255, 255)  # White - Normal
        
        # Set color using multiple methods to ensure override
        rsi_item.setForeground(color)
        rsi_item.setData(Qt.ForegroundRole, color)
        rsi_item.setData(Qt.TextColorRole, color)
        
        # Make text bold for visibility
        font = QFont()
        font.setBold(True)
        rsi_item.setFont(font)
        
        return rsi_item