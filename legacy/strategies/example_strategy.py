#!/usr/bin/env python3
"""
Integrated Strategy System
Combines the enhanced CyberNinja Trader with the sophisticated ThresholdStrategy
"""

import logging
import asyncio
import time
import json
import re
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from enum import Enum, auto
from pathlib import Path
import numpy as np
import pandas as pd
from abc import ABC, abstractmethod

from PyQt5.QtCore import QObject, pyqtSignal, QThread, QMutex, QMutexLocker
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QTableWidget, QTableWidgetItem, QHeaderView, QPushButton, QSpinBox, QDoubleSpinBox, QCheckBox, QGroupBox, QTextEdit, QTabWidget

# Try to import Prometheus metrics, use dummy if not available
try:
    from prometheus_client import Counter, Gauge
    SIGNALS_GENERATED = Counter('signals_generated_total', 'Total trade signals generated', ['ticker', 'action'])
    POSITIONS_ACTIVE = Gauge('positions_active_total', 'Number of active positions', ['ticker'])
    LATCH_SUPPRESSED = Counter('signals_latch_suppressed_total', 'Total signals suppressed by latching', ['ticker'])
    LATCH_THRESHOLD_SUPPRESSED = Counter('signals_latch_threshold_suppressed_total', 'Total signals suppressed by price threshold', ['ticker'])
    LATCH_EXPIRED = Counter('signals_latch_expired_total', 'Total latched signals expired', ['ticker'])
    THRESHOLD_ATTEMPTS = Gauge('threshold_attempts_total', 'Total threshold evaluation attempts per ticker', ['ticker'])
except ImportError:
    # Dummy metrics if Prometheus not installed
    class DummyMetric:
        def labels(self, **kwargs):
            return self
        def inc(self):
            pass
        def set(self, value):
            pass
    
    SIGNALS_GENERATED = DummyMetric()
    POSITIONS_ACTIVE = DummyMetric()
    LATCH_SUPPRESSED = DummyMetric()
    LATCH_THRESHOLD_SUPPRESSED = DummyMetric()
    LATCH_EXPIRED = DummyMetric()
    THRESHOLD_ATTEMPTS = DummyMetric()

# ==================== STRATEGY COMPONENTS ====================

class Action(Enum):
    BUY = auto()
    SELL = auto()
    HOLD = auto()

@dataclass
class TradeSignal:
    """Structured trade signal with validation and additional context."""
    ticker: str
    action: Action
    price: float
    confidence: float  # 0.0 to 1.0
    timestamp: float = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()
        if self.metadata is None:
            self.metadata = {}
        self._validate()

    def _validate(self):
        """Validate trade signal fields."""
        if not 0 <= self.confidence <= 1:
            raise ValueError(f"Confidence must be between 0 and 1, got {self.confidence}")
        if self.price <= 0:
            raise ValueError(f"Price must be positive, got {self.price}")
        if not re.match(r'^[A-Z0-9.]+$', self.ticker):
            raise ValueError(f"Invalid ticker format: {self.ticker}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert signal to dictionary for serialization."""
        return {
            'ticker': self.ticker,
            'action': self.action.name,
            'price': self.price,
            'confidence': self.confidence,
            'timestamp': self.timestamp,
            'metadata': self.metadata
        }

class Strategy(ABC):
    """Abstract base class for trading strategies."""
    @abstractmethod
    async def evaluate(self, ticker: str, historical_bars: List[Dict], price: float) -> Optional[TradeSignal]:
        pass

class EnhancedThresholdStrategy(Strategy):
    """Enhanced threshold strategy with all features from example_strategy.py"""
    
    def __init__(self, 
                 buy_threshold: float = 100, 
                 sell_threshold: float = 200,
                 position_file: str = 'positions.json',
                 config: Optional[Dict[str, Any]] = None):
        """Initialize strategy with all enhanced features."""
        self.logger = logging.getLogger('EnhancedThresholdStrategy')
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        self.position_file = Path(position_file)
        self.position = self._load_positions()
        self.latched_state = self._load_latched_state()
        self.config = config or {}
        
        # Configuration parameters
        self.min_profit_pct = self.config.get('min_profit_pct', 5.0)
        self.use_latching = self.config.get('use_latching', True)
        self.latching_threshold_pct = self.config.get('latching_threshold_pct', 10.0)
        self.latching_expiration_hours = self.config.get('latching_expiration_hours', 24.0)
        self.cooldown_period = self.config.get('cooldown_period', 300)  # 5 minutes
        self.use_volume_adjustment = self.config.get('use_volume_adjustment', True)
        self.use_ema_filter = self.config.get('use_ema_filter', True)
        self.ema_period = self.config.get('ema_period', 20)
        
        # State tracking
        self.last_signal_time: Dict[str, float] = {}
        self.historical_bars_cache: Dict[str, List[Dict]] = {}
        self.mutex = QMutex()
        
        self._validate_config()
        self.logger.info(f"Enhanced strategy initialized with config: {self.config}")

    def _validate_config(self):
        """Validate configuration parameters."""
        validations = [
            (isinstance(self.use_latching, bool), f"use_latching must be boolean: {self.use_latching}"),
            (self.latching_threshold_pct > 0, f"latching_threshold_pct must be positive: {self.latching_threshold_pct}"),
            (self.latching_expiration_hours > 0, f"latching_expiration_hours must be positive: {self.latching_expiration_hours}"),
            (self.min_profit_pct > 0, f"min_profit_pct must be positive: {self.min_profit_pct}"),
            (self.cooldown_period > 0, f"cooldown_period must be positive: {self.cooldown_period}"),
            (self.ema_period > 0, f"ema_period must be positive: {self.ema_period}")
        ]
        
        for valid, msg in validations:
            if not valid:
                raise ValueError(msg)

    def _load_positions(self) -> Dict[str, float]:
        """Load positions from file."""
        try:
            if self.position_file.exists():
                with self.position_file.open('r') as f:
                    positions = json.load(f)
                    for ticker in positions:
                        POSITIONS_ACTIVE.labels(ticker=ticker).set(1)
                    return positions
            return {}
        except Exception as e:
            self.logger.error(f"Failed to load positions: {e}")
            return {}

    def _load_latched_state(self) -> Dict[str, Dict[str, Any]]:
        """Load latched signals from file."""
        try:
            latched_file = self.position_file.parent / 'latched_state.json'
            if latched_file.exists():
                with latched_file.open('r') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            self.logger.error(f"Failed to load latched state: {e}")
            return {}

    def _save_positions(self):
        """Save positions and latched state to files."""
        with QMutexLocker(self.mutex):
            try:
                self.position_file.parent.mkdir(parents=True, exist_ok=True)
                
                with self.position_file.open('w') as f:
                    json.dump(self.position, f, indent=2)
                
                latched_file = self.position_file.parent / 'latched_state.json'
                with latched_file.open('w') as f:
                    json.dump(self.latched_state, f, indent=2)
                    
            except Exception as e:
                self.logger.error(f"Failed to save positions: {e}")

    async def evaluate(self, ticker: str, historical_bars: List[Dict], price: float) -> Optional[TradeSignal]:
        """
        Evaluate price against thresholds with all enhanced features.
        
        Args:
            ticker: Stock symbol
            historical_bars: Historical price data
            price: Current market price
            
        Returns:
            TradeSignal if action needed, else None
        """
        try:
            THRESHOLD_ATTEMPTS.labels(ticker=ticker).inc()
            
            # Check cooldown
            current_time = time.time()
            if self._in_cooldown(ticker, current_time):
                return None
            
            # Store historical bars in cache
            self.historical_bars_cache[ticker] = historical_bars
            
            # Check data sufficiency
            if not self._has_sufficient_data(ticker, historical_bars):
                return None
            
            # Calculate indicators and signals
            confidence = await self._calculate_confidence(ticker, price, historical_bars)
            new_action = self._determine_action(ticker, price, historical_bars)
            
            # Get current latched state
            current_state = self.latched_state.get(ticker, {
                'action': None, 
                'price': None, 
                'timestamp': None
            })
            
            # Apply filters and generate signal
            signal = await self._process_signal(
                ticker, price, new_action, confidence, current_state, historical_bars
            )
            
            if signal:
                self._update_state(ticker, new_action, price, current_time)
                SIGNALS_GENERATED.labels(ticker=ticker, action=signal.action.name).inc()
                self.logger.info(f"Signal generated: {signal.to_dict()}")
            
            return signal
            
        except Exception as e:
            self.logger.error(f"Evaluation failed for {ticker}: {e}", exc_info=True)
            return None

    def _in_cooldown(self, ticker: str, current_time: float) -> bool:
        """Check if ticker is in cooldown period."""
        if ticker in self.last_signal_time:
            time_since_last = current_time - self.last_signal_time[ticker]
            return time_since_last < self.cooldown_period
        return False

    def _has_sufficient_data(self, ticker: str, historical_bars: List[Dict]) -> bool:
        """Check if we have enough historical data."""
        min_required = max(2, self.ema_period if self.use_ema_filter else 2)
        if not historical_bars or len(historical_bars) < min_required:
            self.logger.warning(f"Insufficient data for {ticker}: {len(historical_bars)} bars")
            return False
        return True

    async def _process_signal(self, ticker: str, price: float, new_action: str, 
                              confidence: float, current_state: Dict, 
                              historical_bars: List[Dict]) -> Optional[TradeSignal]:
        """Process signal with latching and filters."""
        
        # Check latching conditions
        if self.use_latching and not self._should_generate_signal(ticker, new_action, price, current_state):
            self._log_suppression(ticker, new_action, price, current_state)
            return None
        
        # Apply EMA filter
        if self.use_ema_filter and not self._passes_ema_filter(ticker, price, historical_bars):
            self.logger.debug(f"Signal suppressed by EMA filter for {ticker}")
            return None
        
        # Build final signal
        return self._build_signal(ticker, new_action, price, confidence)

    def _should_generate_signal(self, ticker: str, new_action: str, price: float, 
                               current_state: Dict[str, Any]) -> bool:
        """Determine if signal should be generated based on latching."""
        current_action = current_state.get('action')
        last_price = current_state.get('price')
        last_timestamp = current_state.get('timestamp')
        
        # New or different action
        if current_action is None or new_action != current_action:
            return True
        
        # Check expiration
        if not self._is_latch_valid(last_timestamp):
            LATCH_EXPIRED.labels(ticker=ticker).inc()
            return True
        
        # Check price threshold
        if last_price and self._price_change_exceeds_threshold(price, last_price):
            return True
        
        return False

    def _is_latch_valid(self, last_timestamp: Optional[float]) -> bool:
        """Check if latched signal is still valid."""
        if last_timestamp is None:
            return False
        hours_elapsed = (time.time() - last_timestamp) / 3600
        return hours_elapsed <= self.latching_expiration_hours

    def _price_change_exceeds_threshold(self, current_price: float, last_price: float) -> bool:
        """Check if price change exceeds threshold."""
        if not last_price:
            return True
        pct_change = abs(current_price - last_price) / last_price * 100
        return pct_change >= self.latching_threshold_pct

    def _passes_ema_filter(self, ticker: str, price: float, historical_bars: List[Dict]) -> bool:
        """Apply EMA trend filter."""
        ema = self._calculate_ema(historical_bars, self.ema_period)
        return price > ema if ema > 0 else True

    def _determine_action(self, ticker: str, price: float, historical_bars: List[Dict]) -> str:
        """Determine trading action with volume adjustment."""
        try:
            # Get thresholds (possibly adjusted by volume)
            buy_thresh, sell_thresh = self._get_adjusted_thresholds(historical_bars)
            
            # Check buy condition
            if price < buy_thresh and ticker not in self.position:
                return Action.BUY.name
            
            # Check sell condition with profit requirement
            if ticker in self.position:
                entry_price = self.position[ticker]
                min_sell_price = entry_price * (1 + self.min_profit_pct / 100)
                
                if price > sell_thresh and price > min_sell_price:
                    return Action.SELL.name
            
            return Action.HOLD.name
            
        except Exception as e:
            self.logger.error(f"Failed to determine action: {e}")
            return Action.HOLD.name

    def _get_adjusted_thresholds(self, historical_bars: List[Dict]) -> tuple:
        """Get thresholds adjusted by volume if enabled."""
        if not self.use_volume_adjustment or not historical_bars:
            return self.buy_threshold, self.sell_threshold
        
        # Get latest volume
        volume = historical_bars[-1].get('volume', 0)
        volume_factor = min(volume / 1_000_000, 0.5)  # Cap at 50% adjustment
        
        # Adjust thresholds based on volume
        adjusted_buy = self.buy_threshold * (1 - volume_factor * 0.1)
        adjusted_sell = self.sell_threshold * (1 + volume_factor * 0.1)
        
        return adjusted_buy, adjusted_sell

    def _build_signal(self, ticker: str, action: str, price: float, 
                     confidence: float) -> Optional[TradeSignal]:
        """Build trade signal and update positions."""
        if action == Action.BUY.name and ticker not in self.position:
            self._update_position(ticker, price)
            return TradeSignal(ticker, Action.BUY, price, confidence)
            
        elif action == Action.SELL.name and ticker in self.position:
            entry_price = self.position[ticker]
            profit_pct = (price - entry_price) / entry_price * 100
            self._clear_position(ticker)
            return TradeSignal(
                ticker, Action.SELL, price, confidence,
                metadata={'profit_pct': profit_pct, 'entry_price': entry_price}
            )
        
        return None

    async def _calculate_confidence(self, ticker: str, price: float, 
                                   historical_bars: List[Dict]) -> float:
        """Calculate dynamic confidence score."""
        try:
            base_confidence = 0.7
            
            # Get volatility factor
            volatility = self._calculate_volatility(historical_bars)
            volatility_penalty = min(volatility * 0.2, 0.3)
            
            # Get distance from threshold
            buy_thresh, sell_thresh = self._get_adjusted_thresholds(historical_bars)
            
            if price < buy_thresh:
                distance_bonus = min((buy_thresh - price) / buy_thresh * 0.3, 0.3)
            elif price > sell_thresh:
                distance_bonus = min((price - sell_thresh) / sell_thresh * 0.3, 0.3)
            else:
                distance_bonus = 0
            
            # Calculate final confidence
            confidence = base_confidence + distance_bonus - volatility_penalty
            return max(0.1, min(1.0, confidence))
            
        except Exception as e:
            self.logger.warning(f"Confidence calculation failed: {e}")
            return 0.5

    def _calculate_volatility(self, historical_bars: List[Dict]) -> float:
        """Calculate price volatility."""
        if not historical_bars or len(historical_bars) < 5:
            return 0.0
        
        try:
            closes = [bar['close'] for bar in historical_bars[-20:]]
            returns = pd.Series(closes).pct_change().dropna()
            return float(returns.std()) if len(returns) > 0 else 0.0
        except Exception:
            return 0.0

    def _calculate_ema(self, historical_bars: List[Dict], period: int) -> float:
        """Calculate exponential moving average."""
        if not historical_bars or len(historical_bars) < period:
            return 0.0
        
        closes = [bar['close'] for bar in historical_bars[-period:]]
        alpha = 2 / (period + 1)
        
        ema = closes[0]
        for close in closes[1:]:
            ema = alpha * close + (1 - alpha) * ema
        
        return ema

    def _update_position(self, ticker: str, price: float):
        """Track bought position."""
        self.position[ticker] = price
        POSITIONS_ACTIVE.labels(ticker=ticker).set(1)
        self._save_positions()
        self.logger.info(f"Position opened: {ticker} @ ${price:.2f}")

    def _clear_position(self, ticker: str):
        """Clear sold position."""
        if ticker in self.position:
            del self.position[ticker]
            POSITIONS_ACTIVE.labels(ticker=ticker).set(0)
            self._save_positions()
            self.logger.info(f"Position closed: {ticker}")

    def _update_state(self, ticker: str, action: str, price: float, timestamp: float):
        """Update latched state and timing."""
        self.latched_state[ticker] = {
            'action': action,
            'price': price,
            'timestamp': timestamp
        }
        self.last_signal_time[ticker] = timestamp
        self._save_positions()

    def _log_suppression(self, ticker: str, action: str, price: float, current_state: Dict):
        """Log why signal was suppressed."""
        if action == current_state.get('action'):
            LATCH_SUPPRESSED.labels(ticker=ticker).inc()
            self.logger.debug(f"Signal suppressed by latching for {ticker}")
        elif current_state.get('price') and not self._price_change_exceeds_threshold(price, current_state['price']):
            LATCH_THRESHOLD_SUPPRESSED.labels(ticker=ticker).inc()
            self.logger.debug(f"Signal suppressed by price threshold for {ticker}")

    def update_thresholds(self, buy_threshold: float, sell_threshold: float):
        """Dynamically update thresholds."""
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        self.logger.info(f"Thresholds updated: buy={buy_threshold}, sell={sell_threshold}")

    def get_positions(self) -> Dict[str, float]:
        """Get current positions."""
        return self.position.copy()

    def get_statistics(self) -> Dict[str, Any]:
        """Get strategy statistics."""
        return {
            'positions_count': len(self.position),
            'positions': self.position,
            'latched_signals': len(self.latched_state),
            'config': self.config
        }


# ==================== STRATEGY RUNNER THREAD ====================

class StrategyRunnerThread(QThread):
    """Thread to run strategy evaluation asynchronously."""
    
    signal_generated = pyqtSignal(dict)  # Emits trade signals
    status_update = pyqtSignal(str)      # Status messages
    error_occurred = pyqtSignal(str)      # Error messages
    
    def __init__(self, strategy: EnhancedThresholdStrategy, parent=None):
        super().__init__(parent)
        self.strategy = strategy
        self.running = False
        self.tickers_to_evaluate = []
        self.historical_data = {}
        self.mutex = QMutex()
        self.loop = None
        
    def add_evaluation(self, ticker: str, price: float, historical_bars: List[Dict]):
        """Queue a ticker for evaluation."""
        with QMutexLocker(self.mutex):
            self.tickers_to_evaluate.append({
                'ticker': ticker,
                'price': price,
                'historical_bars': historical_bars,
                'timestamp': time.time()
            })
    
    def run(self):
        """Main thread loop."""
        self.running = True
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        try:
            self.loop.run_until_complete(self._run_evaluations())
        except Exception as e:
            self.error_occurred.emit(f"Strategy runner error: {e}")
        finally:
            self.loop.close()
    
    async def _run_evaluations(self):
        """Process evaluation queue."""
        while self.running:
            # Get items to evaluate
            with QMutexLocker(self.mutex):
                items = self.tickers_to_evaluate.copy()
                self.tickers_to_evaluate.clear()
            
            # Evaluate each item
            for item in items:
                try:
                    signal = await self.strategy.evaluate(
                        item['ticker'],
                        item['historical_bars'],
                        item['price']
                    )
                    
                    if signal:
                        self.signal_generated.emit(signal.to_dict())
                        
                except Exception as e:
                    self.error_occurred.emit(f"Evaluation error for {item['ticker']}: {e}")
            
            # Sleep briefly
            await asyncio.sleep(0.1)
    
    def stop(self):
        """Stop the thread."""
        self.running = False
        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)


# ==================== STRATEGY CONTROL WIDGET ====================

class StrategyControlWidget(QWidget):
    """Widget for controlling the enhanced strategy."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.strategy = None
        self.runner_thread = None
        self.setup_ui()
        self.initialize_strategy()
        
    def setup_ui(self):
        """Create the control interface."""
        layout = QVBoxLayout(self)
        
        # === Strategy Configuration ===
        config_group = QGroupBox("Strategy Configuration")
        config_layout = QVBoxLayout()
        
        # Thresholds
        thresh_layout = QHBoxLayout()
        thresh_layout.addWidget(QLabel("Buy Threshold:"))
        self.buy_threshold_spin = QDoubleSpinBox()
        self.buy_threshold_spin.setRange(1, 10000)
        self.buy_threshold_spin.setValue(100)
        self.buy_threshold_spin.setSingleStep(10)
        thresh_layout.addWidget(self.buy_threshold_spin)
        
        thresh_layout.addWidget(QLabel("Sell Threshold:"))
        self.sell_threshold_spin = QDoubleSpinBox()
        self.sell_threshold_spin.setRange(1, 10000)
        self.sell_threshold_spin.setValue(200)
        self.sell_threshold_spin.setSingleStep(10)
        thresh_layout.addWidget(self.sell_threshold_spin)
        
        self.update_thresholds_btn = QPushButton("Update Thresholds")
        self.update_thresholds_btn.clicked.connect(self.update_thresholds)
        thresh_layout.addWidget(self.update_thresholds_btn)
        config_layout.addLayout(thresh_layout)
        
        # Feature toggles
        features_layout = QHBoxLayout()
        self.use_latching_check = QCheckBox("Use Latching")
        self.use_latching_check.setChecked(True)
        features_layout.addWidget(self.use_latching_check)
        
        self.use_volume_check = QCheckBox("Volume Adjustment")
        self.use_volume_check.setChecked(True)
        features_layout.addWidget(self.use_volume_check)
        
        self.use_ema_check = QCheckBox("EMA Filter")
        self.use_ema_check.setChecked(True)
        features_layout.addWidget(self.use_ema_check)
        config_layout.addLayout(features_layout)
        
        # Parameters
        params_layout = QHBoxLayout()
        params_layout.addWidget(QLabel("Min Profit %:"))
        self.min_profit_spin = QDoubleSpinBox()
        self.min_profit_spin.setRange(0, 100)
        self.min_profit_spin.setValue(5.0)
        params_layout.addWidget(self.min_profit_spin)
        
        params_layout.addWidget(QLabel("Latch Threshold %:"))
        self.latch_threshold_spin = QDoubleSpinBox()
        self.latch_threshold_spin.setRange(0, 100)
        self.latch_threshold_spin.setValue(10.0)
        params_layout.addWidget(self.latch_threshold_spin)
        
        params_layout.addWidget(QLabel("Cooldown (s):"))
        self.cooldown_spin = QSpinBox()
        self.cooldown_spin.setRange(0, 3600)
        self.cooldown_spin.setValue(300)
        params_layout.addWidget(self.cooldown_spin)
        config_layout.addLayout(params_layout)
        
        config_group.setLayout(config_layout)
        layout.addWidget(config_group)
        
        # === Positions Table ===
        positions_group = QGroupBox("Active Positions")
        positions_layout = QVBoxLayout()
        
        self.positions_table = QTableWidget(0, 4)
        self.positions_table.setHorizontalHeaderLabels(["Ticker", "Entry Price", "Current Price", "Profit %"])
        self.positions_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        positions_layout.addWidget(self.positions_table)
        
        positions_group.setLayout(positions_layout)
        layout.addWidget(positions_group)
        
        # === Signals Log ===
        signals_group = QGroupBox("Recent Signals")
        signals_layout = QVBoxLayout()
        
        self.signals_log = QTextEdit()
        self.signals_log.setReadOnly(True)
        self.signals_log.setMaximumHeight(150)
        signals_layout.addWidget(self.signals_log)
        
        signals_group.setLayout(signals_layout)
        layout.addWidget(signals_group)
        
        # === Control Buttons ===
        control_layout = QHBoxLayout()
        
        self.start_btn = QPushButton("Start Strategy")
        self.start_btn.clicked.connect(self.toggle_strategy)
        control_layout.addWidget(self.start_btn)
        
        self.clear_positions_btn = QPushButton("Clear All Positions")
        self.clear_positions_btn.clicked.connect(self.clear_all_positions)
        control_layout.addWidget(self.clear_positions_btn)
        
        self.reload_btn = QPushButton("Reload Config")
        self.reload_btn.clicked.connect(self.reload_configuration)
        control_layout.addWidget(self.reload_btn)
        
        layout.addLayout(control_layout)
    
    def initialize_strategy(self):
        """Initialize the strategy with current settings."""
        config = self.get_config_from_ui()
        
        self.strategy = EnhancedThresholdStrategy(
            buy_threshold=self.buy_threshold_spin.value(),
            sell_threshold=self.sell_threshold_spin.value(),
            position_file='positions.json',
            config=config
        )
        
        # Create runner thread
        self.runner_thread = StrategyRunnerThread(self.strategy, self)
        self.runner_thread.signal_generated.connect(self.on_signal_generated)
        self.runner_thread.status_update.connect(self.log_message)
        self.runner_thread.error_occurred.connect(self.log_error)
    
    def get_config_from_ui(self) -> Dict[str, Any]:
        """Get configuration from UI elements."""
        return {
            'min_profit_pct': self.min_profit_spin.value(),
            'use_latching': self.use_latching_check.isChecked(),
            'latching_threshold_pct': self.latch_threshold_spin.value(),
            'latching_expiration_hours': 24.0,
            'cooldown_period': self.cooldown_spin.value(),
            'use_volume_adjustment': self.use_volume_check.isChecked(),
            'use_ema_filter': self.use_ema_check.isChecked(),
            'ema_period': 20
        }
    
    def update_thresholds(self):
        """Update strategy thresholds."""
        if self.strategy:
            self.strategy.update_thresholds(
                self.buy_threshold_spin.value(),
                self.sell_threshold_spin.value()
            )
            self.log_message(f"Thresholds updated: Buy={self.buy_threshold_spin.value()}, Sell={self.sell_threshold_spin.value()}")
    
    def toggle_strategy(self):
        """Start/stop the strategy runner."""
        if self.runner_thread and self.runner_thread.isRunning():
            self.runner_thread.stop()
            self.runner_thread.wait()
            self.start_btn.setText("Start Strategy")
            self.log_message("Strategy stopped")
        else:
            if not self.runner_thread:
                self.initialize_strategy()
            self.runner_thread.start()
            self.start_btn.setText("Stop Strategy")
            self.log_message("Strategy started")
    
    def evaluate_ticker(self, ticker: str, price: float, historical_bars: List[Dict]):
        """Queue ticker for evaluation."""
        if self.runner_thread and self.runner_thread.isRunning():
            self.runner_thread.add_evaluation(ticker, price, historical_bars)
    
    def on_signal_generated(self, signal_dict: Dict[str, Any]):
        """Handle generated trade signal."""
        # Log the signal
        timestamp = datetime.fromtimestamp(signal_dict['timestamp']).strftime('%H:%M:%S')
        self.signals_log.append(
            f"[{timestamp}] {signal_dict['action']} {signal_dict['ticker']} "
            f"@ ${signal_dict['price']:.2f} (Conf: {signal_dict['confidence']:.2%})"
        )
        
        # Update positions table
        self.update_positions_table()
        
        # Emit to parent if needed
        if self.parent() and hasattr(self.parent(), 'handle_strategy_signal'):
            self.parent().handle_strategy_signal(signal_dict)
    
    def update_positions_table(self):
        """Update the positions display."""
        if not self.strategy:
            return
        
        positions = self.strategy.get_positions()
        self.positions_table.setRowCount(len(positions))
        
        for row, (ticker, entry_price) in enumerate(positions.items()):
            self.positions_table.setItem(row, 0, QTableWidgetItem(ticker))
            self.positions_table.setItem(row, 1, QTableWidgetItem(f"${entry_price:.2f}"))
            # Current price and profit would need to be fetched from market data
            self.positions_table.setItem(row, 2, QTableWidgetItem("--"))
            self.positions_table.setItem(row, 3, QTableWidgetItem("--"))
    
    def clear_all_positions(self):
        """Clear all positions."""
        if self.strategy:
            self.strategy.position.clear()
            self.strategy._save_positions()
            self.update_positions_table()
            self.log_message("All positions cleared")
    
    def reload_configuration(self):
        """Reload strategy configuration from UI."""
        if self.strategy:
            config = self.get_config_from_ui()
            self.strategy.config.update(config)
            self.strategy._validate_config()
            self.log_message("Configuration reloaded")
    
    def log_message(self, message: str):
        """Log a status message."""
        timestamp = datetime.now().strftime('%H:%M:%S')
        self.signals_log.append(f"[{timestamp}] {message}")
    
    def log_error(self, error: str):
        """Log an error message."""
        timestamp = datetime.now().strftime('%H:%M:%S')
        self.signals_log.append(f"[{timestamp}] ERROR: {error}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get strategy statistics."""
        if self.strategy:
            return self.strategy.get_statistics()
        return {}


# ==================== INTEGRATION TAB ====================

class IntegratedStrategyTab(QWidget):
    """Tab for integrated strategy system in CyberNinja Trader."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_ui()
        
    def setup_ui(self):
        """Create the integrated interface."""
        layout = QVBoxLayout(self)
        
        # Title
        title = QLabel("🎯 Integrated Strategy System")
        title.setStyleSheet("font-size: 16px; font-weight: bold;")
        layout.addWidget(title)
        
        # Add tab widget for multiple strategies
        self.strategy_tabs = QTabWidget()
        
        # Enhanced Threshold Strategy
        self.threshold_strategy_widget = StrategyControlWidget(self)
        self.strategy_tabs.addTab(self.threshold_strategy_widget, "Threshold Strategy")
        
        # Placeholder for future strategies
        placeholder = QLabel("Additional strategies can be added here")
        placeholder.setStyleSheet("padding: 20px;")
        self.strategy_tabs.addTab(placeholder, "More Strategies")
        
        layout.addWidget(self.strategy_tabs)
        
    def handle_market_data(self, ticker: str, data: Dict[str, Any]):
        """Process incoming market data for strategy evaluation."""
        # Extract price and prepare historical bars
        price = data.get('price')
        if not price:
            return
        
        # Create minimal historical bars from current data
        # In production, this would come from a proper data source
        historical_bars = [
            {
                'close': price,
                'volume': data.get('volume', 0),
                'timestamp': datetime.now().isoformat()
            }
        ]
        
        # Send to active strategy
        if self.threshold_strategy_widget.runner_thread and self.threshold_strategy_widget.runner_thread.isRunning():
            self.threshold_strategy_widget.evaluate_ticker(ticker, price, historical_bars)
    
    def handle_strategy_signal(self, signal: Dict[str, Any]):
        """Handle signal from strategy."""
        # Forward to main application
        if self.parent():
            self.parent().log(f"🎯 Strategy Signal: {signal['action']} {signal['ticker']} @ ${signal['price']:.2f}")


if __name__ == '__main__':
    # Test the integrated strategy
    import sys
    from PyQt5.QtWidgets import QApplication
    
    app = QApplication(sys.argv)
    
    # Create test window
    window = IntegratedStrategyTab()
    window.setWindowTitle("Strategy System Test")
    window.resize(800, 600)
    window.show()
    
    # Test with sample data
    test_data = {
        'ticker': 'AAPL',
        'price': 150.50,
        'volume': 1000000
    }
    window.handle_market_data('AAPL', test_data)
    
    sys.exit(app.exec_())