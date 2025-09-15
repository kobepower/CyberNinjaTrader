#!/usr/bin/env python3
"""
Complete Integrated Trading System
Combines CyberNinja Trader Enhanced, Strategy Consumer, and all strategies
"""

import json
import logging
import asyncio
import time
import datetime
import sys
import os
from pathlib import Path
from typing import Dict, Any, List, Optional
import numpy as np
import pandas as pd
import yfinance as yf
import aio_pika
import threading
from dataclasses import dataclass
from enum import Enum, auto
import socket

from PyQt5.QtCore import QObject, pyqtSignal, QThread, QProcess, QMutex, QMutexLocker, pyqtSlot
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QTableWidget, 
    QTableWidgetItem, QHeaderView, QPushButton, QSpinBox, 
    QDoubleSpinBox, QCheckBox, QGroupBox, QTextEdit, QTabWidget,
    QComboBox, QProgressBar, QApplication, QMainWindow
)

# Try to import Finnhub
try:
    import finnhub
    FINNHUB_AVAILABLE = True
except ImportError:
    FINNHUB_AVAILABLE = False
    logging.warning("Finnhub not installed. Some features will be limited.")

# Try to import Prometheus metrics
try:
    from prometheus_client import Counter, Histogram, Gauge
    MESSAGE_PROCESS_LATENCY = Histogram(
        'strategy_consumer_process_latency_seconds',
        'Time to process a price message',
        ['ticker']
    )
    PROCESSING_ERRORS = Counter(
        'strategy_consumer_errors_total',
        'Total errors in strategy consumer',
        ['ticker']
    )
    REVERSAL_ATTEMPTS = Gauge(
        'reversal_attempts_total',
        'Total reversal detection attempts per ticker',
        ['ticker']
    )
    SIGNALS_GENERATED = Counter(
        'signals_generated_total',
        'Total trade signals generated',
        ['ticker', 'action']
    )
    POSITIONS_ACTIVE = Gauge(
        'positions_active_total',
        'Number of active positions',
        ['ticker']
    )
except ImportError:
    # Dummy metrics if Prometheus not installed
    class DummyMetric:
        def labels(self, **kwargs):
            return self
        def inc(self):
            pass
        def set(self, value):
            pass
        def observe(self, value):
            pass
    
    MESSAGE_PROCESS_LATENCY = DummyMetric()
    PROCESSING_ERRORS = DummyMetric()
    REVERSAL_ATTEMPTS = DummyMetric()
    SIGNALS_GENERATED = DummyMetric()
    POSITIONS_ACTIVE = DummyMetric()

# ==================== LOGGING SETUP ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(threadName)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== REVERSAL UTILITIES ====================
def is_reversal(
    current_bar: Dict,
    prev_day_low: float,
    retracement_threshold: float = 0.2,
    prev_low_tolerance: float = 0.003,
    min_volume_factor: float = 1.5,
    average_volume: float = 1000.0
) -> bool:
    """Check if current bar represents a reversal pattern."""
    if not current_bar or prev_day_low <= 0:
        return False
    
    # Check if we're near previous day low
    price_diff = abs(current_bar['low'] - prev_day_low) / prev_day_low
    if price_diff > prev_low_tolerance:
        return False
    
    # Check for bullish reversal pattern
    body = current_bar['close'] - current_bar['open']
    range_ = current_bar['high'] - current_bar['low']
    
    if range_ <= 0:
        return False
    
    # Bullish candle with lower shadow
    if body > 0 and (current_bar['open'] - current_bar['low']) / range_ > retracement_threshold:
        # Check volume confirmation
        if current_bar['volume'] > average_volume * min_volume_factor:
            return True
    
    return False

def is_confirmation(
    prev_bar: Dict,
    current_bar: Dict,
    min_body_factor: float = 0.001,
    min_upward_move: float = 0.001
) -> bool:
    """Check if current bar confirms the reversal."""
    if not prev_bar or not current_bar:
        return False
    
    # Current bar should be bullish
    current_body = current_bar['close'] - current_bar['open']
    if current_body < current_bar['close'] * min_body_factor:
        return False
    
    # Should be moving up from previous bar
    if current_bar['close'] < prev_bar['close'] * (1 + min_upward_move):
        return False
    
    return True

def compute_average_volume(bars: List[Dict], lookback: int = 20) -> float:
    """Compute average volume over lookback period."""
    volumes = [bar['volume'] for bar in bars[-lookback:] 
              if 'volume' in bar and bar['volume'] is not None]
    return (sum(volumes) / len(volumes)) if volumes else 1000.0

# ==================== STRATEGY CLASSES ====================

class Action(Enum):
    BUY = auto()
    SELL = auto()
    HOLD = auto()

@dataclass
class TradeSignal:
    """Structured trade signal."""
    ticker: str
    action: Action
    price: float
    confidence: float
    timestamp: float = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()
        if self.metadata is None:
            self.metadata = {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            'ticker': self.ticker,
            'action': self.action.name,
            'price': self.price,
            'confidence': self.confidence,
            'timestamp': self.timestamp,
            'metadata': self.metadata
        }

class Strategy:
    """Base strategy class."""
    async def evaluate(self, ticker: str, price: float, volume: float = 0) -> Any:
        raise NotImplementedError

class ThresholdStrategy(Strategy):
    """Threshold-based trading strategy."""
    
    def __init__(self, buy_threshold: float = 100, sell_threshold: float = 200, 
                 config: Optional[Dict] = None):
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        self.config = config or {}
        self.position = {}
        self.logger = logging.getLogger('ThresholdStrategy')
        
    async def evaluate(self, ticker: str, price: float, volume: float = 0) -> Any:
        """Evaluate price against thresholds using percentage-based logic."""
        try:
            # Store price history for percentage calculations
            if not hasattr(self, 'price_history'):
                self.price_history = {}
            if ticker not in self.price_history:
                self.price_history[ticker] = []
            
            self.price_history[ticker].append(price)
            if len(self.price_history[ticker]) > 20:  # Keep last 20 prices
                self.price_history[ticker].pop(0)
            
            # Need at least 5 prices for analysis
            if len(self.price_history[ticker]) < 5:
                return None
                
            # Calculate recent average and price change
            recent_prices = self.price_history[ticker][-5:]
            avg_price = sum(recent_prices) / len(recent_prices)
            price_change = (price - avg_price) / avg_price
            
            # Generate signals based on significant price movements
            if price_change < -0.02 and ticker not in self.position:  # 2% drop - buy opportunity
                self.position[ticker] = price
                signal = {
                    'action': 'buy',
                    'ticker': ticker,
                    'price': price,
                    'type': 'THRESHOLD',
                    'confidence': min(95, 70 + abs(price_change) * 1000),
                    'progress': int(70 + abs(price_change) * 1000),
                    'volume': volume,
                    'timestamp': datetime.datetime.now().isoformat()
                }
                self.logger.info(f"BUY signal for {ticker} at {price} (drop: {price_change:.2%})")
                return signal
                
            elif price_change > 0.015 and ticker in self.position:  # 1.5% gain - take profit
                entry_price = self.position[ticker]
                profit = (price - entry_price) / entry_price
                del self.position[ticker]
                signal = {
                    'action': 'sell',
                    'ticker': ticker,
                    'price': price,
                    'type': 'BREAKOUT',
                    'confidence': min(90, 60 + profit * 500),
                    'progress': int(60 + profit * 500),
                    'volume': volume,
                    'profit': profit,
                    'timestamp': datetime.datetime.now().isoformat()
                }
                self.logger.info(f"SELL signal for {ticker} at {price} (profit: {profit:.2%})")
                return signal
                
            # Volume spike signals
            elif volume > 1000:
                signal = {
                    'action': 'monitor',
                    'ticker': ticker,
                    'price': price,
                    'type': 'VOLUME_SPIKE',
                    'confidence': min(85, 50 + (volume / 100)),
                    'progress': int(min(100, 50 + (volume / 50))),
                    'volume': volume,
                    'timestamp': datetime.datetime.now().isoformat()
                }
                self.logger.info(f"VOLUME signal for {ticker} at {price} (vol: {volume})")
                return signal
                
        except Exception as e:
            self.logger.error(f"Evaluation error: {e}")
        return None

class ReversalStrategy(Strategy):
    """Reversal detection strategy."""
    
    def __init__(self, config: Dict[str, Any], historical_bars: Dict[str, List[Dict]],
                 prev_day_low_cache: Optional[Dict[str, float]] = None):
        self.config = config
        self.historical_bars = historical_bars
        self.prev_day_low_cache = prev_day_low_cache or {}
        self.reversal_config = config.get('reversal_config', {
            'retracement_threshold': 0.2,
            'prev_low_tolerance': 0.003,
            'min_volume_factor': 1.5,
            'min_body_factor': 0.001,
            'min_upward_move': 0.001,
            'cooldown_period': 300
        })
        self.last_signal_time: Dict[str, float] = {}
        self.logger = logging.getLogger('ReversalStrategy')

    async def evaluate(self, ticker: str, price: float, volume: float = 0) -> Any:
        """Evaluate for reversal patterns."""
        if ticker not in self.historical_bars or len(self.historical_bars[ticker]) < 2:
            return None

        current_time = time.time()
        if ticker in self.last_signal_time:
            if current_time - self.last_signal_time[ticker] < self.reversal_config['cooldown_period']:
                return None

        current_bar = {
            'open': price,
            'high': price,
            'low': price,
            'close': price,
            'volume': volume,
            'ticker': ticker,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        prev_day_low = self._get_prev_day_low(ticker)
        if prev_day_low == 0.0:
            prev_day_low = current_bar['low']

        average_volume = compute_average_volume(self.historical_bars[ticker])
        
        REVERSAL_ATTEMPTS.labels(ticker=ticker).inc()
        
        if is_reversal(
            current_bar, prev_day_low,
            retracement_threshold=self.reversal_config['retracement_threshold'],
            prev_low_tolerance=self.reversal_config['prev_low_tolerance'],
            min_volume_factor=self.reversal_config['min_volume_factor'],
            average_volume=average_volume
        ):
            if len(self.historical_bars[ticker]) >= 2:
                prev_bar = self.historical_bars[ticker][-2]
                if is_confirmation(
                    prev_bar, current_bar,
                    min_body_factor=self.reversal_config['min_body_factor'],
                    min_upward_move=self.reversal_config['min_upward_move']
                ):
                    signal = {
                        'action': 'buy',
                        'ticker': ticker,
                        'price': current_bar['close'],
                        'type': 'reversal',
                        'timestamp': datetime.datetime.now().isoformat()
                    }
                    self.last_signal_time[ticker] = current_time
                    self.logger.info(f"Reversal signal for {ticker}: {signal}")
                    return signal
        return None

    def _get_prev_day_low(self, ticker: str) -> float:
        """Get previous day's low price."""
        cached = self.prev_day_low_cache.get(ticker)
        if cached is not None and cached > 0:
            return float(cached)
        
        bars = self.historical_bars.get(ticker, [])
        if not bars:
            return 0.0
        
        today = datetime.datetime.now().date()
        prev_day_bars = [
            bar for bar in bars
            if datetime.datetime.fromisoformat(bar['timestamp']).date() < today
        ]
        
        if not prev_day_bars:
            return 0.0
        
        return float(min(bar.get('low', float('inf')) for bar in prev_day_bars))

# ==================== STRATEGY CONSUMER ====================

class StrategyConsumer:
    """Main strategy consumer that processes price messages."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger('StrategyConsumer')
        self._validate_config()
        self.historical_bars: Dict[str, List[Dict]] = {
            ticker: [] for ticker in config['tickers']
        }
        self.prev_day_low: Dict[str, float] = {}
        self.connection = None
        self.strategies: List[Strategy] = self._load_strategies()
        
        if FINNHUB_AVAILABLE and 'finnhub_api_key' in config:
            try:
                self.finnhub_client = finnhub.Client(api_key=config['finnhub_api_key'])
                self.logger.info("Connected to Finnhub API")
            except Exception as e:
                self.logger.error(f"Failed to initialize Finnhub: {e}")
                self.finnhub_client = None
        else:
            self.finnhub_client = None
            
        self.last_fetch_time = 0
        self.running = False

    def _validate_config(self):
        """Validate configuration."""
        required = ['rabbitmq', 'tickers']
        for key in required:
            if key not in self.config:
                raise ValueError(f"Missing config key: {key}")

    def _load_strategies(self) -> List[Strategy]:
        """Load and initialize strategies."""
        strategies = []
        
        # Load threshold strategy
        thresholds = self.config.get('strategy_thresholds', {})
        strategies.append(
            ThresholdStrategy(
                buy_threshold=thresholds.get('buy', 100),
                sell_threshold=thresholds.get('sell', 200),
                config=self.config.get('strategy_config', {})
            )
        )
        
        # Load reversal strategy
        strategies.append(
            ReversalStrategy(
                self.config,
                self.historical_bars,
                self.prev_day_low
            )
        )
        
        self.logger.info(f"Loaded {len(strategies)} strategies")
        return strategies

    async def seed_historical_data_yf(self) -> None:
        """Seed historical data from Yahoo Finance."""
        total = 0
        for ticker in self.config['tickers']:
            try:
                # Get intraday data
                df = yf.download(
                    ticker,
                    period="2d",
                    interval="1m",
                    auto_adjust=False,
                    threads=False
                )
                
                if df is not None and not df.empty:
                    bars = []
                    for ts, row in df.iterrows():
                        bars.append({
                            "open": float(row["Open"]),
                            "high": float(row["High"]),
                            "low": float(row["Low"]),
                            "close": float(row["Close"]),
                            "volume": float(row.get("Volume", 0.0)),
                            "ticker": ticker,
                            "timestamp": pd.Timestamp(ts).to_pydatetime().isoformat()
                        })
                    
                    self.historical_bars[ticker] = bars[-600:]
                    total += 1
                
                # Get previous day low
                dfd = yf.download(
                    ticker,
                    period="6d",
                    interval="1d",
                    auto_adjust=False,
                    threads=False
                )
                
                if dfd is not None and not dfd.empty:
                    dfd = dfd.sort_index()
                    today = datetime.datetime.now().date()
                    last_idx = -2 if len(dfd) >= 2 and dfd.index.max().date() == today else -1
                    pdl = float(dfd["Low"].iloc[last_idx])
                    if pdl > 0:
                        self.prev_day_low[ticker] = pdl
                        
            except Exception as e:
                self.logger.warning(f"YF seed failed for {ticker}: {e}")
        
        self.logger.info(f"Seeded data for {total} tickers")

    async def fetch_historical_data(self):
        """Fetch/update historical data."""
        if not self.finnhub_client:
            return
            
        current_time = time.time()
        if current_time - self.last_fetch_time < self.config.get('fetch_interval', 60):
            return
        
        try:
            for ticker in self.config['tickers']:
                quote = self.finnhub_client.quote(symbol=ticker)
                if quote and quote.get('c') is not None:
                    bar = {
                        'open': float(quote.get('o', 0)),
                        'high': float(quote.get('h', 0)),
                        'low': float(quote.get('l', 0)),
                        'close': float(quote.get('c', 0)),
                        'volume': float(quote.get('v', 0)),
                        'ticker': ticker,
                        'timestamp': datetime.datetime.now().isoformat()
                    }
                    
                    self.historical_bars[ticker].append(bar)
                    if len(self.historical_bars[ticker]) > 600:
                        self.historical_bars[ticker] = self.historical_bars[ticker][-600:]
            
            self.last_fetch_time = current_time
        except Exception as e:
            self.logger.error(f"Data fetch failed: {e}")

    async def connect_rabbitmq(self) -> aio_pika.RobustConnection:
        """Connect to RabbitMQ."""
        max_retries = 5
        host = self.config['rabbitmq']['host']
        
        for attempt in range(max_retries):
            try:
                self.connection = await aio_pika.connect_robust(
                    f"amqp://guest:guest@{host}:5672/"
                )
                self.logger.info("Connected to RabbitMQ")
                return self.connection
            except Exception as e:
                self.logger.warning(f"RabbitMQ connection failed (attempt {attempt+1}): {e}")
                if attempt + 1 == max_retries:
                    raise
                await asyncio.sleep(2 ** attempt)

    async def process_message(self, message: aio_pika.IncomingMessage, channel: aio_pika.Channel):
        """Process incoming price message."""
        async with message.process():
            try:
                start_time = time.time()
                body = json.loads(message.body.decode("utf-8"))
                ticker = body.get("ticker")
                price = body.get("price")
                volume = body.get("volume", 0)
                
                if not ticker or price is None:
                    raise ValueError("Invalid message: missing ticker or price")
                
                self.logger.info(f"Processing: {ticker} @ ${price} (vol: {volume})")
                
                # Update historical bars
                current_bar = {
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'volume': volume,
                    'ticker': ticker,
                    'timestamp': datetime.datetime.now().isoformat()
                }
                
                if ticker not in self.historical_bars:
                    self.historical_bars[ticker] = []
                    
                self.historical_bars[ticker].append(current_bar)
                if len(self.historical_bars[ticker]) > 600:
                    self.historical_bars[ticker].pop(0)
                
                # Evaluate strategies
                for i, strategy in enumerate(self.strategies):
                    try:
                        strategy_name = strategy.__class__.__name__
                        signal = await strategy.evaluate(ticker, price, volume)
                        if signal:
                            await channel.default_exchange.publish(
                                aio_pika.Message(json.dumps(signal).encode()),
                                routing_key=self.config['rabbitmq']['signal_queue']
                            )
                            self.logger.info(f"SIGNAL: {strategy_name} generated signal: {signal}")
                            SIGNALS_GENERATED.labels(
                                ticker=ticker,
                                action=signal.get('action', 'unknown')
                            ).inc()
                        else:
                            # Debug: log when no signal is generated
                            if ticker == 'AAPL':  # Only log for AAPL to avoid spam
                                self.logger.debug(f"DEBUG: {strategy_name} evaluated {ticker} @ ${price:.2f} - no signal")
                    except Exception as e:
                        self.logger.error(f"Strategy evaluation failed for {strategy.__class__.__name__}: {e}")
                
                MESSAGE_PROCESS_LATENCY.labels(ticker=ticker).observe(
                    time.time() - start_time
                )
                
            except Exception as e:
                PROCESSING_ERRORS.labels(ticker='unknown').inc()
                self.logger.error(f"Message processing failed: {e}")

    async def start_consuming(self) -> None:
        """Start consuming messages from RabbitMQ."""
        try:
            self.running = True
            
            # Skip historical data seeding to avoid Yahoo Finance rate limiting
            # Historical data will be built from incoming price messages
            self.logger.info("Skipping historical data seeding - using real-time data only")
            
            # Connect to RabbitMQ
            connection = await self.connect_rabbitmq()
            
            async with connection:
                channel = await connection.channel()
                
                # Declare queues
                price_queue = await channel.declare_queue(
                    self.config['rabbitmq']['price_queue'],
                    durable=True
                )
                await channel.declare_queue(
                    self.config['rabbitmq']['signal_queue'],
                    durable=True
                )
                
                self.logger.info("Started consuming prices...")
                
                # Start data updater
                async def updater():
                    while self.running:
                        await self.fetch_historical_data()
                        await asyncio.sleep(self.config.get('fetch_interval', 60))
                
                update_task = asyncio.create_task(updater())
                
                try:
                    async for message in price_queue:
                        if not self.running:
                            break
                        await self.process_message(message, channel)
                finally:
                    update_task.cancel()
                    try:
                        await update_task
                    except asyncio.CancelledError:
                        pass
                        
        except Exception as e:
            self.logger.error(f"Consumer failed: {e}")
            raise
        finally:
            self.running = False

    def stop(self):
        """Stop the consumer."""
        self.running = False

# ==================== STRATEGY CONSUMER THREAD ====================

class StrategyConsumerThread(QThread):
    """Thread to run the strategy consumer."""
    
    signal_generated = pyqtSignal(dict)
    status_update = pyqtSignal(str)
    error_occurred = pyqtSignal(str)
    
    def __init__(self, config: Dict[str, Any], parent=None):
        super().__init__(parent)
        self.config = config
        self.consumer = None
        self.loop = None
        self.running = False
        
    def run(self):
        """Run the consumer in this thread."""
        self.running = True
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        try:
            self.consumer = StrategyConsumer(self.config)
            self.status_update.emit("Strategy Consumer started")
            self.loop.run_until_complete(self.consumer.start_consuming())
        except Exception as e:
            self.error_occurred.emit(f"Consumer error: {e}")
            logger.error(f"Consumer thread error: {e}")
        finally:
            self.running = False
            self.loop.close()
    
    def stop(self):
        """Stop the consumer thread."""
        self.running = False
        if self.consumer:
            self.consumer.stop()
        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)

# ==================== STRATEGY CONSUMER WIDGET ====================

class StrategyConsumerWidget(QWidget):
    """Widget for controlling the strategy consumer."""
    
    def __init__(self, config: Dict[str, Any], parent=None):
        super().__init__(parent)
        self.config = config
        self.consumer_thread = None
        self.consumer_process = None
        self.setup_ui()
        
    def setup_ui(self):
        """Create the UI."""
        layout = QVBoxLayout(self)
        
        # Title
        title = QLabel("📡 Strategy Consumer Control")
        title.setStyleSheet("font-size: 16px; font-weight: bold;")
        layout.addWidget(title)
        
        # Status
        self.status_label = QLabel("Status: Stopped")
        layout.addWidget(self.status_label)
        
        # Configuration
        config_group = QGroupBox("Configuration")
        config_layout = QVBoxLayout()
        
        # RabbitMQ settings
        rabbitmq_layout = QHBoxLayout()
        rabbitmq_layout.addWidget(QLabel("RabbitMQ Host:"))
        self.rabbitmq_host = QComboBox()
        self.rabbitmq_host.setEditable(True)
        self.rabbitmq_host.addItem(self.config.get('rabbitmq', {}).get('host', 'localhost'))
        rabbitmq_layout.addWidget(self.rabbitmq_host)
        config_layout.addLayout(rabbitmq_layout)
        
        # Tickers
        tickers_layout = QHBoxLayout()
        tickers_layout.addWidget(QLabel("Tickers:"))
        self.tickers_edit = QTextEdit()
        self.tickers_edit.setMaximumHeight(60)
        self.tickers_edit.setPlainText(
            ', '.join(self.config.get('tickers', []))
        )
        tickers_layout.addWidget(self.tickers_edit)
        config_layout.addLayout(tickers_layout)
        
        config_group.setLayout(config_layout)
        layout.addWidget(config_group)
        
        # Control buttons
        control_layout = QHBoxLayout()
        
        self.start_thread_btn = QPushButton("Start (Thread)")
        self.start_thread_btn.clicked.connect(self.toggle_consumer_thread)
        control_layout.addWidget(self.start_thread_btn)
        
        self.start_process_btn = QPushButton("Start (Process)")
        self.start_process_btn.clicked.connect(self.toggle_consumer_process)
        control_layout.addWidget(self.start_process_btn)
        
        layout.addLayout(control_layout)
        
        # Logs
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setMaximumHeight(200)
        layout.addWidget(self.log_output)
        
    def toggle_consumer_thread(self):
        """Start/stop consumer as thread."""
        if self.consumer_thread and self.consumer_thread.isRunning():
            self.consumer_thread.stop()
            self.consumer_thread.wait()
            self.consumer_thread = None
            self.start_thread_btn.setText("Start (Thread)")
            self.status_label.setText("Status: Stopped")
            self.log("Consumer thread stopped")
        else:
            # Update config from UI
            self.update_config_from_ui()
            
            # Start consumer thread
            self.consumer_thread = StrategyConsumerThread(self.config, self)
            self.consumer_thread.status_update.connect(self.log)
            self.consumer_thread.error_occurred.connect(self.log_error)
            self.consumer_thread.signal_generated.connect(self.handle_signal)
            self.consumer_thread.start()
            
            self.start_thread_btn.setText("Stop (Thread)")
            self.status_label.setText("Status: Running (Thread)")
            self.log("Consumer thread started")
    
    def toggle_consumer_process(self):
        """Start/stop consumer as separate process."""
        if self.consumer_process and self.consumer_process.state() == QProcess.Running:
            self.consumer_process.terminate()
            if not self.consumer_process.waitForFinished(5000):
                self.consumer_process.kill()
            self.consumer_process = None
            self.start_process_btn.setText("Start (Process)")
            self.status_label.setText("Status: Stopped")
            self.log("Consumer process stopped")
        else:
            # Find the strategy_consumer.py script
            script_path = Path(__file__).parent / 'strategy_consumer.py'
            if not script_path.exists():
                # Try alternative locations
                script_path = Path(__file__).parent.parent / 'backend' / 'app' / 'strategies' / 'strategy_consumer.py'
            
            if not script_path.exists():
                self.log_error(f"Cannot find strategy_consumer.py")
                return
            
            # Update config file
            self.update_config_from_ui()
            config_path = Path(__file__).parent / 'shared' / 'config.json'
            config_path.parent.mkdir(exist_ok=True)
            with open(config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
            
            # Start process
            self.consumer_process = QProcess(self)
            self.consumer_process.readyReadStandardOutput.connect(self.handle_process_output)
            self.consumer_process.readyReadStandardError.connect(self.handle_process_error)
            self.consumer_process.finished.connect(self.handle_process_finished)
            
            self.consumer_process.setProgram(sys.executable)
            self.consumer_process.setArguments([str(script_path)])
            self.consumer_process.start()
            
            self.start_process_btn.setText("Stop (Process)")
            self.status_label.setText("Status: Running (Process)")
            self.log(f"Consumer process started: {script_path}")
    
    def handle_process_output(self):
        """Handle output from consumer process."""
        if self.consumer_process:
            output = self.consumer_process.readAllStandardOutput().data().decode()
            for line in output.strip().split('\n'):
                if line:
                    self.log(f"[Process] {line}")
    
    def handle_process_error(self):
        """Handle error output from consumer process."""
        if self.consumer_process:
            error = self.consumer_process.readAllStandardError().data().decode()
            for line in error.strip().split('\n'):
                if line:
                    self.log_error(f"[Process] {line}")
    
    def handle_process_finished(self, exit_code, exit_status):
        """Handle process termination."""
        self.log(f"Consumer process finished with code {exit_code}")
        self.start_process_btn.setText("Start (Process)")
        self.status_label.setText("Status: Stopped")
    
    def update_config_from_ui(self):
        """Update configuration from UI inputs."""
        # Update RabbitMQ host
        if 'rabbitmq' not in self.config:
            self.config['rabbitmq'] = {}
        self.config['rabbitmq']['host'] = self.rabbitmq_host.currentText()
        
        # Update tickers
        tickers_text = self.tickers_edit.toPlainText()
        tickers = [t.strip().upper() for t in tickers_text.split(',') if t.strip()]
        self.config['tickers'] = tickers
    
    def handle_signal(self, signal: Dict[str, Any]):
        """Handle generated signal."""
        self.log(f"Signal: {signal.get('action', 'unknown').upper()} "
               f"{signal.get('ticker', 'unknown')} @ ${signal.get('price', 0):.2f}")
        
        # Forward to parent if available
        if self.parent() and hasattr(self.parent(), 'handle_strategy_signal'):
            self.parent().handle_strategy_signal(signal)
    
    def log(self, message: str):
        """Log a message."""
        timestamp = datetime.datetime.now().strftime('%H:%M:%S')
        self.log_output.append(f"[{timestamp}] {message}")
    
    def log_error(self, message: str):
        """Log an error message."""
        timestamp = datetime.datetime.now().strftime('%H:%M:%S')
        self.log_output.append(f"[{timestamp}] ERROR: {message}")
    
    def cleanup(self):
        """Clean up resources."""
        if self.consumer_thread and self.consumer_thread.isRunning():
            self.consumer_thread.stop()
            self.consumer_thread.wait()
        
        if self.consumer_process and self.consumer_process.state() == QProcess.Running:
            self.consumer_process.terminate()
            self.consumer_process.waitForFinished(5000)


# ==================== MAIN TEST APPLICATION ====================

class TestStrategyConsumerApp(QMainWindow):
    """Test application for the strategy consumer."""
    
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Strategy Consumer Test")
        self.setGeometry(100, 100, 800, 600)
        
        # Load or create config
        self.config = self.load_config()
        
        # Create widget
        self.consumer_widget = StrategyConsumerWidget(self.config, self)
        self.setCentralWidget(self.consumer_widget)
        
    def load_config(self) -> Dict[str, Any]:
        """Load configuration."""
        config_path = Path(__file__).parent / 'shared' / 'config.json'
        
        if config_path.exists():
            with open(config_path, 'r') as f:
                return json.load(f)
        
        # Default configuration
        return {
            'rabbitmq': {
                'host': 'localhost',
                'price_queue': 'price_queue',
                'signal_queue': 'signal_queue'
            },
            'tickers': ['AAPL', 'GOOGL', 'MSFT', 'TSLA'],
            'fetch_interval': 60,
            'strategy_thresholds': {
                'buy': 100,
                'sell': 200
            },
            'strategy_config': {
                'min_profit_pct': 5.0,
                'use_latching': True,
                'latching_threshold_pct': 10.0,
                'cooldown_period': 300
            },
            'reversal_config': {
                'retracement_threshold': 0.2,
                'prev_low_tolerance': 0.003,
                'min_volume_factor': 1.5,
                'min_body_factor': 0.001,
                'min_upward_move': 0.001,
                'cooldown_period': 300
            }
        }
    
    def handle_strategy_signal(self, signal: Dict[str, Any]):
        """Handle strategy signal."""
        print(f"Received signal: {signal}")
    
    def closeEvent(self, event):
        """Handle application close."""
        self.consumer_widget.cleanup()
        event.accept()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    
    # Set dark theme
    app.setStyleSheet("""
        QWidget {
            background-color: #2b2b2b;
            color: #ffffff;
        }
        QGroupBox {
            border: 1px solid #555;
            margin-top: 10px;
            padding-top: 10px;
        }
        QGroupBox::title {
            color: #00c8ff;
        }
        QPushButton {
            background-color: #404040;
            border: 1px solid #555;
            padding: 5px;
            border-radius: 3px;
        }
        QPushButton:hover {
            background-color: #505050;
        }
        QTextEdit, QLineEdit, QComboBox {
            background-color: #404040;
            border: 1px solid #555;
        }
    """)
    
    window = TestStrategyConsumerApp()
    window.show()
    
    sys.exit(app.exec_())