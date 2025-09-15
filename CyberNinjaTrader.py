#!/usr/bin/env python3
"""
CyberNinja Trader Pro - Enhanced Edition
A sophisticated trading system with improved resource management, caching, and reliability
"""

import sys
import traceback
import json
import asyncio
import aio_pika
import requests
import websocket
import threading
import signal
import atexit
import time
import ssl
import os
import psutil
import logging
from pathlib import Path
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from functools import wraps
from collections import deque
from enum import Enum
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from logging.handlers import RotatingFileHandler

import numpy as np
import yfinance as yf
import pandas as pd
import pandas_ta as ta

from PyQt5.QtWidgets import (
    QApplication, QWidget, QLabel, QVBoxLayout, QHBoxLayout, QComboBox,
    QProgressBar, QCheckBox, QTextEdit, QGroupBox, QPushButton, QLineEdit,
    QScrollArea, QMainWindow, QTabWidget, QTableWidget, QTableWidgetItem,
    QHeaderView, QStatusBar
)
from PyQt5.QtCore import (
    Qt, QThread, pyqtSignal, QCoreApplication, pyqtSlot, QDateTime, 
    QTimer, QObject, QRunnable, QThreadPool, QProcess, QMutex, QMutexLocker
)
from PyQt5.QtGui import QFont, QColor, QPalette

# Import your custom modules (adjust paths as needed)
from gui.reversal_radar import ReversalRadarTab
from backend.app.utils.rabbitmq_api import rabbitmq_api
from gui.strategy_engine_tab import EnhancedStrategyEngineTab

# Enable high-DPI scaling
QCoreApplication.setAttribute(Qt.AA_EnableHighDpiScaling)
QCoreApplication.setAttribute(Qt.AA_UseHighDpiPixmaps)

# ==================== CONSTANTS ====================
FETCH_INTERVAL = 60
LOG_COOLDOWN = 1000
LOG_HISTORY_LIMIT = 100
MAX_BUFFER_SIZE = 1000
CACHE_TIMEOUT = 30
BASE_RETRY_DELAY = 5.0
MAX_RETRY_DELAY = 60.0
MAX_RETRIES = 5
BATCH_INTERVAL = 2.0
WS_PING_INTERVAL = 20
MAX_THREADS = 10
MAX_PROCESSES = 3

# ==================== LOGGING SETUP ====================
def setup_logging():
    """Configure application-wide logging"""
    logger = logging.getLogger('CyberNinjaTrader')
    logger.setLevel(logging.INFO)
    
    crash_dir = Path.home() / ".cyberninja_trader"
    crash_dir.mkdir(parents=True, exist_ok=True)
    
    handler = RotatingFileHandler(
        crash_dir / "app.log", 
        maxBytes=10*1024*1024, 
        backupCount=5
    )
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

logger = setup_logging()

# ==================== EXCEPTION HANDLER ====================
def handle_exception(exc_type, exc_value, exc_traceback):
    """Global uncaught exception handler for GUI crashes"""
    import traceback
    from pathlib import Path
    from datetime import datetime
    
    try:
        # Let Ctrl+C behave normally
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        # Guard against re-entrancy
        if getattr(handle_exception, "_in_progress", False):
            print("🚨 Re-entrant exception handler call. Printing only.")
            traceback.print_exception(exc_type, exc_value, exc_traceback)
            return
        handle_exception._in_progress = True

        # Console output
        print("🚨 Uncaught GUI exception:")
        traceback.print_exception(exc_type, exc_value, exc_traceback)

        # Write to crash log
        try:
            crash_dir = Path.home() / ".cyberninja_trader"
            crash_dir.mkdir(parents=True, exist_ok=True)
            crash_file = crash_dir / "crash.log"
            
            with crash_file.open("a", encoding="utf-8") as f:
                f.write("\n" + "="*80 + "\n")
                f.write(f"[{datetime.now().isoformat(timespec='seconds')}] Uncaught exception\n")
                traceback.print_exception(exc_type, exc_value, exc_traceback, file=f)
            print(f"📝 Crash logged to: {crash_file}")
        except Exception as log_err:
            print(f"⚠️ Failed to write crash log: {log_err}")

        # Update UI if possible
        try:
            win = globals().get("window", None)
            if win is not None:
                from PyQt5.QtCore import QTimer
                def _ui_update():
                    try:
                        if hasattr(win, "StatusSignal"):
                            win.StatusSignal.emit(f"🔴 GUI Crash: {str(exc_value)[:50]}")
                        if hasattr(win, "LogSignal"):
                            win.LogSignal.emit(f"🚨💥 Uncaught exception: {exc_value}")
                    except Exception as ui_err:
                        print(f"🧯 GUI crash logging failed: {ui_err}")
                QTimer.singleShot(0, _ui_update)
        except Exception as e:
            print(f"🧯 GUI crash handling failed: {e}")
    finally:
        handle_exception._in_progress = False

# ==================== ENUMS ====================
class ConnectionState(Enum):
    """Connection states for WebSocket and RabbitMQ"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"

class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"      # Failures exceeded, rejecting requests
    HALF_OPEN = "half_open"  # Testing if service recovered

# ==================== DATA CLASSES ====================
@dataclass
class CacheEntry:
    """Cache entry with timestamp"""
    data: Any
    timestamp: float
    hits: int = 0

@dataclass
class PerformanceMetric:
    """Performance tracking metric"""
    name: str
    values: deque = field(default_factory=lambda: deque(maxlen=100))
    
    def add(self, value: float):
        self.values.append(value)
    
    @property
    def average(self) -> float:
        return sum(self.values) / len(self.values) if self.values else 0
    
    @property
    def latest(self) -> float:
        return self.values[-1] if self.values else 0

# ==================== UTILITY CLASSES ====================
class ResourceManager:
    """Manages threads and processes with limits"""
    def __init__(self, max_threads=MAX_THREADS, max_processes=MAX_PROCESSES):
        self.max_threads = max_threads
        self.max_processes = max_processes
        self.thread_pool = QThreadPool.globalInstance()
        self.thread_pool.setMaxThreadCount(max_threads)
        self.active_processes = []
        self.mutex = QMutex()
        
    def add_process(self, process: QProcess) -> bool:
        """Add a process if under limit"""
        with QMutexLocker(self.mutex):
            if len(self.active_processes) < self.max_processes:
                self.active_processes.append(process)
                process.finished.connect(lambda: self.remove_process(process))
                return True
            return False
    
    def remove_process(self, process: QProcess):
        """Remove a finished process"""
        with QMutexLocker(self.mutex):
            if process in self.active_processes:
                self.active_processes.remove(process)
    
    def cleanup(self):
        """Clean up all resources"""
        with QMutexLocker(self.mutex):
            for proc in self.active_processes:
                if proc.state() == QProcess.Running:
                    proc.kill()
                    proc.waitForFinished(5000)
            self.active_processes.clear()

class DataCache:
    """Enhanced caching system with TTL and hit tracking"""
    def __init__(self, default_ttl=CACHE_TIMEOUT):
        self.cache: Dict[str, CacheEntry] = {}
        self.default_ttl = default_ttl
        self.mutex = QMutex()
        
    def get(self, key: str, ttl: Optional[float] = None) -> Optional[Any]:
        """Get cached value if not expired"""
        with QMutexLocker(self.mutex):
            if key not in self.cache:
                return None
            
            entry = self.cache[key]
            ttl = ttl or self.default_ttl
            
            if time.time() - entry.timestamp > ttl:
                del self.cache[key]
                return None
            
            entry.hits += 1
            return entry.data
    
    def set(self, key: str, data: Any):
        """Store value in cache"""
        with QMutexLocker(self.mutex):
            self.cache[key] = CacheEntry(data, time.time())
    
    def invalidate(self, key: str = None):
        """Invalidate specific key or entire cache"""
        with QMutexLocker(self.mutex):
            if key:
                self.cache.pop(key, None)
            else:
                self.cache.clear()
    
    def cleanup_expired(self):
        """Remove expired entries"""
        with QMutexLocker(self.mutex):
            current_time = time.time()
            expired_keys = [
                k for k, v in self.cache.items() 
                if current_time - v.timestamp > self.default_ttl
            ]
            for key in expired_keys:
                del self.cache[key]

class CircuitBreaker:
    """Circuit breaker pattern for connection resilience"""
    def __init__(self, failure_threshold=5, recovery_timeout=60, half_open_attempts=3):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_attempts = half_open_attempts
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitBreakerState.CLOSED
        self.half_open_count = 0
        self.mutex = QMutex()
    
    def call(self, func, *args, **kwargs):
        """Execute function through circuit breaker"""
        with QMutexLocker(self.mutex):
            if self.state == CircuitBreakerState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_count = 0
                else:
                    raise Exception("Circuit breaker is OPEN")
            
            try:
                result = func(*args, **kwargs)
                self._on_success()
                return result
            except Exception as e:
                self._on_failure()
                raise e
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time passed to try reset"""
        return (self.last_failure_time and 
                time.time() - self.last_failure_time >= self.recovery_timeout)
    
    def _on_success(self):
        """Handle successful call"""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.half_open_count += 1
            if self.half_open_count >= self.half_open_attempts:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
        else:
            self.failure_count = 0
    
    def _on_failure(self):
        """Handle failed call"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN
        elif self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN

class PerformanceMonitor:
    """Monitor and track performance metrics"""
    def __init__(self):
        self.metrics: Dict[str, PerformanceMetric] = {
            'message_processing_time': PerformanceMetric('message_processing_time'),
            'api_response_time': PerformanceMetric('api_response_time'),
            'ui_update_latency': PerformanceMetric('ui_update_latency'),
            'memory_usage': PerformanceMetric('memory_usage'),
            'thread_count': PerformanceMetric('thread_count')
        }
        self.mutex = QMutex()
    
    def record(self, metric_name: str, value: float):
        """Record a metric value"""
        with QMutexLocker(self.mutex):
            if metric_name in self.metrics:
                self.metrics[metric_name].add(value)
    
    def get_summary(self) -> Dict[str, Dict[str, float]]:
        """Get summary of all metrics"""
        with QMutexLocker(self.mutex):
            return {
                name: {
                    'average': metric.average,
                    'latest': metric.latest,
                    'count': len(metric.values)
                }
                for name, metric in self.metrics.items()
            }
    
    def start_monitoring(self):
        """Start automatic monitoring"""
        def _monitor():
            process = psutil.Process()
            self.record('memory_usage', process.memory_info().rss / 1024 / 1024)
            self.record('thread_count', process.num_threads())
        
        self.monitor_timer = QTimer()
        self.monitor_timer.timeout.connect(_monitor)
        self.monitor_timer.start(5000)  # Every 5 seconds

# ==================== HELPER FUNCTIONS ====================
def safe_float(val: float, fallback: float = 0.0) -> float:
    """Safely convert to float with fallback"""
    import math
    try:
        if isinstance(val, pd.Series):
            val = val.iloc[0]
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return fallback
        return f
    except:
        return fallback

def safe_set(widget: QObject, method_name: str, *args, **kwargs) -> None:
    """Safely call widget method"""
    try:
        if widget and hasattr(widget, method_name):
            fn = getattr(widget, method_name)
            QTimer.singleShot(0, lambda: fn(*args, **kwargs))
    except Exception as e:
        logger.error(f"safe_set failed: {e}")

def safe_emit(signal, data) -> None:
    """Safely emit signal"""
    try:
        if signal:
            signal.emit(data)
    except Exception as e:
        logger.error(f"safe_emit failed: {e}")

def safe_ui(fn):
    """Decorator for safe UI operations"""
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        try:
            return fn(self, *args, **kwargs)
        except Exception as e:
            logger.error(f"Safe UI crash caught: {e}")
            traceback.print_exc()
            if hasattr(self, 'LogSignal'):
                self.LogSignal.emit(f"🚨 Safe UI crash: {e}")
            if hasattr(self, 'StatusSignal'):
                self.StatusSignal.emit(f"🔴 UI Crash: {str(e)[:50]}")
    return wrapper

# ==================== CUSTOM WIDGETS ====================
class AnimatedButton(QPushButton):
    """Enhanced button with animations"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setStyleSheet("""
            QPushButton {
                background-color: #001f3f;
                color: #00c8ff;
                border: 1px solid #00c8ff;
                padding: 5px;
                border-radius: 3px;
            }
            QPushButton:hover {
                background-color: #003366;
                border: 2px solid #00c8ff;
            }
            QPushButton:disabled {
                background-color: #444;
                color: #888;
                border: 1px solid #444;
            }
            QPushButton:checked {
                background-color: #003366;
                color: #00ffcc;
                border: 2px solid #00ffcc;
            }
        """)
        self.setCheckable(True)
        self.setChecked(True)

# ==================== WORKER CLASSES ====================
class AiWorkerSignals(QObject):
    done = pyqtSignal(dict)
    error = pyqtSignal(str)

class AiWorker(QRunnable):
    def __init__(self, ticker: str, circuit_breaker: CircuitBreaker = None):
        super().__init__()
        self.ticker = ticker
        self.signals = AiWorkerSignals()
        self.circuit_breaker = circuit_breaker or CircuitBreaker()
    
    @pyqtSlot()
    def run(self):
        try:
            def _api_call():
                return requests.post(
                    "http://localhost:8000/analyze", 
                    json={"ticker": self.ticker}, 
                    timeout=5
                )
            
            if self.circuit_breaker:
                r = self.circuit_breaker.call(_api_call)
            else:
                r = _api_call()
            
            if r.ok:
                self.signals.done.emit(r.json())
            else:
                self.signals.error.emit(r.text)
        except Exception as e:
            self.signals.error.emit(str(e))

class DataFetchThread(QThread):
    data_fetched = pyqtSignal(dict)
    error = pyqtSignal(str)
    
    def __init__(self, ticker, parent=None, indicators_only=False, cache=None):
        super().__init__(parent)
        self.ticker = ticker
        self.parent = parent
        self.is_shutting_down = False
        self.indicators_only = indicators_only
        self.cache = cache or DataCache()
        self.performance_monitor = None
        
    def run(self):
        start_time = time.time()
        try:
            if not self.ticker or not isinstance(self.ticker, str):
                logger.error(f"Invalid ticker: {self.ticker}")
                return
            
            self.ticker = self.ticker.strip().upper()
            cache_key = f"data_{self.ticker}_{self.indicators_only}"
            
            # Check cache first
            cached_data = self.cache.get(cache_key)
            if cached_data:
                logger.info(f"Using cached data for {self.ticker}")
                self.data_fetched.emit(cached_data)
                return
            
            logger.info(f"Fetching data for {self.ticker}")
            
            # Import the rate-limited fetcher
            try:
                # Import here to avoid circular imports
                import asyncio
                from backend.app.utils.fetch_prices import get_rate_limited_fetcher
                
                # Get rate-limited fetcher
                fetcher = get_rate_limited_fetcher()
                
                # Run async fetch in thread-safe way
                loop = None
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # If event loop is running, use run_in_executor
                        import concurrent.futures
                        def run_fetch():
                            new_loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(new_loop)
                            try:
                                return new_loop.run_until_complete(
                                    fetcher.fetch_price_with_fallback(self.ticker)
                                )
                            finally:
                                new_loop.close()
                        
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            future = executor.submit(run_fetch)
                            result = future.result(timeout=30)
                    else:
                        result = loop.run_until_complete(
                            fetcher.fetch_price_with_fallback(self.ticker)
                        )
                except RuntimeError:
                    # No event loop in current thread
                    result = asyncio.run(fetcher.fetch_price_with_fallback(self.ticker))
                
                if not result:
                    logger.error(f"No data returned for {self.ticker} from any source")
                    self.error.emit(f"🔴 No Data for {self.ticker} (All sources failed)")
                    return
                
                # Convert to DataFrame format for compatibility
                import pandas as pd
                hist = pd.DataFrame({
                    'Close': [result['price']],
                    'Volume': [result.get('volume', 0)],
                    'High': [result.get('high', result['price'])],
                    'Low': [result.get('low', result['price'])],
                    'Open': [result.get('open', result['price'])]
                })
                
            except Exception as e:
                logger.error(f"Failed to fetch data for {self.ticker}: {e}")
                self.error.emit(f"🔴 Fetch Error for {self.ticker}: {str(e)[:50]}")
                return
            
            # Calculate indicators
            current_price = hist['Close'].iloc[-1]
            current_volume = hist['Volume'].iloc[-1]
            avg_vol = hist['Volume'].mean()
            
            # RSI calculation
            rsi = 0
            if len(hist) >= 14:
                try:
                    rsi = ta.rsi(hist['Close'], length=14).iloc[-1]
                    if np.isnan(rsi) or np.isinf(rsi):
                        rsi = 0
                except Exception as e:
                    logger.error(f"RSI calculation failed: {e}")
            
            # MACD calculation
            macd = {'MACD_12_26_9': 0, 'MACDs_12_26_9': 0, 'MACDh_12_26_9': 0}
            if len(hist) >= 26:
                try:
                    macd_raw = ta.macd(hist['Close'])
                    if macd_raw is not None and not macd_raw.empty:
                        macd = macd_raw.iloc[-1].to_dict()
                except Exception as e:
                    logger.error(f"MACD calculation failed: {e}")
            
            update_data = {
                'ticker': self.ticker,
                'price': None if self.indicators_only else current_price,
                'rsi': rsi,
                'current_vol': current_volume,
                'avg_vol': avg_vol,
                'macd': macd,
                'source': 'indicators' if self.indicators_only else 'yahoo'
            }
            
            # Cache the data
            self.cache.set(cache_key, update_data)
            
            if not self.is_shutting_down:
                self.data_fetched.emit(update_data)
            
            # Record performance metric
            if self.performance_monitor:
                self.performance_monitor.record('api_response_time', time.time() - start_time)
                
        except Exception as e:
            logger.error(f"Error in DataFetchThread: {e}")
            traceback.print_exc()
            if not self.is_shutting_down:
                self.error.emit(f"🔴 Fetch Error: {str(e)[:50]}")

class EnhancedSignalConsumerThread(QThread):
    """Enhanced signal consumer with circuit breaker and better buffer management"""
    signal_received = pyqtSignal(dict)
    connection_status = pyqtSignal(bool)
    
    def __init__(self, config: dict, parent=None):
        super().__init__(parent)
        self.config = config
        self.running = True
        self.connection_state = ConnectionState.DISCONNECTED
        self.loop = None
        self.buffer = deque(maxlen=MAX_BUFFER_SIZE)
        self.circuit_breaker = CircuitBreaker()
        self.connection = None
        self.channel = None
        self.queue = None
        self.tasks = []
        self.consumer_tag = None
        self.is_shutting_down = False
        self.shutdown_lock = QMutex()
        self.performance_monitor = None
        
    async def consume_signals(self):
        while self.running:
            try:
                await self._connect_and_consume()
            except Exception as e:
                logger.error(f"Signal consumer error: {e}")
                self.connection_state = ConnectionState.ERROR
                self.connection_status.emit(False)
                await asyncio.sleep(BASE_RETRY_DELAY)
    
    async def _connect_and_consume(self):
        """Connect to RabbitMQ and consume messages"""
        self.connection_state = ConnectionState.CONNECTING
        
        def _connect():
            return aio_pika.connect_robust(
                host=self.config['rabbitmq']['host'],
                timeout=5
            )
        
        self.connection = await asyncio.wait_for(
            _connect(), 
            timeout=10
        )
        
        self.connection_state = ConnectionState.CONNECTED
        self.connection_status.emit(True)
        
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=10)
        
        self.queue = await self.channel.declare_queue(
            self.config['rabbitmq']['signal_queue'],
            durable=True,
            auto_delete=False,
            arguments={'x-queue-type': 'classic'}
        )
        
        logger.info(f"Connected to signal queue: {self.queue.name}")
        
        async with self.queue.iterator(no_ack=False) as queue_iter:
            self.consumer_tag = queue_iter.consumer_tag
            async for message in queue_iter:
                if not self.running:
                    break
                
                # Buffer management with automatic cleanup
                if len(self.buffer) >= MAX_BUFFER_SIZE - 10:
                    # Keep only recent half when near limit
                    temp_buffer = list(self.buffer)
                    self.buffer.clear()
                    self.buffer.extend(temp_buffer[-MAX_BUFFER_SIZE//2:])
                
                try:
                    start_time = time.time()
                    data = json.loads(message.body.decode())
                    self.buffer.append((message, data))
                    
                    if self.performance_monitor:
                        self.performance_monitor.record(
                            'message_processing_time', 
                            time.time() - start_time
                        )
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    await message.nack(requeue=True)
    
    async def emit_buffered_messages(self):
        """Emit buffered messages to UI"""
        while self.running:
            if self.buffer:
                batch = []
                try:
                    # Process up to 10 messages at once
                    for _ in range(min(10, len(self.buffer))):
                        if self.buffer:
                            batch.append(self.buffer.popleft())
                    
                    for msg, data in batch:
                        if not self.is_shutting_down:
                            start_time = time.time()
                            safe_emit(self.signal_received, data)
                            
                            if self.performance_monitor:
                                self.performance_monitor.record(
                                    'ui_update_latency',
                                    time.time() - start_time
                                )
                        
                        await msg.ack()
                        logger.info(f"Processed signal: {data.get('ticker', 'Unknown')}")
                        
                except Exception as e:
                    logger.error(f"Error emitting signals: {e}")
                    # Re-add failed messages to buffer
                    for msg, data in batch:
                        self.buffer.appendleft((msg, data))
            
            await asyncio.sleep(BATCH_INTERVAL)
    
    def run(self):
        """Main thread run method"""
        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            
            self.tasks = [
                self.loop.create_task(self.consume_signals()),
                self.loop.create_task(self.emit_buffered_messages())
            ]
            
            logger.info("SignalConsumerThread started")
            self.loop.run_forever()
        except Exception as e:
            logger.error(f"SignalConsumer loop error: {e}")
            traceback.print_exc()
        finally:
            self._cleanup_sync()
    
    def stop(self):
        """Stop the consumer thread"""
        with QMutexLocker(self.shutdown_lock):
            if self.is_shutting_down:
                return
            
            logger.info("Stopping SignalConsumerThread")
            self.is_shutting_down = True
            self.running = False
            
            if self.loop and self.loop.is_running():
                for task in self.tasks:
                    task.cancel()
                
                future = asyncio.run_coroutine_threadsafe(
                    self.cleanup(),
                    self.loop
                )
                try:
                    future.result(timeout=10.0)
                except Exception as e:
                    logger.error(f"Cleanup error: {e}")
                
                self.loop.call_soon_threadsafe(self.loop.stop)
    
    async def cleanup(self):
        """Async cleanup"""
        logger.info("Cleaning up SignalConsumerThread")
        try:
            if self.channel and not self.channel.is_closed:
                if self.consumer_tag and self.queue:
                    try:
                        await self.queue.cancel(self.consumer_tag)
                    except Exception as e:
                        logger.warning(f"Error cancelling consumer: {e}")
                await self.channel.close()
            
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
            
            self.connection_state = ConnectionState.DISCONNECTED
            self.connection_status.emit(False)
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    def _cleanup_sync(self):
        """Synchronous cleanup for thread termination"""
        if self.loop:
            try:
                self.loop.close()
            except Exception as e:
                logger.error(f"Error closing event loop: {e}")

# Similar enhancement for PriceConsumerThread
class EnhancedPriceConsumerThread(QThread):
    """Enhanced price consumer with WebSocket management"""
    price_received = pyqtSignal(dict)
    connection_status = pyqtSignal(bool)
    
    def __init__(self, config: dict, parent=None):
        super().__init__(parent)
        self.config = config
        self.running = True
        self.connection_state = ConnectionState.DISCONNECTED
        self.ws_state = ConnectionState.DISCONNECTED
        self.loop = None
        self.buffer = deque(maxlen=MAX_BUFFER_SIZE)
        self.circuit_breaker = CircuitBreaker()
        self.ws = None
        self.ws_thread = None
        self.ticker = None
        self.api_key = os.getenv('FINNHUB_API_KEY', config.get('finnhub_api_key', ''))
        self.shutdown_lock = QMutex()
        self.is_shutting_down = False
        self.performance_monitor = None
        
        # RabbitMQ connection variables (needed for price consumer)
        self.connection = None
        self.channel = None
        self.queue = None
        self.tasks = []
        self.consumer_tag = None
        
    def run(self):
        """Run the price consumer thread"""
        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            
            # Start consumer task
            self.tasks = [
                self.loop.create_task(self.consume_prices()),
                self.loop.create_task(self.emit_buffered_messages())
            ]
            
            logger.info("PriceConsumerThread started")
            self.loop.run_until_complete(asyncio.gather(*self.tasks, return_exceptions=True))
            
        except Exception as e:
            logger.error(f"PriceConsumerThread error: {e}")
        finally:
            logger.info("Stopping PriceConsumerThread")
            self._cleanup()
            if self.loop and not self.loop.is_closed():
                self.loop.close()
    
    async def consume_prices(self):
        """Consume price messages from RabbitMQ"""
        while self.running:
            try:
                await self._connect_and_consume()
            except Exception as e:
                logger.error(f"Price consumer error: {e}")
                self.connection_state = ConnectionState.ERROR
                self.connection_status.emit(False)
                await asyncio.sleep(BASE_RETRY_DELAY)
    
    async def _connect_and_consume(self):
        """Connect to RabbitMQ and consume price messages"""
        self.connection_state = ConnectionState.CONNECTING
        logger.info("PriceConsumerThread connecting to RabbitMQ...")
        
        def _connect():
            return aio_pika.connect_robust(
                host=self.config['rabbitmq']['host'],
                timeout=5
            )
        
        self.connection = await asyncio.wait_for(
            _connect(), 
            timeout=10
        )
        
        self.connection_state = ConnectionState.CONNECTED
        self.connection_status.emit(True)
        logger.info("PriceConsumerThread connected to RabbitMQ")
        
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=10)
        
        # Get queue name from config
        queue_name = self.config['rabbitmq']['price_queue']
        self.queue = await self.channel.declare_queue(
            queue_name,
            durable=True,
            auto_delete=False,
            arguments={'x-queue-type': 'classic'}
        )
        
        logger.info(f"PriceConsumerThread bound to price queue: {self.queue.name}")
        
        async with self.queue.iterator(no_ack=False) as queue_iter:
            self.consumer_tag = queue_iter.consumer_tag
            logger.info(f"PriceConsumerThread consumer registered with tag: {self.consumer_tag}")
            
            async for message in queue_iter:
                if not self.running:
                    break
                
                # Buffer management
                if len(self.buffer) >= MAX_BUFFER_SIZE - 10:
                    temp_buffer = list(self.buffer)
                    self.buffer.clear()
                    self.buffer.extend(temp_buffer[-MAX_BUFFER_SIZE//2:])
                
                try:
                    start_time = time.time()
                    data = json.loads(message.body.decode())
                    self.buffer.append((message, data))
                    
                    if self.performance_monitor:
                        self.performance_monitor.record(
                            'price_message_processing_time', 
                            time.time() - start_time
                        )
                        
                    logger.info(f"Price message received: {data}")
                    logger.debug(f"Price message received for {data.get('ticker', 'unknown')}: ${data.get('price', 0):.2f}")
                    
                except Exception as e:
                    logger.error(f"Error processing price message: {e}")
                    await message.nack(requeue=True)
    
    async def emit_buffered_messages(self):
        """Emit buffered price messages to UI"""
        while self.running:
            if self.buffer:
                batch = []
                try:
                    # Process up to 10 messages at once
                    for _ in range(min(10, len(self.buffer))):
                        if self.buffer:
                            batch.append(self.buffer.popleft())
                    
                    for msg, data in batch:
                        try:
                            # Emit to UI
                            self.price_received.emit(data)
                            await msg.ack()
                        except Exception as e:
                            logger.error(f"Error emitting price data: {e}")
                            await msg.nack(requeue=True)
                            
                except Exception as e:
                    logger.error(f"Error processing price buffer: {e}")
            
            await asyncio.sleep(0.1)  # Small delay to prevent CPU spinning
    
    def _cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up PriceConsumerThread")
        
        try:
            if self.loop and not self.loop.is_closed():
                if self.consumer_tag and self.channel:
                    # Cancel consumer
                    cleanup_task = self.loop.create_task(self._cancel_consumer())
                    self.loop.run_until_complete(cleanup_task)
                
                # Close connections
                if self.channel:
                    close_task = self.loop.create_task(self.channel.close())
                    self.loop.run_until_complete(close_task)
                
                if self.connection:
                    close_task = self.loop.create_task(self.connection.close())
                    self.loop.run_until_complete(close_task)
                    
        except Exception as e:
            logger.error(f"Error during PriceConsumerThread cleanup: {e}")
    
    async def _cancel_consumer(self):
        """Cancel the consumer safely"""
        try:
            if hasattr(self.queue, 'cancel') and self.consumer_tag:
                await self.queue.cancel(self.consumer_tag)
                logger.info("PriceConsumerThread consumer cancelled")
        except Exception as e:
            logger.error(f"Error cancelling price consumer: {e}")
    
    def stop(self):
        """Stop the consumer thread"""
        with QMutexLocker(self.shutdown_lock):
            if self.is_shutting_down:
                return
            self.is_shutting_down = True
            
        logger.info("Stopping PriceConsumerThread...")
        self.running = False
        
        # Cancel all tasks
        if self.loop and self.tasks:
            for task in self.tasks:
                if not task.done():
                    task.cancel()
        
        # Close WebSocket if running
        if self.ws_thread:
            self.ws_thread.join(timeout=2)
        
        self.quit()
        self.wait(5000)  # Wait up to 5 seconds for clean shutdown
    
    def subscribe_to_finnhub(self, ticker: str):
        """Subscribe to Finnhub WebSocket with proper state management"""
        with QMutexLocker(self.shutdown_lock):
            if self.ws_state == ConnectionState.CONNECTING:
                logger.info(f"Already connecting to Finnhub for {ticker}")
                return
            
            self.ws_state = ConnectionState.CONNECTING
            
            # Close existing connection if any
            if self.ws and self.ws.sock and self.ws.sock.connected:
                try:
                    self.ws.send(json.dumps({"type": "unsubscribe", "symbol": self.ticker}))
                    self.ws.close()
                except Exception:
                    pass
            
            self.ticker = ticker.strip().upper()
            if not self.ticker or not self.api_key:
                logger.error(f"Invalid ticker or API key")
                return
            
            # Create and start WebSocket connection
            self._start_websocket()

# ==================== MAIN APPLICATION ====================
class CyberNinjaTraderEnhanced(QMainWindow):
    """Enhanced main application with improved resource management"""
    UpdateUISignal = pyqtSignal(dict)
    ModeChangedSignal = pyqtSignal(bool)
    LogSignal = pyqtSignal(str)
    StatusSignal = pyqtSignal(str)
    
    def __init__(self):
        super().__init__()
        
        # Core components
        self.config = self.load_config()
        self.resource_manager = ResourceManager()
        self.cache = DataCache()
        self.performance_monitor = PerformanceMonitor()
        
        # Circuit breakers for external services
        self.api_circuit_breaker = CircuitBreaker()
        self.rabbitmq_circuit_breaker = CircuitBreaker()
        
        # Application state
        self.is_shutting_down = False
        self.shutdown_lock = QMutex()
        self.live_mode = False
        self.selected_ticker = None
        
        # Thread management
        self.signal_thread = None
        self.price_thread = None
        self.data_fetch_thread = None
        self.bottom_signal_process = None
        
        # UI state
        self.last_status = {}
        self.last_update_time = QDateTime.currentDateTime().addSecs(-5)
        self.last_log_time = QDateTime.currentDateTime()
        self.ticker_update_times = {}
        self.last_source_per_ticker = {}
        
        # Strategy filters
        self.strategy_filters = {
            'reversal': True,
            'breakout': True,
            'threshold': True,
            'ai_enhanced': True
        }
        
        # Initialize UI
        self.setup_ui()
        self.setup_styles()
        self.setup_timers()
        self.setup_threads()
        
        # Connect signals
        self.LogSignal.connect(self._append_log, Qt.QueuedConnection)
        self.StatusSignal.connect(self.status_label.setText, Qt.QueuedConnection)
        
        # Start performance monitoring
        self.performance_monitor.start_monitoring()
        
        # Register cleanup handlers
        self.register_shutdown_handlers()
        
        self.log("🟢 Enhanced Application initialized")
        self.setWindowTitle("🥷📈⚔️ CyberNinja Trader Pro - Enhanced Edition")
    
    def load_config(self, env='production') -> dict:
        """Load configuration with environment support and initialize watchlist manager"""
        # Initialize watchlist manager first
        from watchlist_utils import get_watchlist_manager
        self.watchlist_manager = get_watchlist_manager()
        
        config_dir = Path(__file__).resolve().parent / 'shared'
        base_config_path = config_dir / 'config.json'
        env_config_path = config_dir / f'config.{env}.json'
        
        try:
            # Load base configuration
            with open(base_config_path, 'r') as f:
                config = json.load(f)
            
            # Merge environment-specific config if exists
            if env_config_path.exists():
                with open(env_config_path, 'r') as f:
                    env_config = json.load(f)
                    config.update(env_config)
            
            # Load tickers from watchlist instead of config
            config['tickers'] = self.watchlist_manager.get_all_tickers(enabled_only=True)
            if not config['tickers']:  # Fallback if no tickers in watchlist
                config['tickers'] = ["AAPL", "NVDA", "AMZN", "META", "TSLA", "PLTR"]
                self.log("⚠️ No tickers in watchlist, using defaults")
            
            return config
        except FileNotFoundError:
            self.log("🚨 Config file not found, using defaults")
            # Still try to load from watchlist even if config file missing
            tickers = ["AAPL", "NVDA", "AMZN", "META", "TSLA", "PLTR"]
            try:
                watchlist_tickers = self.watchlist_manager.get_all_tickers(enabled_only=True)
                if watchlist_tickers:
                    tickers = watchlist_tickers
            except:
                pass
            
            return {
                "rabbitmq": {
                    "host": "localhost",
                    "price_queue": "price_queue",
                    "signal_queue": "signal_queue"
                },
                "tickers": tickers,
                "fetch_interval": FETCH_INTERVAL
            }
    
    def setup_threads(self):
        """Setup enhanced thread infrastructure"""
        try:
            # Data fetch thread with caching
            self.data_fetch_thread = DataFetchThread("", self, cache=self.cache)
            self.data_fetch_thread.performance_monitor = self.performance_monitor
            self.data_fetch_thread.data_fetched.connect(self.on_data_fetched, Qt.QueuedConnection)
            self.data_fetch_thread.error.connect(self.StatusSignal.emit, Qt.QueuedConnection)
            
            # Enhanced signal consumer
            self.signal_thread = EnhancedSignalConsumerThread(self.config, self)
            self.signal_thread.performance_monitor = self.performance_monitor
            self.signal_thread.signal_received.connect(self.handle_signal, Qt.QueuedConnection)
            self.signal_thread.connection_status.connect(self.update_signal_connection_status)
            
            # Connect to strategy engine if available
            if hasattr(self, 'strategy_engine_tab'):
                self.signal_thread.signal_received.connect(
                    self.strategy_engine_tab.on_signal, 
                    Qt.QueuedConnection
                )
            
            # Enhanced price consumer
            self.price_thread = EnhancedPriceConsumerThread(self.config, self)
            self.price_thread.performance_monitor = self.performance_monitor
            self.price_thread.price_received.connect(self.update_ui_with_data, Qt.QueuedConnection)
            self.price_thread.connection_status.connect(self.update_price_connection_status)
            
            # Connect price updates to Reversal tab for live price tracking
            if hasattr(self, 'reversal_radar_tab'):
                self.price_thread.price_received.connect(
                    self.reversal_radar_tab.update_table, 
                    Qt.QueuedConnection
                )
                self.log("🔗 Connected price updates to Reversal Radar tab")
            
            self.start_threads()
            
        except Exception as e:
            self.log(f"🚨 Thread setup error: {e}")
            logger.error(f"Thread setup error: {e}")
    
    def start_threads(self):
        """Start all threads with resource management"""
        try:
            self.signal_thread.start()
            self.log("🟢 SignalConsumerThread started")
            
            self.price_thread.start()
            self.log("🟢 PriceConsumerThread started")
            
            self.start_bottom_signal_process()
            
        except Exception as e:
            self.log(f"🚨 Thread start error: {e}")
            logger.error(f"Thread start error: {e}")
    
    def start_bottom_signal_process(self):
        """Start bottom signal detector with resource management"""
        try:
            process = QProcess(self)
            
            if not self.resource_manager.add_process(process):
                self.log("⚠️ Process limit reached, cannot start Bottom Signal Detector")
                return
            
            script_path = Path(__file__).resolve().parent.parent / 'multi_ticker_runner.py'
            if not script_path.exists():
                self.log(f"🚨 Script not found: {script_path}")
                return
            
            process.setProgram(sys.executable)
            process.setArguments([str(script_path)])
            process.setProcessChannelMode(QProcess.MergedChannels)
            process.readyReadStandardOutput.connect(self.handle_bottom_output)
            process.start()
            
            self.bottom_signal_process = process
            self.log("📡 Started Bottom Signal Detector")
            
        except Exception as e:
            self.log(f"🚨 Failed to start Bottom Signal Detector: {e}")
    
    def handle_bottom_output(self):
        """Process bottom signal detector output"""
        try:
            output = str(self.bottom_signal_process.readAllStandardOutput(), 'utf-8')
            for line in output.strip().split('\n'):
                if not line.strip():
                    continue
                try:
                    data = json.loads(line.strip())
                    self.reversal_radar_tab.update_signal.emit(data)
                    self.log(f"📡 Bottom signal: {data.get('ticker', 'Unknown')}")
                except Exception as e:
                    logger.error(f"Failed to parse bottom signal: {e}")
        except Exception as e:
            self.log(f"🚨 Error reading bottom process: {e}")
    
    def setup_ui(self):
        """Setup enhanced UI components"""
        # [UI setup code similar to original but with enhancements]
        # Creating main layout
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        
        scroll = QScrollArea()
        scroll.setWidget(self.central_widget)
        scroll.setWidgetResizable(True)
        self.setCentralWidget(scroll)
        
        main_layout = QVBoxLayout(self.central_widget)
        main_layout.setSpacing(10)
        
        # Create tabs
        self.tabs = QTabWidget()
        self.main_tab = QWidget()
        self.reversal_radar_tab = ReversalRadarTab(self)
        
        # Connection to price thread will be made during thread setup
        
        self.rabbitmq_monitor_tab = RabbitMQMonitorTab(self)
        self.strategy_engine_tab = EnhancedStrategyEngineTab(self)
        
        # Add performance monitor tab
        self.performance_tab = self.create_performance_tab()
        
        # Setup main tab
        self.setup_main_tab()
        
        # Add tabs
        self.tabs.addTab(self.main_tab, "🏠 Main")
        self.tabs.addTab(self.reversal_radar_tab, "📊 Reversal Radar")
        self.tabs.addTab(self.rabbitmq_monitor_tab, "📊 RabbitMQ Monitor")
        self.tabs.addTab(self.strategy_engine_tab, "🧠 Strategy Engine")
        self.tabs.addTab(self.performance_tab, "📈 Performance")
        
        main_layout.addWidget(self.tabs)
        
        # Setup status bar with enhanced controls
        self.setup_status_bar()
    
    def create_performance_tab(self) -> QWidget:
        """Create performance monitoring tab"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Performance metrics display
        self.perf_text = QTextEdit()
        self.perf_text.setReadOnly(True)
        layout.addWidget(self.perf_text)
        
        # Update timer
        self.perf_timer = QTimer()
        self.perf_timer.timeout.connect(self.update_performance_display)
        self.perf_timer.start(5000)
        
        return tab
    
    def update_performance_display(self):
        """Update performance metrics display"""
        try:
            metrics = self.performance_monitor.get_summary()
            
            text = "📊 Performance Metrics\n" + "="*50 + "\n\n"
            
            for name, data in metrics.items():
                text += f"{name}:\n"
                text += f"  Latest: {data['latest']:.2f}\n"
                text += f"  Average: {data['average']:.2f}\n"
                text += f"  Samples: {data['count']}\n\n"
            
            # Add cache stats
            text += "\n📦 Cache Statistics\n" + "="*50 + "\n"
            text += f"Entries: {len(self.cache.cache)}\n"
            
            total_hits = sum(entry.hits for entry in self.cache.cache.values())
            text += f"Total Hits: {total_hits}\n"
            
            # Add resource usage
            process = psutil.Process()
            text += "\n💻 Resource Usage\n" + "="*50 + "\n"
            text += f"Memory: {process.memory_info().rss / 1024 / 1024:.2f} MB\n"
            text += f"Threads: {process.num_threads()}\n"
            text += f"CPU: {process.cpu_percent():.1f}%\n"
            
            self.perf_text.setText(text)
            
        except Exception as e:
            logger.error(f"Error updating performance display: {e}")
    
    def setup_main_tab(self):
        """Setup main tab UI components"""
        layout = QVBoxLayout(self.main_tab)
        
        # Title
        self.title = QLabel("🥷 CyberNinja Trading Pro - Enhanced")
        self.title.setFont(QFont("Consolas", 14, QFont.Bold))
        layout.addWidget(self.title)
        
        # Status
        self.status_label = QLabel("🧲 Initializing...")
        layout.addWidget(self.status_label)
        
        # Price table
        self.table = QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels(["Ticker", "Price", "Volume", "Source", "Updated"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.setSortingEnabled(True)
        layout.addWidget(self.table)
        
        # Strategy controls
        self.setup_strategy_controls(layout)
        
        # Ticker controls
        self.setup_ticker_controls(layout)
        
        # Indicators
        self.setup_indicators(layout)
        
        # Log output
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setMaximumHeight(200)
        layout.addWidget(self.log_output)
        
        # Logging control
        self.disable_logging_check = QCheckBox("Disable Logging")
        self.disable_logging_check.stateChanged.connect(self.toggle_logging)
        layout.addWidget(self.disable_logging_check)
    
    def setup_strategy_controls(self, layout):
        """Setup strategy filter controls"""
        strategy_group = QGroupBox("Strategy Filters")
        strategy_layout = QHBoxLayout()
        
        self.strategy_buttons = {}
        for strategy in self.strategy_filters.keys():
            btn = AnimatedButton(f"🎯 {strategy.title()}")
            btn.setChecked(True)
            btn.clicked.connect(lambda checked, s=strategy: self.toggle_strategy(s, checked))
            self.strategy_buttons[strategy] = btn
            strategy_layout.addWidget(btn)
        
        strategy_group.setLayout(strategy_layout)
        layout.addWidget(strategy_group)
    
    def setup_ticker_controls(self, layout):
        """Setup ticker selection controls"""
        ticker_group = QGroupBox("Ticker Selection")
        ticker_layout = QVBoxLayout()
        
        # Ticker dropdown row
        row1 = QHBoxLayout()
        self.ticker_dropdown = QComboBox()
        # Load tickers from watchlist manager
        tickers = self.config.get("tickers", [])
        self.ticker_dropdown.addItems(tickers)
        
        # Add watchlist controls
        watchlist_row = QHBoxLayout()
        self.priority_filter = QComboBox()
        self.priority_filter.addItems(["All Tickers", "High Priority", "Medium Priority", "Low Priority"])
        self.priority_filter.currentTextChanged.connect(self.filter_tickers_by_priority)
        watchlist_row.addWidget(QLabel("Filter:"))
        watchlist_row.addWidget(self.priority_filter)
        ticker_layout.addLayout(watchlist_row)
        self.ticker_dropdown.setEditable(True)
        self.ticker_dropdown.currentTextChanged.connect(self.on_ticker_selected)
        
        self.auto_refresh_check = QCheckBox("Auto Refresh")
        self.auto_refresh_check.setChecked(True)
        
        self.refresh_btn = AnimatedButton("🔄 Refresh")
        self.refresh_btn.clicked.connect(self.refresh_selected_ticker)
        
        self.live_mode_check = QCheckBox("📡 Live Mode")
        self.live_mode_check.stateChanged.connect(self.toggle_live_mode)
        
        row1.addWidget(QLabel("Ticker:"))
        row1.addWidget(self.ticker_dropdown)
        row1.addWidget(self.auto_refresh_check)
        row1.addWidget(self.refresh_btn)
        row1.addWidget(self.live_mode_check)
        ticker_layout.addLayout(row1)
        
        # Add ticker row
        row2 = QHBoxLayout()
        self.ticker_input = QLineEdit()
        self.add_ticker_btn = AnimatedButton("➕ Add")
        self.add_ticker_btn.clicked.connect(self.add_ticker)
        
        row2.addWidget(QLabel("New:"))
        row2.addWidget(self.ticker_input)
        row2.addWidget(self.add_ticker_btn)
        ticker_layout.addLayout(row2)
        
        ticker_group.setLayout(ticker_layout)
        layout.addWidget(ticker_group)
    
    def setup_indicators(self, layout):
        """Setup market indicators display"""
        indicators_group = QGroupBox("Market Indicators")
        indicators_layout = QVBoxLayout()
        
        self.price_label = QLabel("Price: --")
        self.price_label.setFont(QFont("Consolas", 16, QFont.Bold))
        
        self.rsi_label = QLabel("RSI: --")
        self.rsi_progress = QProgressBar()
        self.rsi_progress.setRange(0, 100)
        
        self.volume_label = QLabel("Volume: --")
        self.volume_progress = QProgressBar()
        self.volume_progress.setRange(0, 200)
        
        self.macd_label = QLabel("MACD: --")
        
        indicators_layout.addWidget(self.price_label)
        indicators_layout.addWidget(self.rsi_label)
        indicators_layout.addWidget(self.rsi_progress)
        indicators_layout.addWidget(self.volume_label)
        indicators_layout.addWidget(self.volume_progress)
        indicators_layout.addWidget(self.macd_label)
        
        indicators_group.setLayout(indicators_layout)
        layout.addWidget(indicators_group)
    
    def setup_status_bar(self):
        """Setup enhanced status bar"""
        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)
        
        # Message count
        self.message_count_label = QLabel("📊 Messages: --")
        self.statusBar.addWidget(self.message_count_label)
        
        # Connection status
        self.connection_label = QLabel("🔌 Disconnected")
        self.statusBar.addWidget(self.connection_label)
        
        # Cache status
        self.cache_label = QLabel("📦 Cache: 0")
        self.statusBar.addWidget(self.cache_label)
        
        # Performance
        self.perf_label = QLabel("⚡ Performance: --")
        self.statusBar.addWidget(self.perf_label)
        
        # Control buttons
        buttons = [
            ("🧪 Test", self.send_test_signal),
            ("♻️ Purge", self.purge_signal_queue),
            ("🤖 AI", self.trigger_ai_analysis),
            ("📤 Export", self.export_data),
            ("🔧 Settings", self.open_settings)
        ]
        
        for text, handler in buttons:
            btn = AnimatedButton(text)
            btn.clicked.connect(handler)
            self.statusBar.addPermanentWidget(btn)
    
    def setup_timers(self):
        """Setup application timers"""
        # Auto refresh timer
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_data)
        self.auto_refresh_check.stateChanged.connect(self.toggle_auto_refresh)
        if self.auto_refresh_check.isChecked():
            self.timer.start(60000)
        
        # Watchdog timer
        self.watchdog_timer = QTimer()
        self.watchdog_timer.timeout.connect(self.watchdog_heartbeat)
        self.watchdog_timer.start(30000)
        
        # Cache cleanup timer
        self.cache_timer = QTimer()
        self.cache_timer.timeout.connect(self.cache.cleanup_expired)
        self.cache_timer.start(60000)
        
        # Status update timer
        self.status_timer = QTimer()
        self.status_timer.timeout.connect(self.update_status_bar)
        self.status_timer.start(5000)
    
    def setup_styles(self):
        """Apply application styles"""
        self.setStyleSheet("""
            QWidget {
                background-color: #000814;
                color: #00c8ff;
                font-family: Consolas;
            }
            QLabel {
                color: #00c8ff;
            }
            QComboBox, QLineEdit {
                background-color: #001f3f;
                color: #00c8ff;
                border: 1px solid #00c8ff;
            }
            QTextEdit {
                background-color: #001f3f;
                color: #00c8ff !important;
                border: 1px solid #00c8ff;
            }
            QPlainTextEdit {
                background-color: #001f3f;
                color: #00c8ff !important;
                border: 1px solid #00c8ff;
            }
            QCheckBox {
                color: #00c8ff;
            }
            QGroupBox {
                color: #00c8ff;
                border: 1px solid #00c8ff;
                margin-top: 10px;
                padding-top: 10px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 3px 0 3px;
            }
            QProgressBar {
                border: 1px solid #00c8ff;
                border-radius: 5px;
                background-color: #001f3f;
                height: 10px;
            }
            QProgressBar::chunk {
                background-color: #00c8ff;
            }
            QTabWidget::pane {
                border: 1px solid #00c8ff;
            }
            QTabBar::tab {
                background: #001f3f;
                color: #00c8ff;
                padding: 5px;
                margin-right: 2px;
            }
            QTabBar::tab:selected {
                background: #003366;
            }
            QStatusBar {
                background-color: #001f3f;
                color: #00c8ff;
                border-top: 1px solid #00c8ff;
            }
            QTableWidget {
                background-color: #001f3f;
                color: #00c8ff;
                border: 1px solid #00c8ff;
            }
            /* QTableWidget::item rule commented out to allow per-cell colors
            QTableWidget::item {
                color: #00c8ff;
            }
            */
            QHeaderView::section {
                background-color: #001f3f;
                color: #00c8ff;
                border: 1px solid #00c8ff;
            }
        """)
    
    # === Event Handlers ===
    
    def closeEvent(self, event):
        """Handle application closure with proper cleanup"""
        with QMutexLocker(self.shutdown_lock):
            if self.is_shutting_down:
                event.accept()
                return
            
            self.is_shutting_down = True
            self.log("🧻 Initiating graceful shutdown...")
            
            try:
                # Stop timers
                for timer in [self.timer, self.watchdog_timer, self.cache_timer, 
                             self.status_timer, self.perf_timer]:
                    if timer and timer.isActive():
                        timer.stop()
                
                # Stop threads
                for thread in [self.signal_thread, self.price_thread, self.data_fetch_thread]:
                    if thread and thread.isRunning():
                        thread.stop() if hasattr(thread, 'stop') else thread.quit()
                        thread.wait(5000)
                
                # Clean up resources
                self.resource_manager.cleanup()
                
                # Save cache and settings
                self.save_state()
                
                self.log("🟢 Shutdown complete")
                event.accept()
                
            except Exception as e:
                self.log(f"🚨 Shutdown error: {e}")
                logger.error(f"Shutdown error: {e}")
                event.ignore()
    
    def watchdog_heartbeat(self):
        """Monitor application health"""
        try:
            process = psutil.Process()
            mem_mb = process.memory_info().rss / 1024 / 1024
            
            # Record metrics
            self.performance_monitor.record('memory_usage', mem_mb)
            
            # Check for memory issues
            if mem_mb > 1000:  # 1GB threshold
                logger.warning(f"High memory usage: {mem_mb:.2f} MB")
                self.cache.cleanup_expired()
            
            logger.info(f"Watchdog: Memory={mem_mb:.2f}MB, Threads={process.num_threads()}")
            
        except Exception as e:
            logger.error(f"Watchdog error: {e}")
    
    def update_status_bar(self):
        """Update status bar information"""
        try:
            # Update cache status
            self.cache_label.setText(f"📦 Cache: {len(self.cache.cache)}")
            
            # Update performance
            metrics = self.performance_monitor.get_summary()
            avg_latency = metrics.get('ui_update_latency', {}).get('average', 0)
            self.perf_label.setText(f"⚡ Latency: {avg_latency:.1f}ms")
            
        except Exception as e:
            logger.error(f"Status bar update error: {e}")
    
    @pyqtSlot(dict)
    @safe_ui
    def update_ui_with_data(self, data: dict):
        """Update UI with market data"""
        start_time = time.time()
        
        try:
            ticker = data.get("ticker", "UNKNOWN")
            
            # Store price history for RSI calculation
            if not hasattr(self, '_price_history'):
                self._price_history = {}
            if ticker not in self._price_history:
                self._price_history[ticker] = deque(maxlen=50)  # Keep last 50 prices for RSI
            
            if 'price' in data and data['price'] is not None:
                self._price_history[ticker].append(float(data['price']))
            
            # Update table
            row = self._find_or_create_table_row(ticker)
            
            if 'price' in data and data['price'] is not None:
                current_price = data['price']
                price_item = QTableWidgetItem(f"${current_price:.2f}")
                
                # Get previous price to determine color
                if not hasattr(self, '_prev_prices'):
                    self._prev_prices = {}
                
                previous_price = self._prev_prices.get(ticker, current_price)
                
                # Color based on price movement
                if current_price > previous_price:
                    price_item.setForeground(QColor(0, 255, 0))  # Green for up
                elif current_price < previous_price:
                    price_item.setForeground(QColor(255, 0, 0))  # Red for down
                else:
                    price_item.setForeground(QColor(0, 255, 0))  # Green for no change/default
                
                self.table.setItem(row, 1, price_item)
                
                # Store current price as previous for next update
                self._prev_prices[ticker] = current_price
            
            if 'volume' in data and data['volume'] is not None:
                volume = float(data['volume'])
                if volume >= 1e6:
                    volume_display = f"{volume/1e6:.1f}M"
                elif volume >= 1e3:
                    volume_display = f"{volume/1e3:.1f}K"
                else:
                    volume_display = f"{volume:.0f}"
                self.table.setItem(row, 2, QTableWidgetItem(volume_display))
            
            if 'source' in data:
                self.table.setItem(row, 3, QTableWidgetItem(data['source']))
            
            # Update timestamp
            self.table.setItem(row, 4, QTableWidgetItem(
                datetime.now().strftime("%H:%M:%S")
            ))
            
            # Calculate RSI from price history and add to data
            if len(self._price_history[ticker]) >= 14:
                try:
                    import pandas as pd
                    import numpy as np
                    
                    prices = list(self._price_history[ticker])
                    price_series = pd.Series(prices)
                    rsi = ta.rsi(price_series, length=14).iloc[-1]
                    
                    if not (np.isnan(rsi) or np.isinf(rsi)):
                        data['rsi'] = rsi
                        # Update volume data for indicators if available
                        if 'volume' in data:
                            data['current_vol'] = data['volume']
                            data['avg_vol'] = data['volume']  # Simple fallback
                            
                except Exception as e:
                    logger.debug(f"RSI calculation error for {ticker}: {e}")
            
            # Update indicators if selected ticker
            if ticker == self.selected_ticker:
                self._update_indicators(data)
            
            # Record performance
            self.performance_monitor.record(
                'ui_update_latency',
                (time.time() - start_time) * 1000
            )
            
        except Exception as e:
            self.log(f"🚨 UI update error: {e}")
            logger.error(f"UI update error: {e}")
    
    def _find_or_create_table_row(self, ticker: str) -> int:
        """Find or create table row for ticker"""
        for i in range(self.table.rowCount()):
            if self.table.item(i, 0) and self.table.item(i, 0).text() == ticker:
                return i
        
        # Create new row
        row = self.table.rowCount()
        self.table.insertRow(row)
        self.table.setItem(row, 0, QTableWidgetItem(ticker))
        return row
    
    def _update_indicators(self, data: dict):
        """Update indicator displays"""
        if 'price' in data and data['price'] is not None:
            current_price = data['price']
            ticker = data.get("ticker", "UNKNOWN")
            
            # Get previous price to determine color
            if not hasattr(self, '_prev_prices'):
                self._prev_prices = {}
            
            previous_price = self._prev_prices.get(ticker, current_price)
            
            # Set price text
            self.price_label.setText(f"Price: ${current_price:.2f}")
            
            # Color based on price movement
            if current_price > previous_price:
                self.price_label.setStyleSheet("QLabel { color: #00FF00; }")  # Green for up
            elif current_price < previous_price:
                self.price_label.setStyleSheet("QLabel { color: #FF0000; }")  # Red for down
            else:
                self.price_label.setStyleSheet("QLabel { color: #00FF00; }")  # Green for default
        
        if 'rsi' in data:
            rsi = safe_float(data['rsi'])
            self.rsi_label.setText(f"RSI: {rsi:.2f}")
            self.rsi_progress.setValue(int(rsi))
        
        if 'current_vol' in data and 'avg_vol' in data:
            current = safe_float(data['current_vol'])
            avg = safe_float(data['avg_vol'], 1)
            ratio = (current / avg) * 100 if avg > 0 else 0
            self.volume_label.setText(f"Volume: {current/1e6:.1f}M")
            self.volume_progress.setValue(int(min(ratio, 200)))
        
        if 'macd' in data and isinstance(data['macd'], dict):
            macd = data['macd']
            self.macd_label.setText(
                f"MACD: {macd.get('MACD_12_26_9', 0):.2f}"
            )
    
    # === Actions ===
    
    def toggle_strategy(self, strategy: str, checked: bool):
        """Toggle strategy filter"""
        self.strategy_filters[strategy] = checked
        self.log(f"{'🟢' if checked else '🔴'} {strategy.title()} strategy {'enabled' if checked else 'disabled'}")
    
    def toggle_live_mode(self, state: int):
        """Toggle live/offline mode"""
        self.live_mode = bool(state)
        self.log(f"{'🟢' if self.live_mode else '🔴'} {'Live' if self.live_mode else 'Offline'} mode")
        
        if self.live_mode and self.selected_ticker:
            self.price_thread.subscribe_to_finnhub(self.selected_ticker)
    
    def on_ticker_selected(self, ticker: str):
        """Handle ticker selection"""
        if ticker:
            self.selected_ticker = ticker.strip().upper()
            self.title.setText(f"📊 {self.selected_ticker} Overview")
            
            # Subscribe to live feed if enabled
            if self.live_mode:
                self.price_thread.subscribe_to_finnhub(self.selected_ticker)
            
            # Update from cache or fetch new
            self.refresh_selected_ticker()
    
    def refresh_selected_ticker(self):
        """Refresh selected ticker data"""
        if not self.selected_ticker:
            return
        
        # Check cache first
        cache_key = f"data_{self.selected_ticker}_False"
        cached = self.cache.get(cache_key)
        
        if cached:
            self.update_ui_with_data(cached)
        else:
            # Fetch new data
            self.update_data()
    
    def update_data(self):
        """Fetch and update data"""
        if self.data_fetch_thread and self.data_fetch_thread.isRunning():
            return
        
        ticker = self.selected_ticker or self.ticker_dropdown.currentText()
        if not ticker:
            return
        
        self.data_fetch_thread = DataFetchThread(
            ticker, 
            self,
            indicators_only=self.live_mode,
            cache=self.cache
        )
        self.data_fetch_thread.performance_monitor = self.performance_monitor
        self.data_fetch_thread.data_fetched.connect(self.on_data_fetched)
        self.data_fetch_thread.error.connect(self.StatusSignal.emit)
        self.data_fetch_thread.start()
    
    @pyqtSlot(dict)
    def on_data_fetched(self, data: dict):
        """Handle fetched data"""
        self.update_ui_with_data(data)
        
        if hasattr(self, 'reversal_radar_tab'):
            self.reversal_radar_tab.update_data_row(data)
    
    @pyqtSlot(dict)
    def handle_signal(self, data: dict):
        """Handle trading signal"""
        try:
            signal_type = data.get('type', 'unknown')
            
            # Apply strategy filters
            if signal_type in self.strategy_filters:
                if not self.strategy_filters[signal_type]:
                    self.log(f"📡 Filtered signal: {signal_type} (disabled)")
                    return
            
            # Process signal
            self.UpdateUISignal.emit(data)
            
            # Update radar if available
            if hasattr(self, 'reversal_radar_tab'):
                self.reversal_radar_tab.update_signal.emit(data)
            
            # Update strategy engine if available
            if hasattr(self, 'strategy_engine_tab'):
                self.strategy_engine_tab.append_signal(data)
            
            self.log(f"📡 Signal: {signal_type} for {data.get('ticker', 'Unknown')}")
            
        except Exception as e:
            self.log(f"🚨 Signal handling error: {e}")
            logger.error(f"Signal handling error: {e}")
    
    def update_signal_connection_status(self, is_alive: bool):
        """Update signal connection status"""
        self.signal_connection_alive = is_alive
        self.update_connection_display()
    
    def update_price_connection_status(self, is_alive: bool):
        """Update price connection status"""
        self.price_connection_alive = is_alive
        self.update_connection_display()
    
    def update_connection_display(self):
        """Update connection status display"""
        status = "🔌 "
        if getattr(self, 'signal_connection_alive', False) and getattr(self, 'price_connection_alive', False):
            status += "Connected"
        elif getattr(self, 'signal_connection_alive', False) or getattr(self, 'price_connection_alive', False):
            status += "Partial"
        else:
            status += "Disconnected"
        
        self.connection_label.setText(status)
    
    def toggle_auto_refresh(self, state: int):
        """Toggle auto refresh"""
        if state:
            self.timer.start(60000)
            self.log("🟢 Auto-refresh enabled")
        else:
            self.timer.stop()
            self.log("🔴 Auto-refresh disabled")
    
    def toggle_logging(self, state: int):
        """Toggle logging output"""
        if state:
            self.log("🔴 Logging disabled")
        else:
            self.log("🟢 Logging enabled")
    
    def add_ticker(self):
        """Add new ticker to watchlist"""
        ticker = self.ticker_input.text().strip().upper()
        
        if not ticker:
            return
        
        existing = [self.ticker_dropdown.itemText(i) 
                   for i in range(self.ticker_dropdown.count())]
        
        if ticker not in existing:
            self.ticker_dropdown.addItem(ticker)
            self.config["tickers"].append(ticker)
            self.save_config()
            
            # Add to table
            row = self._find_or_create_table_row(ticker)
            
            self.ticker_input.clear()
            self.log(f"➕ Added ticker: {ticker}")
    
    def send_test_signal(self):
        """Send test signal to queue"""
        try:
            result = rabbitmq_api.send_test_signal()
            if "error" not in result:
                self.log("🧪 Test signal sent")
            else:
                self.log(f"🚨 Test signal failed: {result['error']}")
        except Exception as e:
            self.log(f"🚨 Test signal error: {e}")
    
    def purge_signal_queue(self):
        """Purge signal queue"""
        try:
            info = rabbitmq_api.get_queue_info_sync("signal_queue")
            if info and info.consumers > 0:
                self.log("⚠️ Cannot purge: Active consumers")
                return
            
            if rabbitmq_api.purge_queue("signal_queue"):
                self.log("♻️ Queue purged")
                self.update_queue_info()
        except Exception as e:
            self.log(f"🚨 Purge error: {e}")
    
    def trigger_ai_analysis(self):
        """Trigger AI analysis for current ticker"""
        if not self.selected_ticker:
            self.log("⚠️ No ticker selected")
            return
        
        self.log(f"🤖 Analyzing {self.selected_ticker}...")
        
        worker = AiWorker(self.selected_ticker, self.api_circuit_breaker)
        worker.signals.done.connect(lambda d: self.log(f"🤖 Analysis: {d}"))
        worker.signals.error.connect(lambda e: self.log(f"🚨 AI error: {e}"))
        
        self.resource_manager.thread_pool.start(worker)
    
    def export_data(self):
        """Export data to file"""
        try:
            from datetime import datetime
            import pandas as pd
            
            # Collect data from table
            data = []
            for row in range(self.table.rowCount()):
                row_data = {}
                for col in range(self.table.columnCount()):
                    item = self.table.item(row, col)
                    header = self.table.horizontalHeaderItem(col).text()
                    row_data[header] = item.text() if item else ""
                data.append(row_data)
            
            # Export to Excel
            df = pd.DataFrame(data)
            filename = f"cyberninja_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            df.to_excel(filename, index=False)
            
            self.log(f"📤 Exported to {filename}")
            
        except Exception as e:
            self.log(f"🚨 Export error: {e}")
    
    def open_settings(self):
        """Open settings dialog"""
        self.log("🔧 Settings (placeholder)")
    
    def update_queue_info(self):
        """Update queue information display"""
        try:
            signal_info = rabbitmq_api.get_queue_info_sync("signal_queue")
            price_info = rabbitmq_api.get_queue_info_sync("price_queue")
            
            signal_count = signal_info.messages if signal_info else 0
            price_count = price_info.messages if price_info else 0
            
            self.message_count_label.setText(
                f"📊 Signal: {signal_count} | "
                f"Price: {price_count}"
            )
            
            # Update RabbitMQ monitor tab if available
            if hasattr(self, 'rabbitmq_monitor_tab'):
                self.rabbitmq_monitor_tab.update_queue_info()
                
        except Exception as e:
            logger.error(f"Queue info update error: {e}")
    
    def _append_log(self, text: str):
        """Append text to log output"""
        try:
            if self.disable_logging_check.isChecked():
                return
            
            # Throttle logging
            now = QDateTime.currentDateTime()
            if self.last_log_time.msecsTo(now) < LOG_COOLDOWN:
                return
            
            # Limit log history
            if self.log_output.document().blockCount() > LOG_HISTORY_LIMIT:
                cursor = self.log_output.textCursor()
                cursor.movePosition(cursor.Start)
                cursor.select(cursor.BlockUnderCursor)
                cursor.removeSelectedText()
            
            # Add timestamp and append
            timestamp = datetime.now().strftime('%H:%M:%S')
            self.log_output.append(f"[{timestamp}] {text}")
            self.last_log_time = now
            
        except Exception as e:
            logger.error(f"Log append error: {e}")
    
    def log(self, message: str):
        """Convenience logging method"""
        self.LogSignal.emit(message)
    
    def save_config(self):
        """Save configuration to file (excluding tickers which are in watchlist.json)"""
        try:
            config_path = Path(__file__).resolve().parent / 'shared' / 'config.json'
            # Create a copy of config without tickers (they're in watchlist now)
            config_to_save = {k: v for k, v in self.config.items() if k != 'tickers'}
            with open(config_path, 'w') as f:
                json.dump(config_to_save, f, indent=4)
        except Exception as e:
            logger.error(f"Config save error: {e}")
    
    def save_state(self):
        """Save application state"""
        try:
            state = {
                'selected_ticker': self.selected_ticker,
                'live_mode': self.live_mode,
                'strategy_filters': self.strategy_filters,
                'priority_filter': getattr(self, 'priority_filter', {}).get('currentText', 'All Tickers') if hasattr(self, 'priority_filter') else 'All Tickers',
                'timestamp': datetime.now().isoformat()
            }
            
            state_path = Path.home() / '.cyberninja_trader' / 'state.json'
            state_path.parent.mkdir(exist_ok=True)
            
            with open(state_path, 'w') as f:
                json.dump(state, f, indent=4)
                
        except Exception as e:
            logger.error(f"State save error: {e}")
    
    def filter_tickers_by_priority(self, priority_text: str):
        """Filter ticker dropdown by priority"""
        try:
            self.ticker_dropdown.clear()
            
            if priority_text == "All Tickers":
                tickers = self.watchlist_manager.get_all_tickers(enabled_only=True)
            elif priority_text == "High Priority":
                tickers = self.watchlist_manager.get_tickers_by_priority(1)
            elif priority_text == "Medium Priority":
                tickers = self.watchlist_manager.get_tickers_by_priority(2)
            elif priority_text == "Low Priority":
                tickers = self.watchlist_manager.get_tickers_by_priority(3)
            else:
                tickers = self.config.get('tickers', [])
            
            self.ticker_dropdown.addItems(tickers)
            self.log(f"📋 Filtered to {len(tickers)} {priority_text.lower()}")
            
        except Exception as e:
            self.log(f"🚨 Error filtering tickers: {e}")
            logger.error(f"Error filtering tickers: {e}")
    
    def refresh_watchlist(self):
        """Refresh watchlist from shared JSON"""
        try:
            self.watchlist_manager.reload()
            tickers = self.watchlist_manager.get_all_tickers(enabled_only=True)
            self.config['tickers'] = tickers
            
            # Update dropdown
            current_filter = self.priority_filter.currentText()
            self.filter_tickers_by_priority(current_filter)
            
            self.log(f"🔄 Watchlist refreshed: {len(tickers)} tickers")
            
        except Exception as e:
            self.log(f"🚨 Error refreshing watchlist: {e}")
            logger.error(f"Error refreshing watchlist: {e}")
    
    def get_ticker_info_display(self, symbol: str) -> str:
        """Get formatted ticker info for display"""
        try:
            ticker_info = self.watchlist_manager.get_ticker_info(symbol)
            if ticker_info:
                priority_map = {1: "High", 2: "Medium", 3: "Low"}
                priority = priority_map.get(ticker_info.priority, "Unknown")
                return f"{symbol} | Priority: {priority} | Sector: {ticker_info.sector or 'N/A'}"
            return symbol
        except Exception as e:
            logger.error(f"Error getting ticker info for {symbol}: {e}")
            return symbol
    
    def load_state(self):
        """Load application state"""
        try:
            state_path = Path.home() / '.cyberninja_trader' / 'state.json'
            if state_path.exists():
                with open(state_path, 'r') as f:
                    state = json.load(f)
                    
                    # Restore state
                    if 'selected_ticker' in state:
                        self.ticker_dropdown.setCurrentText(state['selected_ticker'])
                    
                    if 'strategy_filters' in state:
                        self.strategy_filters.update(state['strategy_filters'])
                        for strategy, enabled in self.strategy_filters.items():
                            if strategy in self.strategy_buttons:
                                self.strategy_buttons[strategy].setChecked(enabled)
                    
                    logger.info("State restored from previous session")
                    
        except Exception as e:
            logger.error(f"State load error: {e}")
    
    def register_shutdown_handlers(self):
        """Register signal handlers for clean shutdown"""
        def shutdown_handler(signum, frame):
            self.log("📡 Shutdown signal received")
            self.close()
        
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)
        
        # Register atexit handler
        atexit.register(self._atexit_handler)
    
    def _atexit_handler(self):
        """Cleanup on exit"""
        logger.info("Application exit handler triggered")
        if not self.is_shutting_down:
            self.resource_manager.cleanup()
            self.save_state()


# ==================== RABBITMQ MONITOR TAB ====================
class RabbitMQMonitorTab(QWidget):
    """Enhanced RabbitMQ monitoring tab"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_ui()
        
    def setup_ui(self):
        layout = QVBoxLayout(self)
        
        # Title
        title = QLabel("📊 RabbitMQ Monitor")
        title.setAlignment(Qt.AlignCenter)
        title.setFont(QFont("Consolas", 12, QFont.Bold))
        layout.addWidget(title)
        
        # Queue table
        self.table = QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels(
            ["Queue", "Messages", "Consumers", "State", "Actions"]
        )
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.table)
        
        # Add standard queues
        self.queues = ["signal_queue", "price_queue"]
        self.purge_buttons = {}
        
        for queue in self.queues:
            row = self.table.rowCount()
            self.table.insertRow(row)
            
            self.table.setItem(row, 0, QTableWidgetItem(queue))
            self.table.setItem(row, 1, QTableWidgetItem("0"))
            self.table.setItem(row, 2, QTableWidgetItem("0"))
            self.table.setItem(row, 3, QTableWidgetItem("unknown"))
            
            btn = AnimatedButton("♻️ Purge")
            btn.clicked.connect(lambda checked, q=queue: self.purge_queue(q))
            self.purge_buttons[queue] = btn
            self.table.setCellWidget(row, 4, btn)
        
        # Refresh timer
        self.refresh_timer = QTimer(self)
        self.refresh_timer.timeout.connect(self.update_queue_info)
        self.refresh_timer.start(5000)
    
    def update_queue_info(self):
        """Update queue information"""
        for row, queue in enumerate(self.queues):
            try:
                info = rabbitmq_api.get_queue_info_sync(queue)
                if info:
                    self.table.setItem(row, 1, QTableWidgetItem(str(info.messages)))
                    self.table.setItem(row, 2, QTableWidgetItem(str(info.consumers)))
                    self.table.setItem(row, 3, QTableWidgetItem(info.state.value))
                    
                    # Enable/disable purge button
                    self.purge_buttons[queue].setEnabled(
                        info.messages > 0 and info.consumers == 0
                    )
            except Exception as e:
                logger.error(f"Queue info error for {queue}: {e}")
    
    def purge_queue(self, queue_name: str):
        """Purge specified queue"""
        try:
            if rabbitmq_api.purge_queue(queue_name):
                self.parent().log(f"♻️ Purged {queue_name}")
                self.update_queue_info()
            else:
                self.parent().log(f"🚨 Failed to purge {queue_name}")
        except Exception as e:
            self.parent().log(f"🚨 Purge error: {e}")


# ==================== MAIN EXECUTION ====================
def main():
    """Main application entry point"""
    # Set exception handler
    sys.excepthook = handle_exception
    
    try:
        # Create application
        app = QApplication(sys.argv)
        
        # Set application style
        app.setStyle("Fusion")
        
        # Create dark palette
        palette = QPalette()
        palette.setColor(QPalette.Window, QColor(10, 10, 20))
        palette.setColor(QPalette.WindowText, Qt.white)
        palette.setColor(QPalette.Base, QColor(25, 25, 35))
        palette.setColor(QPalette.AlternateBase, QColor(35, 35, 45))
        palette.setColor(QPalette.Text, Qt.white)
        palette.setColor(QPalette.Button, QColor(40, 40, 60))
        palette.setColor(QPalette.ButtonText, Qt.white)
        app.setPalette(palette)
        
        # Create and show main window
        global window
        window = CyberNinjaTraderEnhanced()
        window.setGeometry(100, 100, 1400, 900)
        window.show()
        window.raise_()
        window.activateWindow()
        
        # Load previous state
        window.load_state()
        
        logger.info("Application started successfully")
        
        # Run application
        exit_code = app.exec_()
        
        logger.info(f"Application exited with code: {exit_code}")
        sys.exit(exit_code)
        
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()