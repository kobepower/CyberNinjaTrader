import logging
import asyncio
import json
import time
from typing import Optional, Dict, List, Callable, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import yfinance as yf
from prometheus_client import Histogram, Counter, Gauge, Summary
from cachetools import TTLCache
from ratelimit import limits, sleep_and_retry
import aiohttp
import websockets
from pathlib import Path
from collections import deque
from enum import Enum
import signal
import sys
import requests
import threading
import os

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Prometheus metrics with better naming and descriptions
FETCH_LATENCY = Histogram(
    'price_fetch_latency_seconds', 
    'Time to fetch price from data source', 
    ['ticker', 'source']
)
FETCH_ERRORS = Counter(
    'price_fetch_errors_total', 
    'Total price fetch errors by source', 
    ['ticker', 'source', 'error_type']
)
WEBSOCKET_MESSAGES = Counter(
    'websocket_messages_total', 
    'Total WebSocket messages received', 
    ['ticker', 'message_type']
)
WEBSOCKET_ERRORS = Counter(
    'websocket_errors_total', 
    'Total WebSocket errors by type',
    ['error_type']
)
WEBSOCKET_CONNECTION = Gauge(
    'websocket_connection_status', 
    'WebSocket connection status (1=connected, 0=disconnected)'
)
CACHE_HITS = Counter(
    'cache_hits_total',
    'Total cache hits',
    ['ticker']
)
CACHE_MISSES = Counter(
    'cache_misses_total',
    'Total cache misses',
    ['ticker']
)
DATA_STALENESS = Gauge(
    'data_staleness_seconds',
    'Age of cached data in seconds',
    ['ticker']
)

# Rate limiting configuration
CALLS_PER_MINUTE = 60
FINNHUB_CALLS_PER_MINUTE = 30  # Finnhub has stricter limits

class DataSource(Enum):
    """Enumeration of data sources"""
    WEBSOCKET = "websocket"
    FINNHUB_REST = "finnhub_rest"
    YFINANCE = "yfinance"
    CACHE = "cache"

# Global rate limiting state
_last_yahoo_request = 0
_last_finnhub_request = 0
_rate_limit_lock = threading.Lock()

class RateLimitedDataFetcher:
    """Rate-limited data fetcher with automatic fallback between Yahoo and Finnhub"""
    
    def __init__(self, finnhub_api_key: Optional[str] = None):
        # Initialize logger first
        self.logger = logging.getLogger(f"{__name__}.RateLimitedFetcher")
        
        # Try to get API key from multiple sources
        self.finnhub_api_key = finnhub_api_key or self._get_finnhub_key()
        # More aggressive throttling to prevent rate limits
        self.finnhub_cooldown = 1.0  # seconds between Finnhub requests (primary source)
        self.yahoo_cooldown = 3.0   # seconds between Yahoo requests (fallback, very conservative)
        
        # Track failures for circuit breaker pattern
        self.yahoo_failures = 0
        self.finnhub_failures = 0
        self.max_failures = 3
        self.failure_reset_time = 300  # 5 minutes
        self.yahoo_failure_timestamp = 0
        self.finnhub_failure_timestamp = 0
    
    def _get_finnhub_key(self) -> Optional[str]:
        """Get Finnhub API key from environment or config files"""
        # Check environment variable first
        env_key = os.environ.get('FINNHUB_API_KEY')
        if env_key and env_key.strip() and env_key != 'your_finnhub_api_key_here':
            return env_key.strip()
        
        # Try to load from config files
        config_paths = [
            Path(__file__).parent.parent.parent.parent / "shared" / "config.json",
            Path("shared/config.json"),
            Path("config.json")
        ]
        
        for config_path in config_paths:
            try:
                if config_path.exists():
                    with open(config_path, 'r') as f:
                        config = json.load(f)
                    
                    # Check api_keys.finnhub (your config structure)
                    if "api_keys" in config and "finnhub" in config["api_keys"]:
                        key = config["api_keys"]["finnhub"]
                        if key and key.strip() and key != 'your_finnhub_api_key_here':
                            self.logger.info(f"Found Finnhub API key in {config_path}")
                            return key.strip()
                    
                    # Check other possible locations
                    for key_path in ["finnhub_api_key", "services.finnhub_api_key"]:
                        parts = key_path.split('.')
                        value = config
                        try:
                            for part in parts:
                                value = value[part]
                            if value and str(value).strip() and value != 'your_finnhub_api_key_here':
                                self.logger.info(f"Found Finnhub API key in {config_path} at {key_path}")
                                return str(value).strip()
                        except (KeyError, TypeError):
                            continue
                            
            except Exception as e:
                self.logger.debug(f"Could not read config from {config_path}: {e}")
                continue
        
        self.logger.warning("No valid Finnhub API key found in environment or config files")
        return None
    
    def _is_yahoo_available(self) -> bool:
        """Check if Yahoo is available (not in circuit breaker state)"""
        if self.yahoo_failures >= self.max_failures:
            if time.time() - self.yahoo_failure_timestamp > self.failure_reset_time:
                self.yahoo_failures = 0
                self.logger.info("Yahoo Finance circuit breaker reset")
            else:
                return False
        return True
    
    def _is_finnhub_available(self) -> bool:
        """Check if Finnhub is available (not in circuit breaker state)"""
        if not self.finnhub_api_key:
            return False
        if self.finnhub_failures >= self.max_failures:
            if time.time() - self.finnhub_failure_timestamp > self.failure_reset_time:
                self.finnhub_failures = 0
                self.logger.info("Finnhub circuit breaker reset")
            else:
                return False
        return True
    
    async def _rate_limited_yahoo_fetch(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch from Yahoo with rate limiting and exponential backoff"""
        global _last_yahoo_request
        
        # Exponential backoff for retries
        max_retries = 3
        base_delay = 1.0
        
        for attempt in range(max_retries):
            # Apply rate limiting
            with _rate_limit_lock:
                current_time = time.time()
                time_since_last = current_time - _last_yahoo_request
                
                if time_since_last < self.yahoo_cooldown:
                    wait_time = self.yahoo_cooldown - time_since_last
                    self.logger.debug(f"Yahoo rate limit: waiting {wait_time:.2f}s for {ticker}")
                    await asyncio.sleep(wait_time)
                
                _last_yahoo_request = time.time()
            
            try:
                loop = asyncio.get_running_loop()
                
                def fetch_yahoo():
                    try:
                        yf_ticker = yf.Ticker(ticker)
                        # Try 1-minute data first for most recent price
                        hist = yf_ticker.history(period="1d", interval="1m")
                        
                        if hist.empty:
                            # Fallback to daily data
                            self.logger.debug(f"No 1m data for {ticker}, trying daily")
                            hist = yf_ticker.history(period="1d")
                        
                        if not hist.empty:
                            latest = hist.iloc[-1]
                            return {
                                'price': float(latest['Close']),
                                'volume': float(latest['Volume']),
                                'high': float(latest['High']),
                                'low': float(latest['Low']),
                                'open': float(latest['Open']),
                                'source': 'yahoo'
                            }
                        else:
                            self.logger.warning(f"No data returned from Yahoo for {ticker}")
                            return None
                    except Exception as e:
                        # Better error classification
                        error_str = str(e).lower()
                        if any(phrase in error_str for phrase in ['429', 'too many requests', 'rate limit']):
                            self.logger.warning(f"Yahoo rate limited for {ticker}: {e}")
                            raise Exception(f"RATE_LIMITED: {e}")
                        elif any(phrase in error_str for phrase in ['timeout', 'connection']):
                            self.logger.warning(f"Yahoo connection issue for {ticker}: {e}")
                            raise Exception(f"CONNECTION_ERROR: {e}")
                        else:
                            self.logger.error(f"Yahoo API error for {ticker}: {e}")
                            raise Exception(f"API_ERROR: {e}")
                
                result = await loop.run_in_executor(None, fetch_yahoo)
                
                if result:
                    # Reset failure count and cooldown on success
                    if self.yahoo_failures > 0:
                        self.logger.info(f"Yahoo fetch recovered for {ticker} after {attempt + 1} attempts")
                    self.yahoo_failures = 0
                    if self.yahoo_cooldown > 1.2:
                        self.yahoo_cooldown = max(1.2, self.yahoo_cooldown * 0.9)  # Slowly reduce cooldown
                    FETCH_LATENCY.labels(ticker=ticker, source='yahoo').observe(time.time() - _last_yahoo_request)
                    return result
                else:
                    # No data returned, but no error - don't retry
                    self.logger.warning(f"Yahoo returned empty data for {ticker}")
                    return None
                    
            except Exception as e:
                error_str = str(e).lower()
                is_rate_limit = 'rate_limited' in error_str or any(phrase in error_str for phrase in ['429', 'too many requests', 'rate limit'])
                is_connection_error = 'connection_error' in error_str or any(phrase in error_str for phrase in ['timeout', 'connection'])
                
                self.yahoo_failures += 1
                self.yahoo_failure_timestamp = time.time()
                
                # Log appropriate error type
                if is_rate_limit:
                    FETCH_ERRORS.labels(ticker=ticker, source='yahoo', error_type='rate_limited').inc()
                    self.logger.warning(f"Yahoo rate limit hit for {ticker} (attempt {attempt + 1}/{max_retries})")
                    # Increase cooldown more aggressively for rate limits
                    self.yahoo_cooldown = min(self.yahoo_cooldown * 2.0, 10.0)
                elif is_connection_error:
                    FETCH_ERRORS.labels(ticker=ticker, source='yahoo', error_type='connection').inc()
                    self.logger.warning(f"Yahoo connection error for {ticker} (attempt {attempt + 1}/{max_retries})")
                else:
                    FETCH_ERRORS.labels(ticker=ticker, source='yahoo', error_type='api_error').inc()
                    self.logger.error(f"Yahoo API error for {ticker} (attempt {attempt + 1}/{max_retries}): {e}")
                
                # If not the last attempt, apply exponential backoff
                if attempt < max_retries - 1:
                    backoff_delay = base_delay * (2 ** attempt)  # 1s, 2s, 4s
                    if is_rate_limit:
                        backoff_delay *= 2  # Double delay for rate limits
                    
                    self.logger.info(f"Backing off for {backoff_delay:.1f}s before retry {attempt + 2}/{max_retries}")
                    await asyncio.sleep(backoff_delay)
                else:
                    # Final attempt failed
                    self.logger.error(f"Yahoo fetch failed for {ticker} after {max_retries} attempts")
                    raise e
        
        # Should never reach here, but just in case
        return None
    
    async def _rate_limited_finnhub_fetch(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch from Finnhub with enhanced rate limiting and retry logic"""
        global _last_finnhub_request
        
        if not self.finnhub_api_key:
            return None
        
        # Retry logic for Finnhub
        max_retries = 2
        base_delay = 0.5
        
        for attempt in range(max_retries):
            # Apply rate limiting
            with _rate_limit_lock:
                current_time = time.time()
                time_since_last = current_time - _last_finnhub_request
                
                if time_since_last < self.finnhub_cooldown:
                    wait_time = self.finnhub_cooldown - time_since_last
                    self.logger.debug(f"Finnhub rate limit: waiting {wait_time:.2f}s for {ticker}")
                    await asyncio.sleep(wait_time)
                
                _last_finnhub_request = time.time()
            
            try:
                url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={self.finnhub_api_key}"
                
                # Use timeout to prevent hanging requests
                timeout = aiohttp.ClientTimeout(total=10)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            # Finnhub returns current price data
                            if 'c' in data and data['c'] > 0:  # 'c' is current price
                                # Reset failure count on success
                                if self.finnhub_failures > 0:
                                    self.logger.info(f"Finnhub recovered for {ticker} after {attempt + 1} attempts")
                                self.finnhub_failures = 0
                                # Slowly reduce cooldown on success
                                if self.finnhub_cooldown > 1.0:
                                    self.finnhub_cooldown = max(1.0, self.finnhub_cooldown * 0.95)
                                
                                FETCH_LATENCY.labels(ticker=ticker, source='finnhub').observe(
                                    time.time() - _last_finnhub_request
                                )
                                
                                return {
                                    'price': float(data['c']),  # current price
                                    'volume': 0,  # Finnhub doesn't provide volume in quote endpoint
                                    'high': float(data.get('h', data['c'])),  # high of day
                                    'low': float(data.get('l', data['c'])),   # low of day
                                    'open': float(data.get('o', data['c'])),  # open price
                                    'source': 'finnhub'
                                }
                            else:
                                self.logger.warning(f"Finnhub returned invalid data for {ticker}: {data}")
                                return None
                        elif response.status == 429:
                            raise Exception("RATE_LIMITED: Finnhub rate limit exceeded")
                        elif response.status == 403:
                            raise Exception("API_ERROR: Invalid Finnhub API key")
                        elif response.status >= 500:
                            raise Exception("SERVER_ERROR: Finnhub server error")
                        else:
                            raise Exception(f"API_ERROR: Finnhub API error: {response.status}")
                
            except asyncio.TimeoutError:
                error_msg = f"TIMEOUT_ERROR: Finnhub request timeout for {ticker}"
                self.logger.warning(f"{error_msg} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    await asyncio.sleep(base_delay * (2 ** attempt))
                    continue
                else:
                    raise Exception(error_msg)
                    
            except Exception as e:
                error_str = str(e).lower()
                is_rate_limit = 'rate_limited' in error_str or '429' in error_str
                is_server_error = 'server_error' in error_str or '5' in error_str
                
                self.finnhub_failures += 1
                self.finnhub_failure_timestamp = time.time()
                
                # Log appropriate error type
                if is_rate_limit:
                    FETCH_ERRORS.labels(ticker=ticker, source='finnhub', error_type='rate_limited').inc()
                    self.logger.warning(f"Finnhub rate limit hit for {ticker} (attempt {attempt + 1}/{max_retries})")
                    # Increase cooldown for rate limits
                    self.finnhub_cooldown = min(self.finnhub_cooldown * 2.0, 5.0)
                elif is_server_error:
                    FETCH_ERRORS.labels(ticker=ticker, source='finnhub', error_type='server_error').inc()
                    self.logger.warning(f"Finnhub server error for {ticker} (attempt {attempt + 1}/{max_retries})")
                else:
                    FETCH_ERRORS.labels(ticker=ticker, source='finnhub', error_type='api_error').inc()
                    self.logger.error(f"Finnhub API error for {ticker} (attempt {attempt + 1}/{max_retries}): {e}")
                
                # If not the last attempt, apply backoff
                if attempt < max_retries - 1:
                    backoff_delay = base_delay * (2 ** attempt)
                    if is_rate_limit:
                        backoff_delay *= 2  # Double delay for rate limits
                    
                    self.logger.info(f"Backing off for {backoff_delay:.1f}s before Finnhub retry")
                    await asyncio.sleep(backoff_delay)
                else:
                    # Final attempt failed
                    self.logger.error(f"Finnhub fetch failed for {ticker} after {max_retries} attempts")
                    raise e
        
        return None
    
    async def fetch_price_with_fallback(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Fetch price with automatic fallback:
        1. Try Finnhub first (more reliable, better rate limits)
        2. If Finnhub fails or returns empty -> use Yahoo as fallback
        3. If both fail -> return None and log error
        """
        start_time = time.time()
        
        # Try Finnhub first if available (primary source)
        if self._is_finnhub_available():
            try:
                self.logger.debug(f"Attempting Finnhub fetch for {ticker}")
                result = await self._rate_limited_finnhub_fetch(ticker)
                if result:
                    self.logger.debug(f"Finnhub fetch successful for {ticker}: ${result['price']:.2f}")
                    return result
            except Exception as e:
                self.logger.warning(f"Finnhub fetch failed for {ticker}: {e}")
        
        # Fallback to Yahoo if Finnhub failed or unavailable
        if self._is_yahoo_available():
            try:
                self.logger.debug(f"Attempting Yahoo fallback for {ticker}")
                result = await self._rate_limited_yahoo_fetch(ticker)
                if result:
                    self.logger.info(f"Yahoo fallback successful for {ticker}: ${result['price']:.2f}")
                    return result
            except Exception as e:
                self.logger.warning(f"Yahoo fallback failed for {ticker}: {e}")
        
        # Both sources failed
        elapsed = time.time() - start_time
        self.logger.error(f"All data sources failed for {ticker} after {elapsed:.2f}s")
        return None
    
    def get_status(self) -> Dict[str, Any]:
        """Get current status of the fetcher"""
        return {
            'yahoo_available': self._is_yahoo_available(),
            'finnhub_available': self._is_finnhub_available(),
            'yahoo_failures': self.yahoo_failures,
            'finnhub_failures': self.finnhub_failures,
            'yahoo_cooldown': self.yahoo_cooldown,
            'finnhub_cooldown': self.finnhub_cooldown
        }

# Global instance for easy access
_global_fetcher = None

def get_rate_limited_fetcher() -> RateLimitedDataFetcher:
    """Get the global rate-limited fetcher instance"""
    global _global_fetcher
    if _global_fetcher is None:
        _global_fetcher = RateLimitedDataFetcher()
    return _global_fetcher

# Convenience function for synchronous code
def fetch_price_sync(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Synchronous wrapper for rate-limited price fetching
    
    This function handles the async/sync bridge and provides a simple
    interface for synchronous code to use the rate-limited fetcher.
    """
    import asyncio
    import concurrent.futures
    import threading
    
    try:
        fetcher = get_rate_limited_fetcher()
        
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If loop is running, we need to run in a thread
                def run_async():
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        return new_loop.run_until_complete(
                            fetcher.fetch_price_with_fallback(ticker)
                        )
                    finally:
                        new_loop.close()
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_async)
                    return future.result(timeout=30)
            else:
                return loop.run_until_complete(
                    fetcher.fetch_price_with_fallback(ticker)
                )
        except RuntimeError:
            # No event loop in current thread
            return asyncio.run(fetcher.fetch_price_with_fallback(ticker))
    
    except Exception as e:
        logger = logging.getLogger('fetch_price_sync')
        logger.error(f"Error fetching price for {ticker}: {e}")
        return None

@dataclass
class PriceData:
    """Structured price data with metadata"""
    ticker: str
    price: float
    volume: float
    bid: Optional[float] = None
    ask: Optional[float] = None
    bid_size: Optional[int] = None
    ask_size: Optional[int] = None
    timestamp: datetime = field(default_factory=datetime.now)
    source: DataSource = DataSource.CACHE
    
    @property
    def age_seconds(self) -> float:
        """Get age of data in seconds"""
        return (datetime.now() - self.timestamp).total_seconds()
    
    @property
    def is_stale(self, max_age: int = 60) -> bool:
        """Check if data is stale (older than max_age seconds)"""
        return self.age_seconds > max_age
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return {
            'ticker': self.ticker,
            'price': self.price,
            'volume': self.volume,
            'bid': self.bid,
            'ask': self.ask,
            'bid_size': self.bid_size,
            'ask_size': self.ask_size,
            'timestamp': self.timestamp.isoformat(),
            'source': self.source.value,
            'age_seconds': self.age_seconds
        }

class EnhancedCache:
    """Enhanced cache with better staleness tracking and metrics"""
    
    def __init__(self, maxsize: int = 100, ttl: int = 300):
        self._cache = TTLCache(maxsize=maxsize, ttl=ttl)
        self._timestamps = {}
        self.logger = logging.getLogger('EnhancedCache')
    
    def get(self, key: str) -> Optional[PriceData]:
        """Get item from cache with metrics"""
        if key in self._cache:
            CACHE_HITS.labels(ticker=key).inc()
            data = self._cache[key]
            if isinstance(data, PriceData):
                DATA_STALENESS.labels(ticker=key).set(data.age_seconds)
            return data
        else:
            CACHE_MISSES.labels(ticker=key).inc()
            return None
    
    def set(self, key: str, value: PriceData):
        """Set item in cache"""
        self._cache[key] = value
        self._timestamps[key] = datetime.now()
        DATA_STALENESS.labels(ticker=key).set(0)
    
    def clear_stale(self, max_age: int = 300):
        """Clear stale entries older than max_age seconds"""
        now = datetime.now()
        keys_to_remove = []
        
        for key, timestamp in self._timestamps.items():
            if (now - timestamp).total_seconds() > max_age:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            self._cache.pop(key, None)
            self._timestamps.pop(key, None)
            self.logger.info(f"Cleared stale cache entry for {key}")
    
    def __contains__(self, key):
        return key in self._cache

# Global cache instance
price_cache = EnhancedCache(maxsize=100, ttl=300)

class PriceStreamer:
    """Enhanced WebSocket manager with better error handling and reconnection logic"""
    
    def __init__(self, config: Dict, update_callback: Optional[Callable] = None):
        self.logger = logging.getLogger('PriceStreamer')
        self.config = config
        self.tickers = config.get('tickers', [])
        self.finnhub_api_key = config.get('finnhub_api_key')
        self.websocket_url = f"wss://ws.finnhub.io?token={self.finnhub_api_key}"
        
        # Connection state
        self.running = False
        self.connected = False
        self.ws_connection = None
        
        # Reconnection settings with exponential backoff
        self.reconnect_delay = 5
        self.max_reconnect_delay = 300  # 5 minutes max
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        
        # Heartbeat settings
        self.heartbeat_interval = 30
        self.last_heartbeat = datetime.now()
        self.missed_heartbeats = 0
        self.max_missed_heartbeats = 3
        
        # Callbacks and tasks
        self.update_callback = update_callback
        self.tasks: List[asyncio.Task] = []
        
        # Message queue for processing
        self.message_queue = asyncio.Queue(maxsize=1000)
        
        # Validate configuration
        self._validate_config()

    def _validate_config(self):
        """Validate WebSocket configuration with better error messages"""
        if not self.finnhub_api_key:
            raise ValueError("Finnhub API key is missing in config. Please provide 'finnhub_api_key'")
        
        if not self.tickers:
            raise ValueError("Tickers list cannot be empty. Please provide at least one ticker")
        
        if len(self.tickers) > 50:
            self.logger.warning(f"Large number of tickers ({len(self.tickers)}). Consider batching for better performance")
        
        # Validate ticker symbols
        invalid_tickers = [t for t in self.tickers if not isinstance(t, str) or len(t) == 0]
        if invalid_tickers:
            raise ValueError(f"Invalid ticker symbols: {invalid_tickers}")

    async def start(self):
        """Start the WebSocket client with improved error handling"""
        self.running = True
        self.logger.info(f"Starting WebSocket client for {len(self.tickers)} tickers")
        
        # Start message processor task
        processor_task = asyncio.create_task(self._process_messages())
        self.tasks.append(processor_task)
        
        # Start periodic cache cleanup task
        cleanup_task = asyncio.create_task(self._periodic_cleanup())
        self.tasks.append(cleanup_task)
        
        while self.running and self.reconnect_attempts < self.max_reconnect_attempts:
            try:
                await self._connect_and_subscribe()
            except Exception as e:
                self.logger.error(f"Connection failed: {e}")
                await self._handle_reconnection()
        
        # Cleanup tasks
        await self._cleanup_tasks()

    async def _connect_and_subscribe(self):
        """Connect to WebSocket and subscribe to tickers"""
        self.logger.info(f"Connecting to Finnhub WebSocket (attempt {self.reconnect_attempts + 1})...")
        
        try:
            async with websockets.connect(
                self.websocket_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            ) as ws:
                self.ws_connection = ws
                self.connected = True
                self.reconnect_attempts = 0
                self.reconnect_delay = 5  # Reset delay on successful connection
                WEBSOCKET_CONNECTION.set(1)
                
                self.logger.info("Successfully connected to WebSocket")
                
                # Subscribe to all tickers
                await self._subscribe_to_tickers(ws)
                
                # Start heartbeat task
                heartbeat_task = asyncio.create_task(self._send_heartbeat(ws))
                self.tasks.append(heartbeat_task)
                
                # Process incoming messages
                await self._receive_messages(ws)
                
        except websockets.exceptions.InvalidStatusCode as e:
            WEBSOCKET_ERRORS.labels(error_type='invalid_status').inc()
            self.logger.error(f"WebSocket connection rejected: {e}")
            raise
        except websockets.exceptions.ConnectionClosed as e:
            WEBSOCKET_ERRORS.labels(error_type='connection_closed').inc()
            self.logger.warning(f"WebSocket connection closed: {e}")
            raise
        except asyncio.TimeoutError:
            WEBSOCKET_ERRORS.labels(error_type='timeout').inc()
            self.logger.error("WebSocket connection timeout")
            raise
        except Exception as e:
            WEBSOCKET_ERRORS.labels(error_type='unknown').inc()
            self.logger.error(f"Unexpected WebSocket error: {e}")
            raise
        finally:
            self.connected = False
            self.ws_connection = None
            WEBSOCKET_CONNECTION.set(0)

    async def _subscribe_to_tickers(self, ws):
        """Subscribe to all configured tickers"""
        for ticker in self.tickers:
            try:
                subscribe_msg = {"type": "subscribe", "symbol": ticker}
                await ws.send(json.dumps(subscribe_msg))
                self.logger.info(f"Subscribed to {ticker}")
                await asyncio.sleep(0.1)  # Small delay between subscriptions
            except Exception as e:
                self.logger.error(f"Failed to subscribe to {ticker}: {e}")

    async def _receive_messages(self, ws):
        """Receive and queue messages for processing"""
        async for message in ws:
            try:
                # Add to queue for processing
                if not self.message_queue.full():
                    await self.message_queue.put(message)
                else:
                    self.logger.warning("Message queue full, dropping message")
                    WEBSOCKET_ERRORS.labels(error_type='queue_full').inc()
            except Exception as e:
                self.logger.error(f"Error queuing message: {e}")

    async def _process_messages(self):
        """Process queued messages asynchronously"""
        while self.running:
            try:
                # Get message with timeout to allow periodic checks
                message = await asyncio.wait_for(
                    self.message_queue.get(), 
                    timeout=1.0
                )
                await self._handle_message(message)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Error processing message: {e}")

    async def _handle_message(self, message: str):
        """Process WebSocket messages with improved parsing"""
        try:
            data = json.loads(message)
            message_type = data.get('type', 'unknown')
            
            if message_type == 'trade':
                await self._handle_trade_message(data)
            elif message_type == 'quote':
                await self._handle_quote_message(data)
            elif message_type == 'ping':
                self.last_heartbeat = datetime.now()
                self.missed_heartbeats = 0
                self.logger.debug("Received heartbeat response")
            elif message_type == 'error':
                self.logger.error(f"Server error: {data.get('msg', 'Unknown error')}")
                WEBSOCKET_ERRORS.labels(error_type='server_error').inc()
            else:
                self.logger.debug(f"Unhandled message type: {message_type}")
                
        except json.JSONDecodeError as e:
            WEBSOCKET_ERRORS.labels(error_type='json_decode').inc()
            self.logger.error(f"Failed to decode message: {e}")
        except Exception as e:
            WEBSOCKET_ERRORS.labels(error_type='message_processing').inc()
            self.logger.error(f"Error handling message: {e}")

    async def _handle_trade_message(self, data: Dict):
        """Handle trade messages"""
        if 'data' not in data:
            return
        
        for trade in data['data']:
            ticker = trade.get('s')
            if ticker not in self.tickers:
                continue
            
            price_data = PriceData(
                ticker=ticker,
                price=float(trade.get('p', 0)),
                volume=float(trade.get('v', 0)),
                timestamp=datetime.fromtimestamp(trade.get('t', time.time()) / 1000),
                source=DataSource.WEBSOCKET
            )
            
            # Update cache
            price_cache.set(ticker, price_data)
            WEBSOCKET_MESSAGES.labels(ticker=ticker, message_type='trade').inc()
            
            # Call update callback if provided
            if self.update_callback:
                try:
                    await self.update_callback(ticker, price_data)
                except Exception as e:
                    self.logger.error(f"Update callback error for {ticker}: {e}")

    async def _handle_quote_message(self, data: Dict):
        """Handle quote (bid/ask) messages"""
        if 'data' not in data:
            return
        
        for quote in data['data']:
            ticker = quote.get('s')
            if ticker not in self.tickers:
                continue
            
            # Get existing price data or create new
            existing = price_cache.get(ticker)
            if existing:
                existing.bid = float(quote.get('b', 0))
                existing.ask = float(quote.get('a', 0))
                existing.bid_size = int(quote.get('bv', 0))
                existing.ask_size = int(quote.get('av', 0))
                price_cache.set(ticker, existing)
            else:
                price_data = PriceData(
                    ticker=ticker,
                    price=(float(quote.get('b', 0)) + float(quote.get('a', 0))) / 2,
                    volume=0,
                    bid=float(quote.get('b', 0)),
                    ask=float(quote.get('a', 0)),
                    bid_size=int(quote.get('bv', 0)),
                    ask_size=int(quote.get('av', 0)),
                    timestamp=datetime.fromtimestamp(quote.get('t', time.time()) / 1000),
                    source=DataSource.WEBSOCKET
                )
                price_cache.set(ticker, price_data)
            
            WEBSOCKET_MESSAGES.labels(ticker=ticker, message_type='quote').inc()

    async def _send_heartbeat(self, ws):
        """Send periodic heartbeat with health checking"""
        while self.connected and self.running:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                
                # Check if we've missed too many heartbeats
                time_since_last = (datetime.now() - self.last_heartbeat).total_seconds()
                if time_since_last > self.heartbeat_interval * self.max_missed_heartbeats:
                    self.logger.warning(f"No heartbeat response for {time_since_last:.0f} seconds")
                    self.missed_heartbeats += 1
                    if self.missed_heartbeats >= self.max_missed_heartbeats:
                        self.logger.error("Too many missed heartbeats, disconnecting")
                        await ws.close()
                        break
                
                # Send heartbeat
                ping_msg = {"type": "ping"}
                await ws.send(json.dumps(ping_msg))
                self.logger.debug("Sent heartbeat")
                
            except websockets.exceptions.ConnectionClosed:
                self.logger.warning("Connection closed during heartbeat")
                break
            except Exception as e:
                WEBSOCKET_ERRORS.labels(error_type='heartbeat').inc()
                self.logger.error(f"Heartbeat error: {e}")

    async def _handle_reconnection(self):
        """Handle reconnection with exponential backoff"""
        self.reconnect_attempts += 1
        
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            self.logger.error(f"Max reconnection attempts ({self.max_reconnect_attempts}) reached")
            self.running = False
            return
        
        # Exponential backoff with jitter
        delay = min(
            self.reconnect_delay * (2 ** (self.reconnect_attempts - 1)),
            self.max_reconnect_delay
        )
        jitter = delay * 0.1 * (0.5 - asyncio.get_event_loop().time() % 1)
        delay += jitter
        
        self.logger.info(f"Reconnecting in {delay:.1f} seconds...")
        await asyncio.sleep(delay)

    async def _periodic_cleanup(self):
        """Periodically clean up stale cache entries"""
        while self.running:
            try:
                await asyncio.sleep(60)  # Run every minute
                price_cache.clear_stale(max_age=300)
            except Exception as e:
                self.logger.error(f"Cleanup error: {e}")

    async def _cleanup_tasks(self):
        """Clean up all running tasks"""
        for task in self.tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self.tasks.clear()

    async def stop(self):
        """Gracefully stop the WebSocket client"""
        self.logger.info("Stopping WebSocket client...")
        self.running = False
        
        # Close WebSocket connection if active
        if self.ws_connection:
            await self.ws_connection.close()
        
        # Wait for tasks to complete
        await self._cleanup_tasks()
        
        WEBSOCKET_CONNECTION.set(0)
        self.logger.info("WebSocket client stopped")


class FinnhubRESTClient:
    """REST API client for Finnhub with rate limiting"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://finnhub.io/api/v1"
        self.logger = logging.getLogger('FinnhubREST')
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    @sleep_and_retry
    @limits(calls=FINNHUB_CALLS_PER_MINUTE, period=60)
    async def fetch_quote(self, ticker: str) -> Optional[PriceData]:
        """Fetch quote from Finnhub REST API"""
        if not self.session:
            self.session = aiohttp.ClientSession()
        
        start_time = time.time()
        url = f"{self.base_url}/quote"
        params = {'symbol': ticker, 'token': self.api_key}
        
        try:
            async with self.session.get(url, params=params, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    price_data = PriceData(
                        ticker=ticker,
                        price=float(data.get('c', 0)),  # Current price
                        volume=float(data.get('v', 0)),  # Volume
                        bid=float(data.get('b', 0)),     # Bid
                        ask=float(data.get('a', 0)),     # Ask
                        timestamp=datetime.fromtimestamp(data.get('t', time.time())),
                        source=DataSource.FINNHUB_REST
                    )
                    
                    FETCH_LATENCY.labels(ticker=ticker, source='finnhub_rest').observe(
                        time.time() - start_time
                    )
                    
                    return price_data
                else:
                    self.logger.error(f"Finnhub API error for {ticker}: {response.status}")
                    FETCH_ERRORS.labels(
                        ticker=ticker, 
                        source='finnhub_rest', 
                        error_type=f'http_{response.status}'
                    ).inc()
                    return None
                    
        except asyncio.TimeoutError:
            FETCH_ERRORS.labels(ticker=ticker, source='finnhub_rest', error_type='timeout').inc()
            self.logger.error(f"Finnhub timeout for {ticker}")
            return None
        except Exception as e:
            FETCH_ERRORS.labels(ticker=ticker, source='finnhub_rest', error_type='exception').inc()
            self.logger.error(f"Finnhub error for {ticker}: {e}")
            return None


@sleep_and_retry
@limits(calls=CALLS_PER_MINUTE, period=60)
async def fetch_price_yfinance(ticker: str) -> Optional[PriceData]:
    """
    Fetch price using rate-limited fetcher with Yahoo/Finnhub fallback
    
    This function now uses the RateLimitedDataFetcher which:
    1. Applies 1.2s cooldown between Yahoo Finance requests
    2. Falls back to Finnhub if Yahoo fails
    3. Includes circuit breaker logic to handle repeated failures
    """
    start_time = time.time()
    logger = logging.getLogger('RateLimitedFetcher')

    try:
        fetcher = get_rate_limited_fetcher()
        result = await fetcher.fetch_price_with_fallback(ticker)
        
        if result:
            # Determine source for metrics
            source_name = result.get('source', 'unknown')
            data_source = DataSource.YFINANCE if source_name == 'yahoo' else DataSource.FINNHUB_REST
            
            price_data = PriceData(
                ticker=ticker,
                price=result['price'],
                volume=result['volume'],
                source=data_source
            )
            
            # Calculate bid/ask spread estimate (typically 0.01% for liquid stocks)
            spread = price_data.price * 0.0001
            price_data.bid = price_data.price - spread
            price_data.ask = price_data.price + spread
            
            logger.info(f"Fetched {ticker} from {source_name}: ${price_data.price:.2f}")
            return price_data
        else:
            FETCH_ERRORS.labels(ticker=ticker, source='fallback', error_type='all_sources_failed').inc()
            logger.warning(f"All data sources failed for {ticker}")
            return None

    except Exception as e:
        logger.error(f"Rate-limited fetcher error for {ticker}: {e}")
        FETCH_ERRORS.labels(ticker=ticker, source='fallback', error_type='exception').inc()
        return None


async def fetch_price(
    ticker: str, 
    max_staleness: int = 60,
    prefer_realtime: bool = True
) -> Optional[PriceData]:
    """
    Fetch price with multiple fallback sources and staleness checking
    
    Args:
        ticker: Stock symbol
        max_staleness: Maximum age of cached data in seconds
        prefer_realtime: Prefer real-time sources over cached data
    
    Returns:
        PriceData object or None
    """
    logger = logging.getLogger('PriceFetcher')
    
    # Check cache first
    cached_data = price_cache.get(ticker)
    if cached_data and not cached_data.is_stale and not prefer_realtime:
        logger.debug(f"Returning cached price for {ticker}: ${cached_data.price:.2f}")
        return cached_data
    
    # If data is stale or we prefer realtime, try to get fresh data
    if prefer_realtime or (cached_data and cached_data.age_seconds > max_staleness):
        # Try Finnhub REST API first if we have a key
        if hasattr(fetch_price, '_finnhub_client'):
            try:
                fresh_data = await fetch_price._finnhub_client.fetch_quote(ticker)
                if fresh_data:
                    price_cache.set(ticker, fresh_data)
                    return fresh_data
            except Exception as e:
                logger.warning(f"Finnhub REST failed for {ticker}: {e}")
        
        # Fallback to yfinance
        fresh_data = await fetch_price_yfinance(ticker)
        if fresh_data:
            price_cache.set(ticker, fresh_data)
            return fresh_data
    
    # Return cached data even if stale (better than nothing)
    if cached_data:
        logger.warning(f"Returning stale data for {ticker} (age: {cached_data.age_seconds:.0f}s)")
        return cached_data
    
    logger.error(f"No price data available for {ticker}")
    return None


# Global instances
price_streamer: Optional[PriceStreamer] = None
finnhub_client: Optional[FinnhubRESTClient] = None


def init_price_system(config: Dict, update_callback: Optional[Callable] = None):
    """Initialize the complete price system"""
    global price_streamer, finnhub_client
    
    # Initialize WebSocket streamer
    price_streamer = PriceStreamer(config, update_callback)
    
    # Initialize REST client if API key provided
    if config.get('finnhub_api_key'):
        finnhub_client = FinnhubRESTClient(config['finnhub_api_key'])
        # Attach to fetch_price function for easy access
        fetch_price._finnhub_client = finnhub_client
    
    logging.info("Price system initialized")


async def start_price_system():
    """Start all price system components"""
    tasks = []
    
    if price_streamer:
        tasks.append(asyncio.create_task(price_streamer.start()))
    
    if finnhub_client:
        await finnhub_client.__aenter__()
    
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def stop_price_system():
    """Stop all price system components"""
    if price_streamer:
        await price_streamer.stop()
    
    if finnhub_client:
        await finnhub_client.__aexit__(None, None, None)


def signal_handler(sig, frame):
    """Handle shutdown signals gracefully"""
    logging.info(f"Received signal {sig}, shutting down...")
    if price_streamer:
        asyncio.create_task(price_streamer.stop())
    sys.exit(0)


# Example usage and testing
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Price fetching system")
    parser.add_argument('--tickers', nargs='+', default=['AAPL', 'MSFT', 'GOOGL'],
                       help='Tickers to monitor')
    parser.add_argument('--api-key', type=str, required=True,
                       help='Finnhub API key')
    parser.add_argument('--test-mode', action='store_true',
                       help='Run in test mode')
    args = parser.parse_args()
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Configuration
    config = {
        "tickers": args.tickers,
        "finnhub_api_key": args.api_key
    }
    
    async def update_callback(ticker: str, price_data: PriceData):
        """Example callback for price updates"""
        print(f"[UPDATE] {ticker}: ${price_data.price:.2f} "
              f"(Volume: {price_data.volume:,.0f}) "
              f"Source: {price_data.source.value}")
    
    async def test_system():
        """Test the price system"""
        # Initialize
        init_price_system(config, update_callback)
        
        # Start system
        start_task = asyncio.create_task(start_price_system())
        
        # Wait for connection
        await asyncio.sleep(3)
        
        # Test fetching prices
        for ticker in config['tickers']:
            price_data = await fetch_price(ticker)
            if price_data:
                print(f"\n{ticker}:")
                print(f"  Price: ${price_data.price:.2f}")
                print(f"  Volume: {price_data.volume:,.0f}")
                print(f"  Bid/Ask: ${price_data.bid:.2f}/${price_data.ask:.2f}")
                print(f"  Source: {price_data.source.value}")
                print(f"  Age: {price_data.age_seconds:.1f}s")
        
        # Monitor for a while
        if not args.test_mode:
            print("\nMonitoring prices... (Ctrl+C to stop)")
            try:
                await asyncio.sleep(3600)  # Run for an hour
            except KeyboardInterrupt:
                pass
        
        # Cleanup
        await stop_price_system()
        start_task.cancel()
        try:
            await start_task
        except asyncio.CancelledError:
            pass
    
    # Run
    asyncio.run(test_system())