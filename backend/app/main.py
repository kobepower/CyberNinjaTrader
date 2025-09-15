import asyncio
import aio_pika
import logging
import json
import time
import sys
import os
from pathlib import Path
import threading
import signal
import websockets
import yfinance as yf
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from collections import deque, defaultdict
import random

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import your existing modules
try:
    import sys
    from pathlib import Path
    # Add project root to path for watchlist_utils
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from watchlist_utils import SharedWatchlistManager
    # Import bottom signal detector from utils
    from utils.bottom_signal_detector import ImprovedBottomSignalDetector
except ImportError as e:
    logging.warning(f"Could not import project modules: {e}")

# ------------------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(threadName)-12s] %(levelname)-8s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
@dataclass
class ServiceConfig:
    """Service configuration with defaults"""
    rabbitmq_host: str = "localhost"
    price_queue: str = "price_queue"
    signal_queue: str = "signal_queue"
    finnhub_api_key: str = ""
    yahoo_batch_size: int = 3
    yahoo_rate_limit_delay: float = 1.5
    fetch_interval: int = 60
    fallback_after_seconds: int = 120
    max_reconnect_attempts: int = 10
    heartbeat_interval: int = 30
    enable_signal_detection: bool = True
    enable_watchlist_sync: bool = True

# ------------------------------------------------------------------------------
# Data Structures
# ------------------------------------------------------------------------------
@dataclass
class MarketData:
    """Market data container"""
    ticker: str
    price: float
    volume: float
    bid: Optional[float] = None
    ask: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    timestamp: datetime = None
    source: str = "unknown"
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "ticker": self.ticker,
            "price": self.price,
            "volume": self.volume,
            "bid": self.bid,
            "ask": self.ask,
            "high": self.high,
            "low": self.low,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source
        }

# ------------------------------------------------------------------------------
# Main Trading Bot
# ------------------------------------------------------------------------------
class EnhancedTradingBot:
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self.config = self._load_comprehensive_config()
        self.service_config = ServiceConfig(**self.config.get("services", {}))
        
        # State management
        self.running = True
        self.services: List[threading.Thread] = []
        self.threads: List[tuple[threading.Thread, asyncio.AbstractEventLoop]] = []
        
        # Market data tracking
        self.last_prices: Dict[str, float] = {}
        self.last_volumes: Dict[str, float] = {}
        self.last_live_update: Dict[str, float] = {}
        self.price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # Service health tracking
        self.service_health: Dict[str, Dict[str, Any]] = {
            "finnhub": {"connected": False, "last_message": None, "errors": 0},
            "yahoo": {"last_fetch": None, "rate_limited": False, "errors": 0},
            "rabbitmq": {"connected": False, "queued": 0, "errors": 0}
        }
        
        # Yahoo rate limiting
        self.yahoo_queue: deque = deque()
        self.yahoo_batch_size = self.service_config.yahoo_batch_size
        self.yahoo_last_call = 0
        self.yahoo_backoff = 1.0
        
        # Signal detectors
        self.signal_detectors: Dict[str, Any] = {}
        self._initialize_signal_detectors()
        
        # Log API key status
        api_key = self.service_config.finnhub_api_key
        if api_key and api_key.strip():
            logger.info(f"✅ Finnhub API key configured: {api_key[:8]}...{api_key[-4:]}")
        else:
            logger.warning("❌ No Finnhub API key found")
        
        # Watchlist management
        self.watchlist_manager = None
        if self.service_config.enable_watchlist_sync:
            self._initialize_watchlist()
    
    # --------------------------------------------------------------------------
    # Configuration Management
    # --------------------------------------------------------------------------
    def _load_comprehensive_config(self) -> Dict[str, Any]:
        """Load and merge configurations from multiple sources"""
        base_config = {
            "services": {
                "rabbitmq_host": "localhost",
                "price_queue": "price_queue",
                "signal_queue": "signal_queue",
                "finnhub_api_key": os.environ.get("FINNHUB_API_KEY", ""),
                "yahoo_batch_size": 3,
                "fetch_interval": 60,
                "fallback_after_seconds": 120
            },
            "tickers": [],
            "strategy": {
                "enable_bottom_detection": True,
                "enable_reversal_detection": True,
                "stop_loss_pct": 0.003,
                "upper_threshold_pct": 0.01,
                "min_volume_factor": 1.5
            },
            "monitoring": {
                "enable_metrics": True,
                "health_check_interval": 60,
                "log_level": "INFO"
            }
        }
        
        # Try to load config file
        if self.config_path.exists():
            try:
                with open(self.config_path, 'r') as f:
                    loaded_config = json.load(f)
                base_config = self._deep_merge(base_config, loaded_config)
                logger.info(f"✅ Loaded configuration from {self.config_path}")
                
                # Extract Finnhub API key from various possible locations
                finnhub_key = None
                
                # Check api_keys.finnhub first (your config structure)
                if "api_keys" in loaded_config and "finnhub" in loaded_config["api_keys"]:
                    finnhub_key = loaded_config["api_keys"]["finnhub"]
                # Check direct finnhub_api_key
                elif "finnhub_api_key" in loaded_config:
                    finnhub_key = loaded_config["finnhub_api_key"]
                # Check services.finnhub_api_key
                elif "services" in loaded_config and "finnhub_api_key" in loaded_config["services"]:
                    finnhub_key = loaded_config["services"]["finnhub_api_key"]
                
                # Set the key in services if found
                if finnhub_key and finnhub_key.strip():
                    base_config["services"]["finnhub_api_key"] = finnhub_key
                    logger.info(f"✅ Found Finnhub API key in config: {finnhub_key[:8]}...")
                    
            except Exception as e:
                logger.error(f"❌ Error loading config: {e}")
        
        # Load tickers from watchlist if empty
        if not base_config.get("tickers"):
            base_config["tickers"] = self._load_watchlist_tickers()
        
        return base_config
    
    def _deep_merge(self, base: Dict, update: Dict) -> Dict:
        """Deep merge two dictionaries"""
        result = base.copy()
        for key, value in update.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
    
    def _load_watchlist_tickers(self) -> List[str]:
        """Load tickers from standardized watchlist JSON"""
        try:
            watchlist_manager = SharedWatchlistManager()
            tickers = watchlist_manager.get_all_tickers(enabled_only=True)
            if tickers:
                logger.info(f"📋 Loaded {len(tickers)} tickers from shared watchlist")
                return tickers
        except Exception as e:
            logger.error(f"Error loading from watchlist manager: {e}")
        
        # Fallback to default tickers
        fallback_tickers = ["AAPL", "NVDA", "AMZN", "META", "TSLA", "PLTR"]
        logger.warning(f"📋 Using fallback tickers: {fallback_tickers}")
        return fallback_tickers
      
        # Fallback tickers
        return ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA"]
    
    def _initialize_watchlist(self):
        """Initialize watchlist manager"""
        try:
            self.watchlist_manager = SharedWatchlistManager()
            logger.info("✅ Shared watchlist manager initialized")
        except Exception as e:
            logger.warning(f"⚠️ Watchlist manager initialization failed: {e}")
    
    def _initialize_signal_detectors(self):
        """Initialize signal detection for each ticker"""
        if not self.service_config.enable_signal_detection:
            return
        
        try:
            strategy_config = self.config.get("strategy", {})
            for ticker in self.config.get("tickers", []):
                detector = ImprovedBottomSignalDetector(
                    symbol=ticker,
                    stop_loss_pct=strategy_config.get("stop_loss_pct", 0.003),
                    upper_threshold_pct=strategy_config.get("upper_threshold_pct", 0.01),
                    verbose=False
                )
                # Set min_volume_factor as attribute if needed
                detector.min_volume_factor = strategy_config.get("min_volume_factor", 1.5)
                self.signal_detectors[ticker] = detector
            logger.info(f"✅ Initialized {len(self.signal_detectors)} signal detectors")
        except ImportError:
            logger.warning("⚠️ Signal detection not available")
    
    # --------------------------------------------------------------------------
    # Finnhub WebSocket Streamer
    # --------------------------------------------------------------------------
    async def price_streamer_service(self):
        """Finnhub WebSocket streaming service with enhanced reconnection"""
        api_key = self.service_config.finnhub_api_key
        if not api_key:
            logger.warning("⚠️ No Finnhub API key - streamer disabled")
            while self.running:
                await asyncio.sleep(10)
            return
        
        reconnect_attempts = 0
        backoff = 2.0
        
        while self.running and reconnect_attempts < self.service_config.max_reconnect_attempts:
            conn = None
            channel = None
            ws = None
            heartbeat_task = None
            
            try:
                # Connect to WebSocket
                uri = f"wss://ws.finnhub.io?token={api_key}"
                ws = await websockets.connect(uri, ping_interval=20, ping_timeout=10)
                logger.info("🟢 Connected to Finnhub WebSocket")
                self.service_health["finnhub"]["connected"] = True
                reconnect_attempts = 0
                
                # Subscribe to tickers
                for ticker in self.config.get("tickers", []):
                    await ws.send(json.dumps({"type": "subscribe", "symbol": ticker}))
                    logger.info(f"📡 Subscribed to {ticker}")
                    await asyncio.sleep(0.1)  # Rate limit subscriptions
                
                # Heartbeat task
                async def send_heartbeat():
                    while self.running:
                        try:
                            # Check if websocket is still open before sending
                            if hasattr(ws, 'open') and not ws.open:
                                logger.warning("WebSocket connection closed, stopping heartbeat")
                                break
                            if hasattr(ws, 'closed') and ws.closed:
                                logger.warning("WebSocket connection closed, stopping heartbeat")
                                break
                            
                            await ws.send(json.dumps({"type": "ping"}))
                            await asyncio.sleep(self.service_config.heartbeat_interval)
                        except Exception as e:
                            logger.warning(f"Heartbeat failed: {e}")
                            break
                
                heartbeat_task = asyncio.create_task(send_heartbeat())
                
                # Connect to RabbitMQ
                conn = await aio_pika.connect_robust(
                    f"amqp://guest:guest@{self.service_config.rabbitmq_host}/"
                )
                channel = await conn.channel()
                
                # Process messages
                while self.running:
                    try:
                        # Check if websocket is still open
                        if hasattr(ws, 'open') and not ws.open:
                            logger.warning("WebSocket connection closed, stopping message processing")
                            break
                        if hasattr(ws, 'closed') and ws.closed:
                            logger.warning("WebSocket connection closed, stopping message processing")
                            break
                        
                        message = await asyncio.wait_for(ws.recv(), timeout=60.0)
                        data = json.loads(message)
                        
                        if data.get("type") == "trade":
                            await self._process_trade_data(data, channel)
                        elif data.get("type") == "ping":
                            logger.debug("🏓 Pong from Finnhub")
                            
                    except asyncio.TimeoutError:
                        logger.warning("⚠️ No message received for 60s")
                        break
                    except json.JSONDecodeError:
                        continue
                        
            except websockets.exceptions.WebSocketException as e:
                logger.error(f"❌ WebSocket error: {e}")
                self.service_health["finnhub"]["errors"] += 1
            except Exception as e:
                logger.error(f"❌ Streamer error: {e}", exc_info=True)
            finally:
                self.service_health["finnhub"]["connected"] = False
                
                if heartbeat_task:
                    heartbeat_task.cancel()
                
                if ws:
                    await ws.close()
                
                if channel:
                    await channel.close()
                    
                if conn and not conn.is_closed:
                    await conn.close()
            
            if self.running:
                reconnect_attempts += 1
                wait_time = min(backoff * (2 ** reconnect_attempts), 300)
                logger.info(f"🔄 Reconnecting in {wait_time}s (attempt {reconnect_attempts})")
                await asyncio.sleep(wait_time)
    
    async def _process_trade_data(self, data: Dict, channel):
        """Process trade data from WebSocket"""
        for trade in data.get("data", []):
            ticker = trade.get("s")
            price = trade.get("p")
            volume = trade.get("v", 0)
            
            if not ticker or price is None:
                continue
            
            # Update tracking
            self.last_prices[ticker] = price
            self.last_volumes[ticker] = volume
            self.last_live_update[ticker] = time.time()
            self.price_history[ticker].append(price)
            
            # Create market data
            market_data = MarketData(
                ticker=ticker,
                price=float(price),
                volume=float(volume),
                source="finnhub",
                timestamp=datetime.now()
            )
            
            # Check for signals if detector exists
            if ticker in self.signal_detectors:
                await self._check_for_signals(market_data, channel)
            
            # Publish to price queue
            await self._publish_price(market_data, channel)
            
            self.service_health["finnhub"]["last_message"] = time.time()
    
    # --------------------------------------------------------------------------
    # Yahoo Finance Fallback
    # --------------------------------------------------------------------------
    async def yahoo_fallback_service(self):
        """Yahoo Finance fallback service with smart rate limiting"""
        logger.info("✅ Yahoo fallback service started")
        
        conn = None
        channel = None
        
        try:
            conn = await aio_pika.connect_robust(
                f"amqp://guest:guest@{self.service_config.rabbitmq_host}/"
            )
            channel = await conn.channel()
            
            while self.running:
                cycle_start = time.time()
                
                # Determine which tickers need updates
                now = time.time()
                fallback_threshold = self.service_config.fallback_after_seconds
                
                needs_update = [
                    ticker for ticker in self.config.get("tickers", [])
                    if (now - self.last_live_update.get(ticker, 0)) > fallback_threshold
                ]
                
                if needs_update:
                    # Process in batches with rate limiting
                    batch_size = min(self.yahoo_batch_size, len(needs_update))
                    batch = needs_update[:batch_size]
                    
                    logger.info(f"📊 Yahoo updating {len(batch)} of {len(needs_update)} stale tickers")
                    
                    for ticker in batch:
                        if not self.running:
                            break
                        
                        # Rate limit check
                        time_since_last = time.time() - self.yahoo_last_call
                        if time_since_last < self.service_config.yahoo_rate_limit_delay:
                            await asyncio.sleep(self.service_config.yahoo_rate_limit_delay - time_since_last)
                        
                        success = await self._fetch_yahoo_price(ticker, channel)
                        self.yahoo_last_call = time.time()
                        
                        if not success:
                            self.service_health["yahoo"]["errors"] += 1
                        
                        # Add jitter
                        await asyncio.sleep(random.uniform(0.1, 0.5))
                
                # Sleep for remainder of interval
                elapsed = time.time() - cycle_start
                sleep_time = max(0, self.service_config.fetch_interval - elapsed)
                await asyncio.sleep(sleep_time)
                
        except Exception as e:
            logger.error(f"❌ Yahoo service error: {e}", exc_info=True)
        finally:
            if channel:
                await channel.close()
            if conn and not conn.is_closed:
                await conn.close()
    
    async def _fetch_yahoo_price(self, ticker: str, channel) -> bool:
        """Fetch price using rate-limited fetcher with fallback"""
        try:
            # Import rate-limited fetcher
            from utils.fetch_prices import get_rate_limited_fetcher
            
            # Get rate-limited fetcher and fetch with fallback
            fetcher = get_rate_limited_fetcher()
            result = await fetcher.fetch_price_with_fallback(ticker)
            
            if not result:
                return False
            
            market_data = MarketData(
                ticker=ticker,
                price=float(result['price']),
                volume=float(result.get('volume', 0)),
                high=float(result.get('high', result['price'])),
                low=float(result.get('low', result['price'])),
                source=result.get('source', 'fallback')
            )
            
            # Update tracking
            self.last_prices[ticker] = market_data.price
            self.last_volumes[ticker] = market_data.volume
            self.price_history[ticker].append(market_data.price)
            
            # Check for signals
            if ticker in self.signal_detectors:
                await self._check_for_signals(market_data, channel)
            
            # Publish price
            await self._publish_price(market_data, channel)
            
            self.service_health["yahoo"]["last_fetch"] = time.time()
            logger.debug(f"📈 Yahoo: {ticker} ${market_data.price:.2f}")
            return True
            
        except Exception as e:
            if "rate limit" in str(e).lower():
                self.service_health["yahoo"]["rate_limited"] = True
                self.yahoo_batch_size = max(1, self.yahoo_batch_size // 2)
                logger.warning(f"🚫 Yahoo rate limited, reducing batch to {self.yahoo_batch_size}")
            else:
                logger.error(f"❌ Yahoo error for {ticker}: {e}")
            return False
    
    # --------------------------------------------------------------------------
    # Signal Detection and Publishing
    # --------------------------------------------------------------------------
    async def _check_for_signals(self, market_data: MarketData, channel):
        """Check for trading signals using detectors"""
        if not self.service_config.enable_signal_detection:
            return
        
        detector = self.signal_detectors.get(market_data.ticker)
        if not detector:
            return
        
        try:
            # Create trade data for detector
            trade_data = {
                "timestamp": market_data.timestamp.timestamp(),
                "price": market_data.price,
                "volume": market_data.volume
            }
            
            # Process through detector
            signal = detector.process_trade(trade_data)
            
            if signal:
                # Enhance signal with additional data
                signal["ticker"] = market_data.ticker
                signal["source"] = market_data.source
                signal["timestamp"] = market_data.timestamp.isoformat()
                
                # Publish to signal queue
                await channel.default_exchange.publish(
                    aio_pika.Message(body=json.dumps(signal).encode()),
                    routing_key=self.service_config.signal_queue
                )
                
                logger.info(f"🎯 Signal detected for {market_data.ticker}: {signal.get('type')}")
                
        except Exception as e:
            logger.error(f"Signal detection error for {market_data.ticker}: {e}")
    
    async def _publish_price(self, market_data: MarketData, channel):
        """Publish price data to RabbitMQ"""
        try:
            message = aio_pika.Message(
                body=json.dumps(market_data.to_dict()).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )
            
            await channel.default_exchange.publish(
                message,
                routing_key=self.service_config.price_queue
            )
            
            self.service_health["rabbitmq"]["queued"] += 1
            
        except Exception as e:
            logger.error(f"Failed to publish price: {e}")
            self.service_health["rabbitmq"]["errors"] += 1
    
    # --------------------------------------------------------------------------
    # Strategy Consumer
    # --------------------------------------------------------------------------
    async def strategy_consumer_service(self):
        """Enhanced strategy consumer with signal processing"""
        logger.info("🧠 Strategy consumer started")
        
        conn = None
        channel = None
        
        try:
            conn = await aio_pika.connect_robust(
                f"amqp://guest:guest@{self.service_config.rabbitmq_host}/"
            )
            channel = await conn.channel()
            queue = await channel.declare_queue(
                self.service_config.signal_queue,
                durable=True
            )
            
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if not self.running:
                        break
                    
                    async with message.process():
                        try:
                            signal = json.loads(message.body.decode())
                            await self._process_signal(signal)
                        except Exception as e:
                            logger.error(f"Error processing signal: {e}")
                            
        except Exception as e:
            logger.error(f"❌ Strategy consumer error: {e}", exc_info=True)
        finally:
            if channel:
                await channel.close()
            if conn and not conn.is_closed:
                await conn.close()
    
    async def _process_signal(self, signal: Dict):
        """Process incoming trading signal"""
        signal_type = signal.get("type", "UNKNOWN")
        ticker = signal.get("ticker", "UNKNOWN")
        confidence = signal.get("confidence", 0)
        
        logger.info(f"📊 Signal: {signal_type} for {ticker} (confidence: {confidence:.1f}%)")
        
        # Here you would add logic to:
        # - Execute trades
        # - Send notifications
        # - Update positions
        # - Log to database
    
    # --------------------------------------------------------------------------
    # Health Monitoring
    # --------------------------------------------------------------------------
    async def health_monitor_service(self):
        """Monitor service health and log statistics"""
        logger.info("🏥 Health monitor started")
        
        while self.running:
            try:
                await asyncio.sleep(self.config.get("monitoring", {}).get("health_check_interval", 60))
                
                # Log health status
                logger.info("=" * 60)
                logger.info("SYSTEM HEALTH CHECK")
                logger.info("=" * 60)
                
                for service, health in self.service_health.items():
                    status = "🟢" if health.get("connected", True) and health.get("errors", 0) < 10 else "🔴"
                    logger.info(f"{status} {service}: {health}")
                
                # Log market data stats
                active_tickers = len([t for t, ts in self.last_live_update.items() 
                                    if time.time() - ts < 300])
                logger.info(f"📊 Active tickers: {active_tickers}/{len(self.config.get('tickers', []))}")
                logger.info(f"💰 Tracked prices: {len(self.last_prices)}")
                
                logger.info("=" * 60)
                
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
    
    # --------------------------------------------------------------------------
    # Service Management
    # --------------------------------------------------------------------------
    def run_service(self, coro_func, loop: asyncio.AbstractEventLoop):
        """Run service with proper error handling"""
        try:
            loop.run_until_complete(coro_func())
        except asyncio.CancelledError:
            logger.info(f"🛑 {coro_func.__name__} cancelled")
        except Exception as e:
            logger.error(f"🔥 Fatal error in {coro_func.__name__}: {e}", exc_info=True)
    
    def launch_service(self, service_coro, name: str):
        """Launch service in separate thread"""
        loop = asyncio.new_event_loop()
        
        def runner():
            asyncio.set_event_loop(loop)
            self.run_service(service_coro, loop)
        
        thread = threading.Thread(target=runner, name=name, daemon=True)
        thread.start()
        self.services.append(thread)
        self.threads.append((thread, loop))
        logger.info(f"🚀 Started {name}")
    
    def run(self):
        """Main run loop"""
        def shutdown_handler(signum, frame):
            logger.info("🛑 Shutdown signal received")
            self.running = False
            
            # Cancel all tasks in all loops
            for thread, loop in self.threads:
                if loop.is_running():
                    def cancel_tasks():
                        for task in asyncio.all_tasks(loop):
                            task.cancel()
                        loop.stop()
                    loop.call_soon_threadsafe(cancel_tasks)
            
            # Wait for threads to finish
            for thread in self.services:
                thread.join(timeout=5.0)
            
            logger.info("✅ Clean shutdown complete")
            sys.exit(0)
        
        # Register signal handlers
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)
        
        # Launch services
        self.launch_service(self.price_streamer_service, "PriceStreamer")
        self.launch_service(self.yahoo_fallback_service, "YahooFallback")
        self.launch_service(self.strategy_consumer_service, "StrategyConsumer")
        self.launch_service(self.health_monitor_service, "HealthMonitor")
        
        logger.info("=" * 60)
        logger.info("🤖 ENHANCED TRADING BOT STARTED")
        logger.info(f"📋 Tracking {len(self.config.get('tickers', []))} tickers")
        logger.info(f"🔧 Signal detection: {self.service_config.enable_signal_detection}")
        logger.info("=" * 60)
        
        # Keep main thread alive
        try:
            while self.running and any(t.is_alive() for t in self.services):
                time.sleep(1)
        except KeyboardInterrupt:
            shutdown_handler(None, None)


# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # Determine config path - use shared/ structure
    project_root = Path(__file__).parent.parent.parent
    config_paths = [
        project_root / "shared" / "config.json",
        Path("shared/config.json"),  # relative fallback
        Path("config.json"),  # legacy fallback
    ]
    
    config_path = None
    for path in config_paths:
        if path.exists():
            config_path = str(path)
            break
    
    if not config_path:
        config_path = str(config_paths[0])  # Use first as default
    
    # Override with command line argument if provided
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    
    try:
        bot = EnhancedTradingBot(config_path=config_path)
        bot.run()
    except Exception as e:
        logger.error(f"🔥 Failed to start bot: {e}", exc_info=True)
        sys.exit(1)