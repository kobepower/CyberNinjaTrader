import sys
import os
import asyncio
import logging
import json
import signal
from typing import Dict, List, Optional, Set, Any, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from collections import defaultdict, deque
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from enum import Enum

# Adjust sys.path to include parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.historical_data import get_previous_day_low, HistoricalDataFetcher
from utils.realtime_data import subscribe_to_ticker, Trade
from utils.bottom_signal_detector import ImprovedBottomSignalDetector
from utils.rabbitmq_producer import publish_signal
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from watchlist_utils import load_watchlist

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_TOLERANCE = 0.003  # 0.3% tolerance for near-support detection
DEFAULT_MIN_VOLUME_FACTOR = 1.5
DEFAULT_AVG_VOLUME_WINDOW = 20
MAX_CONCURRENT_SUBSCRIPTIONS = 10
HEALTH_CHECK_INTERVAL = 30  # seconds
STATS_REPORT_INTERVAL = 60  # seconds

class SignalType(Enum):
    """Types of trading signals"""
    BOTTOM = "bottom"
    REVERSAL = "reversal"
    BREAKOUT = "breakout"
    VOLUME_SPIKE = "volume_spike"

class SubscriptionStatus(Enum):
    """Status of ticker subscriptions"""
    PENDING = "pending"
    CONNECTED = "connected"
    FAILED = "failed"
    RECONNECTING = "reconnecting"
    STOPPED = "stopped"

@dataclass
class TickerConfig:
    """Configuration for each ticker"""
    symbol: str
    prev_low: Optional[float] = None
    prev_high: Optional[float] = None
    prev_close: Optional[float] = None
    tolerance: float = DEFAULT_TOLERANCE
    min_volume_factor: float = DEFAULT_MIN_VOLUME_FACTOR
    enabled: bool = True
    
@dataclass
class TickerStats:
    """Statistics for each ticker"""
    symbol: str
    trades_processed: int = 0
    signals_generated: int = 0
    reversal_alerts: int = 0
    last_trade_time: Optional[datetime] = None
    volume_history: deque = field(default_factory=lambda: deque(maxlen=DEFAULT_AVG_VOLUME_WINDOW))
    subscription_status: SubscriptionStatus = SubscriptionStatus.PENDING
    errors: int = 0
    last_error: Optional[str] = None
    
    @property
    def average_volume(self) -> float:
        """Calculate average volume"""
        if not self.volume_history:
            return 1000  # Default
        return sum(self.volume_history) / len(self.volume_history)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'symbol': self.symbol,
            'trades_processed': self.trades_processed,
            'signals_generated': self.signals_generated,
            'reversal_alerts': self.reversal_alerts,
            'last_trade_time': self.last_trade_time.isoformat() if self.last_trade_time else None,
            'average_volume': self.average_volume,
            'subscription_status': self.subscription_status.value,
            'errors': self.errors,
            'last_error': self.last_error
        }

@dataclass
class Signal:
    """Enhanced signal data structure"""
    symbol: str
    type: SignalType
    price: float
    volume: float
    timestamp: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)
    progress: int = 0
    confidence: float = 0.0
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'symbol': self.symbol,
            'type': self.type.value,
            'price': self.price,
            'volume': self.volume,
            'timestamp': self.timestamp.isoformat(),
            'progress': self.progress,
            'confidence': self.confidence,
            'metadata': self.metadata
        }

class MultiTickerRunner:
    """Enhanced multi-ticker runner with better orchestration"""
    
    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        rabbitmq_enabled: bool = True
    ):
        self.config = config or {}
        self.rabbitmq_enabled = rabbitmq_enabled
        
        # Ticker management
        self.ticker_configs: Dict[str, TickerConfig] = {}
        self.ticker_stats: Dict[str, TickerStats] = defaultdict(lambda: None)
        self.detectors: Dict[str, ImprovedBottomSignalDetector] = {}
        
        # Task management
        self.subscription_tasks: Dict[str, asyncio.Task] = {}
        self.health_check_task: Optional[asyncio.Task] = None
        self.stats_reporter_task: Optional[asyncio.Task] = None
        
        # State
        self.running = False
        self.shutting_down = False
        
        # Performance
        self.thread_pool = ThreadPoolExecutor(max_workers=5)
        self.historical_fetcher = HistoricalDataFetcher()
        
        # Metrics
        self.total_signals = 0
        self.total_trades = 0
        self.start_time = datetime.now()
        
    async def initialize_tickers(self, tickers: List[str]):
        """Initialize ticker configurations and fetch historical data"""
        logger.info(f"Initializing {len(tickers)} tickers...")
        
        # Fetch historical data in parallel
        tasks = []
        for ticker in tickers:
            self.ticker_configs[ticker] = TickerConfig(symbol=ticker)
            self.ticker_stats[ticker] = TickerStats(symbol=ticker)
            tasks.append(self._fetch_ticker_history(ticker))
        
        # Process results
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        valid_tickers = []
        for ticker, result in zip(tickers, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to initialize {ticker}: {result}")
                self.ticker_stats[ticker].errors += 1
                self.ticker_stats[ticker].last_error = str(result)
            elif result:
                valid_tickers.append(ticker)
                logger.info(f"✅ {ticker} initialized - Prev Low: ${self.ticker_configs[ticker].prev_low:.2f}")
            else:
                logger.warning(f"⚠️ {ticker} - No historical data available")
        
        # Initialize detectors for valid tickers
        for ticker in valid_tickers:
            self.detectors[ticker] = ImprovedBottomSignalDetector(
                ticker,
                verbose=self.config.get('verbose', False)
            )
        
        logger.info(f"Successfully initialized {len(valid_tickers)}/{len(tickers)} tickers")
        return valid_tickers
    
    async def _fetch_ticker_history(self, ticker: str) -> bool:
        """Fetch historical data for a single ticker"""
        try:
            # Get previous day data
            prev_data = await self.historical_fetcher.get_previous_day_data(ticker)
            
            if prev_data:
                config = self.ticker_configs[ticker]
                config.prev_low = prev_data['low']
                config.prev_high = prev_data['high']
                config.prev_close = prev_data['close']
                
                # Initialize volume history with recent data
                bars = await self.historical_fetcher.get_historical_bars(
                    ticker, period="5d", interval="1d"
                )
                if bars:
                    stats = self.ticker_stats[ticker]
                    for bar in bars[-DEFAULT_AVG_VOLUME_WINDOW:]:
                        stats.volume_history.append(bar.volume)
                
                return True
            return False
            
        except Exception as e:
            logger.error(f"Error fetching history for {ticker}: {e}")
            raise
    
    def is_near_support(self, trade: Trade, tolerance: Optional[float] = None) -> bool:
        """Check if trade is near previous day low (support)"""
        config = self.ticker_configs.get(trade.symbol)
        if not config or not config.prev_low:
            return False
        
        tolerance = tolerance or config.tolerance
        distance_ratio = abs(trade.price - config.prev_low) / config.prev_low
        return distance_ratio <= tolerance
    
    def is_volume_significant(self, trade: Trade) -> bool:
        """Check if trade volume is significant"""
        stats = self.ticker_stats.get(trade.symbol)
        config = self.ticker_configs.get(trade.symbol)
        
        if not stats or not config:
            return False
        
        avg_volume = stats.average_volume
        min_volume = avg_volume * config.min_volume_factor
        
        return trade.volume >= min_volume
    
    def calculate_signal_confidence(self, trade: Trade) -> float:
        """Calculate confidence score for a signal (0-100)"""
        confidence = 0.0
        config = self.ticker_configs.get(trade.symbol)
        stats = self.ticker_stats.get(trade.symbol)
        
        if not config or not stats:
            return confidence
        
        # Near support level (up to 40 points)
        if config.prev_low:
            distance_ratio = abs(trade.price - config.prev_low) / config.prev_low
            support_score = max(0, 40 * (1 - distance_ratio / 0.01))  # Linear decay over 1%
            confidence += support_score
        
        # Volume significance (up to 30 points)
        if stats.average_volume > 0:
            volume_ratio = trade.volume / stats.average_volume
            volume_score = min(30, 30 * (volume_ratio / 3))  # Max at 3x average
            confidence += volume_score
        
        # Price action (up to 30 points)
        # This would need more data like bid/ask spread, recent price movement, etc.
        # For now, simplified
        if config.prev_close:
            if trade.price > config.prev_close:  # Trading above previous close
                confidence += 15
        
        return min(100, confidence)
    
    async def handle_trade(self, trade: Trade):
        """Process incoming trade"""
        try:
            # Update statistics
            stats = self.ticker_stats.get(trade.symbol)
            if not stats:
                return
            
            stats.trades_processed += 1
            stats.last_trade_time = datetime.now()
            stats.volume_history.append(trade.volume)
            self.total_trades += 1
            
            # Check for reversal conditions
            if self.is_near_support(trade):
                if self.is_volume_significant(trade):
                    stats.reversal_alerts += 1
                    await self.emit_reversal_alert(trade)
            
            # Run bottom detector
            detector = self.detectors.get(trade.symbol)
            if detector:
                signal_data = detector.process_trade(trade)
                if signal_data:
                    await self.process_signal(trade, signal_data)
            
        except Exception as e:
            logger.error(f"Error handling trade for {trade.symbol}: {e}")
            if stats:
                stats.errors += 1
                stats.last_error = str(e)
    
    async def emit_reversal_alert(self, trade: Trade):
        """Emit a reversal alert"""
        config = self.ticker_configs.get(trade.symbol)
        if not config:
            return
        
        signal = Signal(
            symbol=trade.symbol,
            type=SignalType.REVERSAL,
            price=trade.price,
            volume=trade.volume,
            timestamp=datetime.now(),
            metadata={
                'prev_low': config.prev_low,
                'prev_high': config.prev_high,
                'distance_from_support': abs(trade.price - config.prev_low) if config.prev_low else None
            },
            confidence=self.calculate_signal_confidence(trade)
        )
        
        logger.info(f"🔔 REVERSAL ALERT: {signal.symbol} @ ${signal.price:.2f} "
                   f"(Confidence: {signal.confidence:.1f}%)")
        
        # Emit to stdout for GUI
        print(json.dumps(signal.to_dict()))
        
        # Publish to RabbitMQ if enabled
        if self.rabbitmq_enabled:
            await self.publish_to_rabbitmq(signal)
    
    async def process_signal(self, trade: Trade, signal_data: Dict):
        """Process and emit a bottom signal"""
        config = self.ticker_configs.get(trade.symbol)
        stats = self.ticker_stats.get(trade.symbol)
        
        if not config or not stats:
            return
        
        stats.signals_generated += 1
        self.total_signals += 1
        
        # Calculate progress
        if config.prev_low:
            distance = abs(trade.price - config.prev_low)
            range_span = config.prev_low * 0.01  # 1% threshold
            progress = max(0, min(100, 100 - (distance / range_span * 100)))
        else:
            progress = 0
        
        signal = Signal(
            symbol=trade.symbol,
            type=SignalType.BOTTOM,
            price=trade.price,
            volume=trade.volume,
            timestamp=datetime.now(),
            progress=int(progress),
            confidence=self.calculate_signal_confidence(trade),
            metadata=signal_data
        )
        
        logger.info(f"🎯 BOTTOM SIGNAL: {signal.symbol} @ ${signal.price:.2f} "
                   f"(Progress: {signal.progress}%, Confidence: {signal.confidence:.1f}%)")
        
        # Emit to stdout for GUI
        print(json.dumps(signal.to_dict()))
        
        # Publish to RabbitMQ if enabled
        if self.rabbitmq_enabled:
            await self.publish_to_rabbitmq(signal)
    
    async def publish_to_rabbitmq(self, signal: Signal):
        """Publish signal to RabbitMQ"""
        try:
            publish_signal(signal.to_dict())
            logger.debug(f"📬 Published {signal.type.value} signal for {signal.symbol}")
        except Exception as e:
            logger.error(f"❌ Failed to publish signal: {e}")
    
    async def subscribe_to_ticker_with_retry(self, ticker: str):
        """Subscribe to a ticker with automatic retry on failure"""
        stats = self.ticker_stats[ticker]
        max_retries = 5
        retry_count = 0
        base_delay = 2
        
        while not self.shutting_down and retry_count < max_retries:
            try:
                stats.subscription_status = SubscriptionStatus.RECONNECTING if retry_count > 0 else SubscriptionStatus.PENDING
                
                logger.info(f"Subscribing to {ticker} (attempt {retry_count + 1}/{max_retries})")
                
                # Create trade handler for this ticker
                async def ticker_handler(trade: Trade):
                    await self.handle_trade(trade)
                
                # Subscribe
                stats.subscription_status = SubscriptionStatus.CONNECTED
                await subscribe_to_ticker(
                    ticker,
                    ticker_handler,
                    max_retries=3,
                    initial_delay=2
                )
                
                # If we get here, subscription ended normally
                break
                
            except asyncio.CancelledError:
                logger.info(f"Subscription to {ticker} cancelled")
                stats.subscription_status = SubscriptionStatus.STOPPED
                raise
            except Exception as e:
                retry_count += 1
                stats.errors += 1
                stats.last_error = str(e)
                stats.subscription_status = SubscriptionStatus.FAILED
                
                if retry_count < max_retries and not self.shutting_down:
                    delay = base_delay * (2 ** (retry_count - 1))
                    logger.warning(f"Subscription to {ticker} failed: {e}. Retrying in {delay}s...")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Failed to subscribe to {ticker} after {max_retries} attempts")
                    break
    
    async def health_check_loop(self):
        """Periodic health check of all subscriptions"""
        while not self.shutting_down:
            try:
                await asyncio.sleep(HEALTH_CHECK_INTERVAL)
                
                # Check subscription health
                for ticker, stats in self.ticker_stats.items():
                    if stats.subscription_status == SubscriptionStatus.FAILED:
                        # Try to restart failed subscriptions
                        if ticker in self.subscription_tasks:
                            task = self.subscription_tasks[ticker]
                            if task.done():
                                logger.info(f"Restarting failed subscription for {ticker}")
                                new_task = asyncio.create_task(
                                    self.subscribe_to_ticker_with_retry(ticker)
                                )
                                self.subscription_tasks[ticker] = new_task
                    
                    # Check for stale data
                    if stats.last_trade_time:
                        time_since_trade = (datetime.now() - stats.last_trade_time).total_seconds()
                        if time_since_trade > 300:  # 5 minutes
                            logger.warning(f"No trades for {ticker} in {time_since_trade:.0f} seconds")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")
    
    async def stats_reporter_loop(self):
        """Periodic statistics reporting"""
        while not self.shutting_down:
            try:
                await asyncio.sleep(STATS_REPORT_INTERVAL)
                
                # Calculate runtime
                runtime = (datetime.now() - self.start_time).total_seconds()
                hours = runtime / 3600
                
                # Generate report
                logger.info("=" * 60)
                logger.info("STATISTICS REPORT")
                logger.info("=" * 60)
                logger.info(f"Runtime: {hours:.2f} hours")
                logger.info(f"Total Trades: {self.total_trades:,}")
                logger.info(f"Total Signals: {self.total_signals}")
                logger.info(f"Trades/min: {(self.total_trades / (runtime / 60)):.2f}")
                
                # Per-ticker stats
                for ticker, stats in self.ticker_stats.items():
                    if stats.trades_processed > 0:
                        logger.info(f"  {ticker}: {stats.trades_processed:,} trades, "
                                  f"{stats.signals_generated} signals, "
                                  f"{stats.reversal_alerts} reversals, "
                                  f"Status: {stats.subscription_status.value}")
                
                logger.info("=" * 60)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Stats reporter error: {e}")
    
    async def start(self, tickers: List[str]):
        """Start the multi-ticker runner"""
        logger.info(f"📡 Starting Multi-Ticker Runner with {len(tickers)} tickers...")
        
        self.running = True
        self.start_time = datetime.now()
        
        # Initialize tickers and fetch historical data
        valid_tickers = await self.initialize_tickers(tickers)
        
        if not valid_tickers:
            logger.error("No valid tickers to track!")
            return
        
        # Start health check and stats reporter
        self.health_check_task = asyncio.create_task(self.health_check_loop())
        self.stats_reporter_task = asyncio.create_task(self.stats_reporter_loop())
        
        # Start subscriptions with rate limiting
        for i, ticker in enumerate(valid_tickers):
            if i > 0 and i % MAX_CONCURRENT_SUBSCRIPTIONS == 0:
                await asyncio.sleep(1)  # Rate limiting
            
            task = asyncio.create_task(self.subscribe_to_ticker_with_retry(ticker))
            self.subscription_tasks[ticker] = task
        
        logger.info(f"✅ Started subscriptions for {len(valid_tickers)} tickers")
        
        # Wait for all tasks
        try:
            await asyncio.gather(*self.subscription_tasks.values())
        except asyncio.CancelledError:
            logger.info("Subscriptions cancelled")
        except Exception as e:
            logger.error(f"Error in subscriptions: {e}")
    
    async def stop(self):
        """Gracefully stop the runner"""
        logger.info("🛑 Shutting down Multi-Ticker Runner...")
        self.shutting_down = True
        
        # Cancel all subscription tasks
        for ticker, task in self.subscription_tasks.items():
            if not task.done():
                task.cancel()
                logger.debug(f"Cancelled subscription for {ticker}")
        
        # Wait for tasks to complete
        if self.subscription_tasks:
            await asyncio.gather(*self.subscription_tasks.values(), return_exceptions=True)
        
        # Cancel health check and stats reporter
        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass
        
        if self.stats_reporter_task:
            self.stats_reporter_task.cancel()
            try:
                await self.stats_reporter_task
            except asyncio.CancelledError:
                pass
        
        # Cleanup
        self.thread_pool.shutdown(wait=True)
        await self.historical_fetcher.close()
        
        # Final stats
        runtime = (datetime.now() - self.start_time).total_seconds()
        logger.info(f"📊 Final Stats: {self.total_trades:,} trades, {self.total_signals} signals in {runtime/60:.1f} minutes")
        logger.info("🔚 Multi-Ticker Runner stopped")

def get_tickers_from_user() -> List[str]:
    """Get tickers from watchlist and user input"""
    # Load base watchlist
    base = load_watchlist()
    
    # Get user input
    print("\n" + "="*50)
    print("TICKER CONFIGURATION")
    print("="*50)
    print(f"Watchlist has {len(base)} tickers: {', '.join(base[:10])}{'...' if len(base) > 10 else ''}")
    
    user_input = input("\nEnter additional tickers (comma-separated) or press Enter to use watchlist: ").strip()
    
    # Parse manual input
    manual = []
    if user_input:
        manual = [
            t.strip().upper() 
            for t in user_input.split(",") 
            if t.strip() and len(t.strip()) <= 5 and t.strip().isalnum()
        ]
    
    # Combine and deduplicate
    all_tickers = list(set(manual + base))
    
    # Validate tickers
    valid_tickers = [
        t for t in all_tickers 
        if len(t) <= 5 and t.isalnum()
    ]
    
    if not valid_tickers:
        logger.warning("No valid tickers provided, using default watchlist")
        return base[:10]  # Default to first 10 from watchlist
    
    print(f"\n✅ Tracking {len(valid_tickers)} tickers")
    return valid_tickers

async def main():
    """Main entry point"""
    runner = None
    
    try:
        # Get configuration
        config = {
            'verbose': False,  # Set to True for detailed logging
            'rabbitmq_enabled': True  # Set to False to disable RabbitMQ
        }
        
        # Get tickers
        tickers = get_tickers_from_user()
        
        # Create and start runner
        runner = MultiTickerRunner(config)
        
        # Setup signal handlers for graceful shutdown
        def signal_handler(sig, frame):
            logger.info(f"Received signal {sig}")
            if runner:
                asyncio.create_task(runner.stop())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start the runner
        await runner.start(tickers)
        
    except KeyboardInterrupt:
        logger.info("🛑 Process interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        if runner and not runner.shutting_down:
            await runner.stop()

if __name__ == "__main__":
    asyncio.run(main())