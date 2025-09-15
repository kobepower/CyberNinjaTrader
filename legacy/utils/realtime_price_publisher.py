import asyncio
import aio_pika
import json
import logging
import os
import time
from typing import Optional, Dict, List, Set, Any
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from collections import defaultdict
import finnhub
import yfinance as yf
from enum import Enum

# Import our enhanced modules
from price_utils import PriceUtils, PriceQuote
from realtime_data import RealtimeDataClient, Trade, Quote
from rabbitmq_price_publisher import PricePublisher, PriceUpdate, PriceType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
FINNHUB_API_KEY = os.environ.get('FINNHUB_API_KEY', 'your_api_key_here')
RABBITMQ_URL = os.environ.get('RABBITMQ_URL', 'amqp://guest:guest@localhost/')
PRICE_QUEUE = 'price_queue'
POLLING_INTERVAL = 5  # seconds for fallback polling
BATCH_SIZE = 50
ENABLE_WEBSOCKET = True
ENABLE_POLLING = True

class PublishMode(Enum):
    """Publishing modes"""
    WEBSOCKET = "websocket"  # Real-time via WebSocket
    POLLING = "polling"      # Periodic polling
    HYBRID = "hybrid"        # WebSocket with polling fallback

@dataclass
class PriceConfig:
    """Configuration for price publishing"""
    tickers: List[str]
    mode: PublishMode = PublishMode.HYBRID
    polling_interval: int = POLLING_INTERVAL
    enable_quotes: bool = True
    enable_volume: bool = True
    batch_publishing: bool = True
    dedup_window: int = 1  # seconds
    finnhub_api_key: Optional[str] = None
    rabbitmq_url: str = RABBITMQ_URL

class RealtimePricePublisher:
    """Enhanced real-time price publisher with WebSocket and polling support"""
    
    def __init__(self, config: PriceConfig):
        self.config = config
        
        # API clients
        self.finnhub_api_key = config.finnhub_api_key or FINNHUB_API_KEY
        self.price_utils = PriceUtils(finnhub_api_key=self.finnhub_api_key)
        self.websocket_client: Optional[RealtimeDataClient] = None
        
        # RabbitMQ publisher
        self.publisher: Optional[PricePublisher] = None
        
        # State tracking
        self.active_tickers: Set[str] = set(config.tickers)
        self.last_prices: Dict[str, float] = {}
        self.last_publish_time: Dict[str, float] = {}
        self.websocket_active: Dict[str, bool] = defaultdict(bool)
        
        # Tasks
        self.tasks: List[asyncio.Task] = []
        self.running = False
        
        # Statistics
        self.stats = {
            'prices_published': 0,
            'websocket_updates': 0,
            'polling_updates': 0,
            'errors': 0,
            'duplicates_filtered': 0,
            'start_time': None
        }
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()
    
    async def start(self):
        """Start the price publisher"""
        logger.info(f"🚀 Starting Real-time Price Publisher in {self.config.mode.value} mode")
        self.running = True
        self.stats['start_time'] = datetime.now()
        
        # Initialize RabbitMQ publisher
        self.publisher = PricePublisher(
            amqp_url=self.config.rabbitmq_url,
            queue_name=PRICE_QUEUE,
            batch_size=BATCH_SIZE if self.config.batch_publishing else 1
        )
        await self.publisher.connect()
        
        # Start appropriate mode
        if self.config.mode in [PublishMode.WEBSOCKET, PublishMode.HYBRID]:
            await self._start_websocket_mode()
        
        if self.config.mode in [PublishMode.POLLING, PublishMode.HYBRID]:
            await self._start_polling_mode()
        
        logger.info(f"✅ Price publisher started for {len(self.active_tickers)} tickers")
    
    async def stop(self):
        """Stop the price publisher"""
        logger.info("🛑 Stopping Real-time Price Publisher...")
        self.running = False
        
        # Cancel all tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Close connections
        if self.websocket_client:
            await self.websocket_client.disconnect()
        
        if self.publisher:
            await self.publisher.close()
        
        # Log final statistics
        self._log_statistics()
        logger.info("🔚 Price publisher stopped")
    
    async def _start_websocket_mode(self):
        """Start WebSocket streaming"""
        self.websocket_client = RealtimeDataClient(
            api_key=self.finnhub_api_key,
            enable_quotes=self.config.enable_quotes
        )
        
        await self.websocket_client.connect()
        
        # Subscribe to all tickers
        for ticker in self.active_tickers:
            # Trade handler
            await self.websocket_client.subscribe_to_trades(
                ticker,
                self._create_trade_handler(ticker)
            )
            
            # Quote handler if enabled
            if self.config.enable_quotes:
                await self.websocket_client.subscribe_to_quotes(
                    ticker,
                    self._create_quote_handler(ticker)
                )
            
            self.websocket_active[ticker] = True
            logger.info(f"📊 Subscribed to WebSocket feed for {ticker}")
        
        # Start WebSocket monitor
        self.tasks.append(
            asyncio.create_task(self._websocket_monitor())
        )
    
    async def _start_polling_mode(self):
        """Start polling mode"""
        # Create polling tasks for each ticker
        for ticker in self.active_tickers:
            if self.config.mode == PublishMode.HYBRID:
                # In hybrid mode, only poll if WebSocket fails
                self.tasks.append(
                    asyncio.create_task(self._hybrid_poller(ticker))
                )
            else:
                # Pure polling mode
                self.tasks.append(
                    asyncio.create_task(self._ticker_poller(ticker))
                )
        
        logger.info(f"📈 Started polling for {len(self.active_tickers)} tickers")
    
    def _create_trade_handler(self, ticker: str):
        """Create a trade handler for a specific ticker"""
        async def handler(trade: Trade):
            try:
                # Check for duplicate
                if self._is_duplicate(ticker, trade.price):
                    self.stats['duplicates_filtered'] += 1
                    return
                
                # Create price update
                price_update = PriceUpdate(
                    ticker=ticker,
                    price=trade.price,
                    price_type=PriceType.TRADE,
                    volume=trade.volume if self.config.enable_volume else None,
                    timestamp=trade.datetime,
                    source="websocket"
                )
                
                # Publish
                await self._publish_price(price_update)
                self.stats['websocket_updates'] += 1
                
                # Mark WebSocket as active
                self.websocket_active[ticker] = True
                
            except Exception as e:
                logger.error(f"Error handling trade for {ticker}: {e}")
                self.stats['errors'] += 1
        
        return handler
    
    def _create_quote_handler(self, ticker: str):
        """Create a quote handler for a specific ticker"""
        async def handler(quote: Quote):
            try:
                # Create price update with bid/ask
                price_update = PriceUpdate(
                    ticker=ticker,
                    price=quote.mid_price,
                    price_type=PriceType.QUOTE,
                    bid=quote.bid,
                    ask=quote.ask,
                    bid_size=quote.bid_size,
                    ask_size=quote.ask_size,
                    timestamp=quote.datetime,
                    source="websocket"
                )
                
                # Publish if not duplicate
                if not self._is_duplicate(ticker, quote.mid_price):
                    await self._publish_price(price_update)
                    self.stats['websocket_updates'] += 1
                
            except Exception as e:
                logger.error(f"Error handling quote for {ticker}: {e}")
                self.stats['errors'] += 1
        
        return handler
    
    async def _ticker_poller(self, ticker: str):
        """Poll prices for a specific ticker"""
        while self.running:
            try:
                # Fetch price
                quote = await self.price_utils.fetch_price(ticker, use_cache=False)
                
                # Check for duplicate
                if not self._is_duplicate(ticker, quote.price):
                    # Create price update
                    price_update = PriceUpdate(
                        ticker=ticker,
                        price=quote.price,
                        price_type=PriceType.LAST,
                        volume=quote.volume,
                        bid=quote.bid,
                        ask=quote.ask,
                        high=quote.high,
                        low=quote.low,
                        open=quote.open,
                        timestamp=quote.timestamp,
                        source="polling"
                    )
                    
                    # Publish
                    await self._publish_price(price_update)
                    self.stats['polling_updates'] += 1
                
                # Wait for next poll
                await asyncio.sleep(self.config.polling_interval)
                
            except Exception as e:
                logger.error(f"Polling error for {ticker}: {e}")
                self.stats['errors'] += 1
                await asyncio.sleep(self.config.polling_interval)
    
    async def _hybrid_poller(self, ticker: str):
        """Hybrid poller - only polls when WebSocket is inactive"""
        last_websocket_check = time.time()
        
        while self.running:
            try:
                # Check if WebSocket is active
                if self.websocket_active.get(ticker, False):
                    # WebSocket is working, reset flag and wait
                    if time.time() - last_websocket_check > 30:
                        self.websocket_active[ticker] = False
                        last_websocket_check = time.time()
                    
                    await asyncio.sleep(5)
                    continue
                
                # WebSocket inactive, fall back to polling
                logger.debug(f"WebSocket inactive for {ticker}, polling...")
                
                # Fetch price
                quote = await self.price_utils.fetch_price(ticker, use_cache=False)
                
                # Check for duplicate
                if not self._is_duplicate(ticker, quote.price):
                    # Create price update
                    price_update = PriceUpdate(
                        ticker=ticker,
                        price=quote.price,
                        price_type=PriceType.LAST,
                        volume=quote.volume,
                        bid=quote.bid,
                        ask=quote.ask,
                        timestamp=quote.timestamp,
                        source="polling_fallback"
                    )
                    
                    # Publish
                    await self._publish_price(price_update)
                    self.stats['polling_updates'] += 1
                
                # Wait before next check
                await asyncio.sleep(self.config.polling_interval)
                
            except Exception as e:
                logger.error(f"Hybrid polling error for {ticker}: {e}")
                self.stats['errors'] += 1
                await asyncio.sleep(self.config.polling_interval)
    
    async def _websocket_monitor(self):
        """Monitor WebSocket health"""
        while self.running:
            try:
                await asyncio.sleep(30)
                
                if self.websocket_client:
                    stats = self.websocket_client.get_statistics()
                    
                    # Check for stale connections
                    if stats.get('last_message_time'):
                        time_since_last = (datetime.now() - stats['last_message_time']).seconds
                        if time_since_last > 60:
                            logger.warning(f"WebSocket appears stale ({time_since_last}s since last message)")
                            
                            # Mark all tickers as inactive for hybrid fallback
                            for ticker in self.active_tickers:
                                self.websocket_active[ticker] = False
                
            except Exception as e:
                logger.error(f"WebSocket monitor error: {e}")
    
    async def _publish_price(self, price_update: PriceUpdate):
        """Publish price update to RabbitMQ"""
        try:
            success = await self.publisher.publish_update(price_update)
            
            if success:
                self.stats['prices_published'] += 1
                self.last_prices[price_update.ticker] = price_update.price
                self.last_publish_time[price_update.ticker] = time.time()
                
                # Log significant price changes
                if price_update.ticker in self.last_prices:
                    change = abs(price_update.price - self.last_prices[price_update.ticker])
                    change_pct = (change / self.last_prices[price_update.ticker]) * 100
                    
                    if change_pct > 1.0:  # 1% change
                        logger.info(
                            f"📈 Significant move: {price_update.ticker} "
                            f"${price_update.price:.2f} ({change_pct:+.2f}%)"
                        )
            
        except Exception as e:
            logger.error(f"Failed to publish price for {price_update.ticker}: {e}")
            self.stats['errors'] += 1
    
    def _is_duplicate(self, ticker: str, price: float) -> bool:
        """Check if price is a duplicate within dedup window"""
        # Check if price changed
        if ticker in self.last_prices:
            if self.last_prices[ticker] == price:
                # Check time window
                if ticker in self.last_publish_time:
                    time_since_last = time.time() - self.last_publish_time[ticker]
                    if time_since_last < self.config.dedup_window:
                        return True
        
        return False
    
    def _log_statistics(self):
        """Log statistics"""
        if self.stats['start_time']:
            runtime = (datetime.now() - self.stats['start_time']).total_seconds()
            rate = self.stats['prices_published'] / runtime if runtime > 0 else 0
            
            logger.info("📊 Final Statistics:")
            logger.info(f"  Runtime: {runtime:.0f}s")
            logger.info(f"  Prices published: {self.stats['prices_published']:,}")
            logger.info(f"  WebSocket updates: {self.stats['websocket_updates']:,}")
            logger.info(f"  Polling updates: {self.stats['polling_updates']:,}")
            logger.info(f"  Duplicates filtered: {self.stats['duplicates_filtered']:,}")
            logger.info(f"  Errors: {self.stats['errors']}")
            logger.info(f"  Rate: {rate:.2f} prices/second")
    
    async def add_ticker(self, ticker: str):
        """Dynamically add a ticker"""
        ticker = ticker.upper()
        
        if ticker in self.active_tickers:
            logger.warning(f"Ticker {ticker} already active")
            return
        
        self.active_tickers.add(ticker)
        
        # Subscribe via WebSocket if available
        if self.websocket_client:
            await self.websocket_client.subscribe_to_trades(
                ticker,
                self._create_trade_handler(ticker)
            )
            
            if self.config.enable_quotes:
                await self.websocket_client.subscribe_to_quotes(
                    ticker,
                    self._create_quote_handler(ticker)
                )
        
        # Start polling if needed
        if self.config.mode in [PublishMode.POLLING, PublishMode.HYBRID]:
            if self.config.mode == PublishMode.HYBRID:
                self.tasks.append(
                    asyncio.create_task(self._hybrid_poller(ticker))
                )
            else:
                self.tasks.append(
                    asyncio.create_task(self._ticker_poller(ticker))
                )
        
        logger.info(f"➕ Added ticker: {ticker}")
    
    async def remove_ticker(self, ticker: str):
        """Dynamically remove a ticker"""
        ticker = ticker.upper()
        
        if ticker not in self.active_tickers:
            logger.warning(f"Ticker {ticker} not active")
            return
        
        self.active_tickers.remove(ticker)
        
        # Unsubscribe from WebSocket
        if self.websocket_client:
            await self.websocket_client.unsubscribe(ticker)
        
        # Remove from tracking
        self.websocket_active.pop(ticker, None)
        self.last_prices.pop(ticker, None)
        self.last_publish_time.pop(ticker, None)
        
        logger.info(f"➖ Removed ticker: {ticker}")

# Legacy compatibility function
def publish_price(ticker: str):
    """
    Legacy synchronous function for backward compatibility
    """
    import pika
    
    # Get price using enhanced price utils
    price_utils = PriceUtils(finnhub_api_key=FINNHUB_API_KEY)
    
    try:
        # Fetch price synchronously
        quote = price_utils.fetch_price_sync(ticker)
        
        message = {
            "ticker": ticker,
            "price": quote.price,
            "timestamp": quote.timestamp.isoformat()
        }
        
        # Publish to RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        channel = connection.channel()
        channel.queue_declare(queue=PRICE_QUEUE, durable=True)
        
        channel.basic_publish(
            exchange='',
            routing_key=PRICE_QUEUE,
            body=json.dumps(message).encode(),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        
        logger.info(f"✅ Published: {message}")
        connection.close()
        
    except Exception as e:
        logger.error(f"❌ Failed to publish {ticker}: {e}")

# Main execution
async def main():
    """Main execution function"""
    # Configuration
    config = PriceConfig(
        tickers=["AAPL", "MSFT", "GOOGL", "NVDA", "TSLA"],
        mode=PublishMode.HYBRID,  # WebSocket with polling fallback
        polling_interval=5,
        enable_quotes=True,
        enable_volume=True,
        batch_publishing=True,
        dedup_window=1
    )
    
    # Run publisher
    async with RealtimePricePublisher(config) as publisher:
        # Run for a while
        logger.info("Publisher running. Press Ctrl+C to stop...")
        
        try:
            # Dynamically add/remove tickers after 30 seconds
            await asyncio.sleep(30)
            await publisher.add_ticker("AMD")
            
            # Keep running
            while True:
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Shutdown requested...")

if __name__ == "__main__":
    # For async operation (recommended)
    asyncio.run(main())
    
    # For legacy synchronous operation
    # while True:
    #     for ticker in ["AAPL", "MSFT"]:
    #         publish_price(ticker)
    #     time.sleep(5)