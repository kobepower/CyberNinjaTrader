import asyncio
import aio_pika
import json
import logging
import time
from typing import Optional, Dict, List, Any, Union
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from collections import deque
from enum import Enum
import pika  # For backward compatibility
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_BATCH_SIZE = 100
DEFAULT_BATCH_TIMEOUT = 1.0  # seconds
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 0.5
DEFAULT_QUEUE_NAME = "price_queue"

class PriceType(Enum):
    """Types of price updates"""
    TRADE = "trade"
    QUOTE = "quote"
    BID = "bid"
    ASK = "ask"
    MARK = "mark"
    LAST = "last"

@dataclass
class PriceUpdate:
    """Complete price update data structure"""
    ticker: str
    price: float
    price_type: PriceType = PriceType.LAST
    volume: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    bid_size: Optional[int] = None
    ask_size: Optional[int] = None
    high: Optional[float] = None
    low: Optional[float] = None
    open: Optional[float] = None
    prev_close: Optional[float] = None
    timestamp: Optional[datetime] = None
    source: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
    
    @property
    def spread(self) -> Optional[float]:
        """Calculate bid-ask spread"""
        if self.bid and self.ask:
            return self.ask - self.bid
        return None
    
    @property
    def mid_price(self) -> Optional[float]:
        """Calculate mid price"""
        if self.bid and self.ask:
            return (self.bid + self.ask) / 2
        return self.price
    
    @property
    def age_seconds(self) -> float:
        """Get age of price in seconds"""
        return (datetime.now() - self.timestamp).total_seconds()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        data = {
            'ticker': self.ticker,
            'price': self.price,
            'price_type': self.price_type.value,
            'timestamp': self.timestamp.isoformat()
        }
        
        # Add optional fields if present
        optional_fields = [
            'volume', 'bid', 'ask', 'bid_size', 'ask_size',
            'high', 'low', 'open', 'prev_close', 'source'
        ]
        for field in optional_fields:
            value = getattr(self, field)
            if value is not None:
                data[field] = value
        
        if self.metadata:
            data['metadata'] = self.metadata
        
        # Add calculated fields
        if self.spread is not None:
            data['spread'] = self.spread
        if self.mid_price != self.price:
            data['mid_price'] = self.mid_price
        
        return data
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PriceUpdate':
        """Create from dictionary"""
        # Handle timestamp
        if 'timestamp' in data:
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        
        # Handle price_type enum
        if 'price_type' in data:
            data['price_type'] = PriceType(data['price_type'])
        
        return cls(**data)

class PricePublisher:
    """Enhanced async price publisher with batching and connection pooling"""
    
    def __init__(
        self,
        amqp_url: str = "amqp://guest:guest@localhost/",
        queue_name: str = DEFAULT_QUEUE_NAME,
        batch_size: int = DEFAULT_BATCH_SIZE,
        batch_timeout: float = DEFAULT_BATCH_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES
    ):
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.max_retries = max_retries
        
        # Connection management
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.queue: Optional[aio_pika.Queue] = None
        self.connection_lock = asyncio.Lock()
        
        # Batching
        self.batch_queue: deque = deque()
        self.batch_task: Optional[asyncio.Task] = None
        self.batch_lock = asyncio.Lock()
        
        # Statistics
        self.stats = {
            'messages_sent': 0,
            'batches_sent': 0,
            'errors': 0,
            'retries': 0,
            'connection_failures': 0
        }
        
        # For sync compatibility
        self.thread_pool = ThreadPoolExecutor(max_workers=1)
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
    
    async def connect(self):
        """Establish connection to RabbitMQ"""
        async with self.connection_lock:
            if self.connection and not self.connection.is_closed:
                return
            
            try:
                self.connection = await aio_pika.connect_robust(
                    self.amqp_url,
                    reconnect_interval=5
                )
                self.channel = await self.connection.channel()
                self.queue = await self.channel.declare_queue(
                    self.queue_name,
                    durable=True
                )
                
                # Start batch processor
                if not self.batch_task or self.batch_task.done():
                    self.batch_task = asyncio.create_task(self._batch_processor())
                
                logger.info(f"✅ Connected to RabbitMQ, queue: {self.queue_name}")
                
            except Exception as e:
                self.stats['connection_failures'] += 1
                logger.error(f"❌ Failed to connect to RabbitMQ: {e}")
                raise
    
    async def close(self):
        """Close connection and flush pending messages"""
        try:
            # Flush pending messages
            await self._flush_batch()
            
            # Cancel batch processor
            if self.batch_task and not self.batch_task.done():
                self.batch_task.cancel()
                try:
                    await self.batch_task
                except asyncio.CancelledError:
                    pass
            
            # Close connection
            if self.channel:
                await self.channel.close()
            if self.connection:
                await self.connection.close()
            
            logger.info("📴 Closed RabbitMQ connection")
            
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
    
    async def publish_price(
        self,
        ticker: str,
        price: float,
        **kwargs
    ) -> bool:
        """
        Publish a single price update
        
        Args:
            ticker: Stock symbol
            price: Current price
            **kwargs: Additional fields (volume, bid, ask, etc.)
            
        Returns:
            True on success, False on failure
        """
        price_update = PriceUpdate(ticker=ticker, price=price, **kwargs)
        return await self.publish_update(price_update)
    
    async def publish_update(self, update: PriceUpdate) -> bool:
        """
        Publish a PriceUpdate object
        
        Args:
            update: PriceUpdate object
            
        Returns:
            True on success, False on failure
        """
        try:
            await self.connect()
            
            # Add to batch queue
            async with self.batch_lock:
                self.batch_queue.append(update)
                
                # Check if batch is full
                if len(self.batch_queue) >= self.batch_size:
                    await self._flush_batch()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to queue price update: {e}")
            self.stats['errors'] += 1
            return False
    
    async def publish_batch(self, updates: List[PriceUpdate]) -> bool:
        """
        Publish multiple price updates at once
        
        Args:
            updates: List of PriceUpdate objects
            
        Returns:
            True on success, False on failure
        """
        try:
            await self.connect()
            
            # Add all to batch queue
            async with self.batch_lock:
                self.batch_queue.extend(updates)
                
                # Flush if batch is large
                if len(self.batch_queue) >= self.batch_size:
                    await self._flush_batch()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to queue batch: {e}")
            self.stats['errors'] += 1
            return False
    
    async def _batch_processor(self):
        """Background task to process batches periodically"""
        while True:
            try:
                await asyncio.sleep(self.batch_timeout)
                
                if self.batch_queue:
                    await self._flush_batch()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Batch processor error: {e}")
                self.stats['errors'] += 1
    
    async def _flush_batch(self):
        """Flush pending messages to RabbitMQ"""
        async with self.batch_lock:
            if not self.batch_queue:
                return
            
            # Get all pending messages
            messages = list(self.batch_queue)
            self.batch_queue.clear()
        
        # Retry logic
        for attempt in range(self.max_retries):
            try:
                await self.connect()
                
                # Publish all messages
                for update in messages:
                    message = aio_pika.Message(
                        body=update.to_json().encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        timestamp=datetime.now(),
                        content_type="application/json",
                        headers={
                            'ticker': update.ticker,
                            'price_type': update.price_type.value,
                            'source': update.source or 'unknown'
                        }
                    )
                    
                    await self.channel.default_exchange.publish(
                        message,
                        routing_key=self.queue_name
                    )
                    
                    self.stats['messages_sent'] += 1
                
                self.stats['batches_sent'] += 1
                
                logger.info(
                    f"📡 Published batch of {len(messages)} price updates "
                    f"({messages[0].ticker} @ ${messages[0].price:.2f}, ...)"
                )
                return
                
            except Exception as e:
                self.stats['retries'] += 1
                if attempt < self.max_retries - 1:
                    delay = DEFAULT_RETRY_DELAY * (2 ** attempt)
                    logger.warning(
                        f"Batch publish failed (attempt {attempt + 1}/{self.max_retries}): {e}. "
                        f"Retrying in {delay}s..."
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Failed to publish batch after {self.max_retries} attempts")
                    self.stats['errors'] += 1
                    # Put messages back in queue for next attempt
                    async with self.batch_lock:
                        self.batch_queue.extendleft(reversed(messages))
    
    async def publish_market_snapshot(
        self,
        snapshot: Dict[str, float]
    ) -> bool:
        """
        Publish a market snapshot (multiple tickers at once)
        
        Args:
            snapshot: Dictionary of ticker -> price
            
        Returns:
            True on success, False on failure
        """
        updates = [
            PriceUpdate(ticker=ticker, price=price, price_type=PriceType.MARK)
            for ticker, price in snapshot.items()
        ]
        return await self.publish_batch(updates)
    
    def get_stats(self) -> Dict[str, int]:
        """Get publisher statistics"""
        return self.stats.copy()
    
    def reset_stats(self):
        """Reset statistics"""
        self.stats = {
            'messages_sent': 0,
            'batches_sent': 0,
            'errors': 0,
            'retries': 0,
            'connection_failures': 0
        }
        logger.info("📊 Statistics reset")

class SyncPricePublisher:
    """Synchronous wrapper for backward compatibility"""
    
    def __init__(self, host: str = 'localhost', queue_name: str = DEFAULT_QUEUE_NAME):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
    
    def connect(self):
        """Connect to RabbitMQ"""
        if not self.connection or self.connection.is_closed:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.host)
            )
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
    
    def publish_price(self, ticker: str, price: float) -> bool:
        """
        Publish a single price (sync version)
        
        Args:
            ticker: Stock symbol
            price: Current price
            
        Returns:
            True on success, False on failure
        """
        message = {
            "ticker": ticker,
            "price": price,
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            self.connect()
            
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=json.dumps(message).encode(),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Persistent
                    timestamp=int(time.time())
                )
            )
            
            logger.info(f"✅ Published: {ticker} @ ${price:.2f}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to publish {ticker}: {e}")
            return False
    
    def close(self):
        """Close connection"""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("📴 Closed RabbitMQ connection")
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

# Backward compatibility function
def publish_price(ticker: str, price: float) -> bool:
    """
    Legacy function for backward compatibility
    
    Args:
        ticker: Stock symbol
        price: Current price
        
    Returns:
        True on success, False on failure
    """
    publisher = SyncPricePublisher()
    try:
        return publisher.publish_price(ticker, price)
    finally:
        publisher.close()

# Global async publisher instance
_async_publisher: Optional[PricePublisher] = None

async def get_async_publisher() -> PricePublisher:
    """Get or create global async publisher"""
    global _async_publisher
    if _async_publisher is None:
        _async_publisher = PricePublisher()
        await _async_publisher.connect()
    return _async_publisher

# Example usage and testing
if __name__ == "__main__":
    async def test_async_publisher():
        """Test async publisher"""
        print("\n=== Testing Async Publisher ===")
        
        async with PricePublisher() as publisher:
            # Single price update
            await publisher.publish_price(
                "AAPL",
                175.50,
                volume=1000000,
                bid=175.48,
                ask=175.52
            )
            
            # Multiple updates
            updates = [
                PriceUpdate(
                    ticker="MSFT",
                    price=405.25,
                    volume=500000,
                    price_type=PriceType.TRADE
                ),
                PriceUpdate(
                    ticker="GOOGL",
                    price=142.30,
                    volume=750000,
                    bid=142.28,
                    ask=142.32
                ),
                PriceUpdate(
                    ticker="NVDA",
                    price=850.00,
                    volume=2000000,
                    high=855.00,
                    low=845.00
                )
            ]
            await publisher.publish_batch(updates)
            
            # Market snapshot
            snapshot = {
                "SPY": 450.25,
                "QQQ": 380.50,
                "IWM": 195.75
            }
            await publisher.publish_market_snapshot(snapshot)
            
            # Wait for batch to flush
            await asyncio.sleep(2)
            
            # Show statistics
            print("\n📊 Statistics:")
            stats = publisher.get_stats()
            for key, value in stats.items():
                print(f"  {key}: {value}")
    
    def test_sync_publisher():
        """Test sync publisher (backward compatible)"""
        print("\n=== Testing Sync Publisher ===")
        
        # Using context manager
        with SyncPricePublisher() as publisher:
            publisher.publish_price("AAPL", 175.50)
            publisher.publish_price("MSFT", 405.25)
        
        # Legacy function
        print("\n=== Testing Legacy Function ===")
        publish_price("TSLA", 250.00)
        publish_price("AMD", 150.00)
    
    # Run tests
    print("Testing RabbitMQ Price Publisher")
    
    # Test async version
    asyncio.run(test_async_publisher())
    
    # Test sync version
    test_sync_publisher()
    
    print("\n✅ All tests completed")
    input("\nPress Enter to exit...")