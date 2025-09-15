import asyncio
import aio_pika
import pika
import json
import logging
import hashlib
import time
from typing import Optional, Dict, Any, List, Set
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum, IntEnum
from collections import deque
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_SIGNAL_QUEUE = "signal_queue"
HIGH_PRIORITY_QUEUE = "high_priority_signals"
DEAD_LETTER_QUEUE = "dead_letter_signals"
DEFAULT_TTL = 3600  # 1 hour
DEFAULT_MAX_RETRIES = 3
DEFAULT_BATCH_SIZE = 50
DEFAULT_BATCH_TIMEOUT = 0.5
DEDUP_WINDOW = 60  # seconds

class SignalType(Enum):
    """Types of trading signals"""
    BOTTOM = "bottom"
    TOP = "top"
    REVERSAL = "reversal"
    BREAKOUT = "breakout"
    BREAKDOWN = "breakdown"
    VOLUME_SPIKE = "volume_spike"
    MOMENTUM = "momentum"
    MEAN_REVERSION = "mean_reversion"
    ALERT = "alert"

class SignalAction(Enum):
    """Trading actions"""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"
    CLOSE = "close"
    SCALE_IN = "scale_in"
    SCALE_OUT = "scale_out"
    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"

class SignalPriority(IntEnum):
    """Signal priority levels"""
    CRITICAL = 1
    HIGH = 2
    MEDIUM = 3
    LOW = 4

class SignalStatus(Enum):
    """Signal processing status"""
    PENDING = "pending"
    SENT = "sent"
    ACKNOWLEDGED = "acknowledged"
    PROCESSED = "processed"
    FAILED = "failed"
    EXPIRED = "expired"

@dataclass
class TradingSignal:
    """Complete trading signal data structure"""
    # Required fields
    action: SignalAction
    ticker: str
    price: float
    
    # Optional fields with defaults
    signal_type: SignalType = SignalType.ALERT
    volume: Optional[float] = None
    confidence: float = 0.0
    priority: SignalPriority = SignalPriority.MEDIUM
    
    # Entry/Exit parameters
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    position_size: Optional[int] = None
    
    # Risk metrics
    risk_reward_ratio: Optional[float] = None
    max_loss: Optional[float] = None
    expected_return: Optional[float] = None
    
    # Technical indicators
    rsi: Optional[float] = None
    macd: Optional[float] = None
    support_level: Optional[float] = None
    resistance_level: Optional[float] = None
    
    # Metadata
    signal_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    expiry: Optional[datetime] = None
    source: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Post-initialization processing"""
        # Generate signal ID if not provided
        if self.signal_id is None:
            self.signal_id = str(uuid.uuid4())
        
        # Set timestamp if not provided
        if self.timestamp is None:
            self.timestamp = datetime.now()
        
        # Set expiry if not provided (1 hour default)
        if self.expiry is None:
            self.expiry = self.timestamp + timedelta(seconds=DEFAULT_TTL)
        
        # Initialize metadata if not provided
        if self.metadata is None:
            self.metadata = {}
        
        # Validate signal
        self.validate()
    
    def validate(self):
        """Validate signal data"""
        # Price validation
        if self.price <= 0:
            raise ValueError(f"Invalid price: {self.price}")
        
        # Ticker validation
        if not self.ticker or len(self.ticker) > 10:
            raise ValueError(f"Invalid ticker: {self.ticker}")
        
        # Stop loss validation
        if self.stop_loss:
            if self.action in [SignalAction.BUY, SignalAction.SCALE_IN]:
                if self.stop_loss >= self.price:
                    raise ValueError(f"Stop loss {self.stop_loss} must be below entry {self.price}")
            elif self.action in [SignalAction.SELL, SignalAction.SCALE_OUT]:
                if self.stop_loss <= self.price:
                    raise ValueError(f"Stop loss {self.stop_loss} must be above entry {self.price}")
        
        # Take profit validation
        if self.take_profit:
            if self.action in [SignalAction.BUY, SignalAction.SCALE_IN]:
                if self.take_profit <= self.price:
                    raise ValueError(f"Take profit {self.take_profit} must be above entry {self.price}")
            elif self.action in [SignalAction.SELL, SignalAction.SCALE_OUT]:
                if self.take_profit >= self.price:
                    raise ValueError(f"Take profit {self.take_profit} must be below entry {self.price}")
        
        # Confidence validation
        if not 0 <= self.confidence <= 100:
            raise ValueError(f"Confidence must be between 0 and 100: {self.confidence}")
    
    @property
    def is_expired(self) -> bool:
        """Check if signal has expired"""
        return datetime.now() > self.expiry
    
    @property
    def age_seconds(self) -> float:
        """Get age of signal in seconds"""
        return (datetime.now() - self.timestamp).total_seconds()
    
    @property
    def ttl_remaining(self) -> float:
        """Get remaining time to live in seconds"""
        return max(0, (self.expiry - datetime.now()).total_seconds())
    
    def calculate_risk_reward(self):
        """Calculate risk-reward ratio"""
        if self.stop_loss and self.take_profit:
            risk = abs(self.price - self.stop_loss)
            reward = abs(self.take_profit - self.price)
            if risk > 0:
                self.risk_reward_ratio = reward / risk
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        data = {
            'signal_id': self.signal_id,
            'action': self.action.value,
            'ticker': self.ticker,
            'price': self.price,
            'signal_type': self.signal_type.value,
            'confidence': self.confidence,
            'priority': self.priority.value,
            'timestamp': self.timestamp.isoformat(),
            'expiry': self.expiry.isoformat(),
            'ttl_remaining': self.ttl_remaining,
            'age_seconds': self.age_seconds
        }
        
        # Add optional fields
        optional_fields = [
            'volume', 'entry_price', 'stop_loss', 'take_profit',
            'position_size', 'risk_reward_ratio', 'max_loss', 'expected_return',
            'rsi', 'macd', 'support_level', 'resistance_level', 'source'
        ]
        
        for field in optional_fields:
            value = getattr(self, field)
            if value is not None:
                data[field] = value
        
        if self.metadata:
            data['metadata'] = self.metadata
        
        return data
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TradingSignal':
        """Create from dictionary"""
        # Handle enums
        if 'action' in data:
            data['action'] = SignalAction(data['action'])
        if 'signal_type' in data:
            data['signal_type'] = SignalType(data['signal_type'])
        if 'priority' in data:
            data['priority'] = SignalPriority(data['priority'])
        
        # Handle timestamps
        if 'timestamp' in data:
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        if 'expiry' in data:
            data['expiry'] = datetime.fromisoformat(data['expiry'])
        
        # Remove calculated fields
        data.pop('ttl_remaining', None)
        data.pop('age_seconds', None)
        
        return cls(**data)
    
    def get_hash(self) -> str:
        """Generate hash for deduplication"""
        key = f"{self.ticker}:{self.action.value}:{self.price}:{self.signal_type.value}"
        return hashlib.md5(key.encode()).hexdigest()

class SignalProducer:
    """Enhanced async signal producer with validation and priority queues"""
    
    def __init__(
        self,
        amqp_url: str = "amqp://guest:guest@localhost/",
        queue_name: str = DEFAULT_SIGNAL_QUEUE,
        enable_priority: bool = True,
        enable_dedup: bool = True,
        batch_size: int = DEFAULT_BATCH_SIZE,
        batch_timeout: float = DEFAULT_BATCH_TIMEOUT
    ):
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.enable_priority = enable_priority
        self.enable_dedup = enable_dedup
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        
        # Connection management
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.queues: Dict[str, aio_pika.Queue] = {}
        self.connection_lock = asyncio.Lock()
        
        # Batching
        self.batch_queue: deque = deque()
        self.batch_task: Optional[asyncio.Task] = None
        self.batch_lock = asyncio.Lock()
        
        # Deduplication
        self.sent_signals: Set[str] = set()
        self.dedup_timestamps: Dict[str, float] = {}
        
        # Statistics
        self.stats = {
            'signals_sent': 0,
            'signals_deduplicated': 0,
            'signals_expired': 0,
            'batches_sent': 0,
            'errors': 0,
            'by_priority': {p.name: 0 for p in SignalPriority},
            'by_action': {a.name: 0 for a in SignalAction}
        }
    
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
                await self.channel.set_qos(prefetch_count=10)
                
                # Declare queues
                await self._declare_queues()
                
                # Start batch processor
                if not self.batch_task or self.batch_task.done():
                    self.batch_task = asyncio.create_task(self._batch_processor())
                
                logger.info("✅ Connected to RabbitMQ for signal publishing")
                
            except Exception as e:
                logger.error(f"❌ Failed to connect to RabbitMQ: {e}")
                raise
    
    async def _declare_queues(self):
        """Declare all required queues"""
        # Main signal queue
        self.queues[DEFAULT_SIGNAL_QUEUE] = await self.channel.declare_queue(
            self.queue_name,
            durable=True,
            arguments={
                'x-message-ttl': DEFAULT_TTL * 1000,  # milliseconds
                'x-dead-letter-exchange': '',
                'x-dead-letter-routing-key': DEAD_LETTER_QUEUE
            }
        )
        
        if self.enable_priority:
            # High priority queue
            self.queues[HIGH_PRIORITY_QUEUE] = await self.channel.declare_queue(
                HIGH_PRIORITY_QUEUE,
                durable=True,
                arguments={
                    'x-max-priority': 10,
                    'x-message-ttl': DEFAULT_TTL * 1000
                }
            )
        
        # Dead letter queue
        self.queues[DEAD_LETTER_QUEUE] = await self.channel.declare_queue(
            DEAD_LETTER_QUEUE,
            durable=True
        )
        
        logger.info(f"📦 Declared queues: {list(self.queues.keys())}")
    
    async def publish_signal(self, signal: TradingSignal) -> bool:
        """
        Publish a trading signal
        
        Args:
            signal: TradingSignal object
            
        Returns:
            True on success, False on failure
        """
        try:
            # Validate signal
            signal.validate()
            
            # Check if expired
            if signal.is_expired:
                logger.warning(f"Signal {signal.signal_id} has expired")
                self.stats['signals_expired'] += 1
                return False
            
            # Check for duplicates
            if self.enable_dedup:
                if not self._should_send_signal(signal):
                    logger.debug(f"Signal {signal.signal_id} deduplicated")
                    self.stats['signals_deduplicated'] += 1
                    return False
            
            await self.connect()
            
            # Add to batch or send immediately based on priority
            if signal.priority <= SignalPriority.HIGH:
                # Send high priority signals immediately
                await self._send_signal_immediate(signal)
            else:
                # Batch lower priority signals
                async with self.batch_lock:
                    self.batch_queue.append(signal)
                    if len(self.batch_queue) >= self.batch_size:
                        await self._flush_batch()
            
            return True
            
        except ValueError as e:
            logger.error(f"Signal validation failed: {e}")
            self.stats['errors'] += 1
            return False
        except Exception as e:
            logger.error(f"Failed to publish signal: {e}")
            self.stats['errors'] += 1
            return False
    
    async def _send_signal_immediate(self, signal: TradingSignal):
        """Send a signal immediately (for high priority)"""
        # Select queue based on priority
        if self.enable_priority and signal.priority <= SignalPriority.HIGH:
            queue_name = HIGH_PRIORITY_QUEUE
        else:
            queue_name = self.queue_name
        
        queue = self.queues.get(queue_name, self.queues[DEFAULT_SIGNAL_QUEUE])
        
        # Create message
        message = aio_pika.Message(
            body=signal.to_json().encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            priority=signal.priority.value if self.enable_priority else None,
            expiration=str(int(signal.ttl_remaining * 1000)),  # milliseconds
            timestamp=datetime.now(),
            message_id=signal.signal_id,
            content_type="application/json",
            headers={
                'signal_type': signal.signal_type.value,
                'ticker': signal.ticker,
                'action': signal.action.value,
                'priority': signal.priority.name,
                'source': signal.source or 'unknown'
            }
        )
        
        # Publish
        await self.channel.default_exchange.publish(
            message,
            routing_key=queue.name
        )
        
        # Update stats
        self.stats['signals_sent'] += 1
        self.stats['by_priority'][signal.priority.name] += 1
        self.stats['by_action'][signal.action.name] += 1
        
        # Track for deduplication
        if self.enable_dedup:
            self._mark_signal_sent(signal)
        
        logger.info(
            f"🚨 Published {signal.priority.name} priority {signal.signal_type.value} signal: "
            f"{signal.ticker} {signal.action.value} @ ${signal.price:.2f}"
        )
    
    async def _batch_processor(self):
        """Background task to process batches"""
        while True:
            try:
                await asyncio.sleep(self.batch_timeout)
                
                if self.batch_queue:
                    await self._flush_batch()
                
                # Clean old dedup entries
                if self.enable_dedup:
                    self._cleanup_dedup_cache()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Batch processor error: {e}")
                self.stats['errors'] += 1
    
    async def _flush_batch(self):
        """Flush pending signals"""
        async with self.batch_lock:
            if not self.batch_queue:
                return
            
            signals = list(self.batch_queue)
            self.batch_queue.clear()
        
        # Send all signals
        for signal in signals:
            try:
                await self._send_signal_immediate(signal)
            except Exception as e:
                logger.error(f"Failed to send signal {signal.signal_id}: {e}")
                # Put back in queue for retry
                async with self.batch_lock:
                    self.batch_queue.appendleft(signal)
        
        self.stats['batches_sent'] += 1
        logger.info(f"📦 Flushed batch of {len(signals)} signals")
    
    def _should_send_signal(self, signal: TradingSignal) -> bool:
        """Check if signal should be sent (deduplication)"""
        signal_hash = signal.get_hash()
        
        # Check if recently sent
        if signal_hash in self.sent_signals:
            last_sent = self.dedup_timestamps.get(signal_hash, 0)
            if time.time() - last_sent < DEDUP_WINDOW:
                return False
        
        return True
    
    def _mark_signal_sent(self, signal: TradingSignal):
        """Mark signal as sent for deduplication"""
        signal_hash = signal.get_hash()
        self.sent_signals.add(signal_hash)
        self.dedup_timestamps[signal_hash] = time.time()
    
    def _cleanup_dedup_cache(self):
        """Remove old entries from dedup cache"""
        current_time = time.time()
        expired = []
        
        for signal_hash, timestamp in self.dedup_timestamps.items():
            if current_time - timestamp > DEDUP_WINDOW:
                expired.append(signal_hash)
        
        for signal_hash in expired:
            self.sent_signals.discard(signal_hash)
            self.dedup_timestamps.pop(signal_hash, None)
    
    async def close(self):
        """Close connection and flush pending signals"""
        try:
            # Flush pending signals
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
            
            logger.info("📴 Closed signal producer connection")
            
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics"""
        return self.stats.copy()
    
    def reset_stats(self):
        """Reset statistics"""
        self.stats = {
            'signals_sent': 0,
            'signals_deduplicated': 0,
            'signals_expired': 0,
            'batches_sent': 0,
            'errors': 0,
            'by_priority': {p.name: 0 for p in SignalPriority},
            'by_action': {a.name: 0 for a in SignalAction}
        }

# Backward compatibility wrapper
def publish_signal(signal_data: Any) -> bool:
    """
    Legacy function for backward compatibility
    
    Args:
        signal_data: Signal data (dict or any type)
        
    Returns:
        True on success, False on failure
    """
    # Convert to dict if needed
    if not isinstance(signal_data, dict):
        signal_data = {"signal": signal_data}
    
    connection = None
    try:
        # Extract fields with defaults
        action = signal_data.get('action', 'buy')
        ticker = signal_data.get('ticker', 'UNKNOWN')
        price = signal_data.get('price', 0.0)
        
        # Create connection
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        channel = connection.channel()
        channel.queue_declare(queue=DEFAULT_SIGNAL_QUEUE, durable=True)
        
        # Add timestamp if not present
        if 'timestamp' not in signal_data:
            signal_data['timestamp'] = datetime.now().isoformat()
        
        # Send signal as JSON
        message = json.dumps(signal_data)
        channel.basic_publish(
            exchange='',
            routing_key=DEFAULT_SIGNAL_QUEUE,
            body=message.encode(),
            properties=pika.BasicProperties(
                delivery_mode=2  # Persistent
            )
        )
        
        logger.info(f"📬 Published signal: {message}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to publish signal: {e}")
        return False
        
    finally:
        if connection and not connection.is_closed:
            connection.close()
            logger.info("📴 Closed RabbitMQ connection")

# Example usage and testing
if __name__ == "__main__":
    async def test_async_producer():
        """Test async signal producer"""
        print("\n=== Testing Async Signal Producer ===")
        
        async with SignalProducer() as producer:
            # Create various signals
            signals = [
                # High priority signal
                TradingSignal(
                    action=SignalAction.BUY,
                    ticker="AAPL",
                    price=175.50,
                    signal_type=SignalType.BREAKOUT,
                    priority=SignalPriority.HIGH,
                    confidence=92.5,
                    stop_loss=173.00,
                    take_profit=180.00,
                    volume=1500000
                ),
                
                # Medium priority signal
                TradingSignal(
                    action=SignalAction.SELL,
                    ticker="MSFT",
                    price=405.25,
                    signal_type=SignalType.REVERSAL,
                    priority=SignalPriority.MEDIUM,
                    confidence=78.0,
                    stop_loss=408.00,
                    take_profit=400.00
                ),
                
                # Low priority alert
                TradingSignal(
                    action=SignalAction.HOLD,
                    ticker="GOOGL",
                    price=142.30,
                    signal_type=SignalType.ALERT,
                    priority=SignalPriority.LOW,
                    confidence=65.0
                )
            ]
            
            # Publish signals
            for signal in signals:
                success = await producer.publish_signal(signal)
                if success:
                    print(f"✅ Published {signal.ticker} {signal.action.value} signal")
            
            # Test duplicate (should be deduplicated)
            duplicate = signals[0]
            success = await producer.publish_signal(duplicate)
            print(f"Duplicate signal: {'Sent' if success else 'Deduplicated'}")
            
            # Wait for batch to flush
            await asyncio.sleep(1)
            
            # Show statistics
            print("\n📊 Statistics:")
            stats = producer.get_stats()
            print(f"  Signals sent: {stats['signals_sent']}")
            print(f"  Deduplicated: {stats['signals_deduplicated']}")
            print(f"  By priority: {stats['by_priority']}")
            print(f"  By action: {stats['by_action']}")
    
    def test_legacy_function():
        """Test legacy publish_signal function"""
        print("\n=== Testing Legacy Function ===")
        
        test_signal = {
            "action": "buy",
            "ticker": "TSLA",
            "price": 250.00,
            "confidence": 85.0
        }
        
        success = publish_signal(test_signal)
        print(f"Legacy signal: {'✅ Sent' if success else '❌ Failed'}")
    
    # Run tests
    print("Testing RabbitMQ Signal Producer")
    
    # Test async version
    asyncio.run(test_async_producer())
    
    # Test legacy function
    test_legacy_function()
    
    print("\n✅ All tests completed")