import aiohttp
import asyncio
import aio_pika
import json
import time
import logging
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from contextlib import asynccontextmanager
from urllib.parse import quote

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 1.0
DEFAULT_MAX_BACKOFF = 60
DEFAULT_CONNECTION_TIMEOUT = 10
DEFAULT_PREFETCH_COUNT = 10

class QueueState(Enum):
    """RabbitMQ queue states"""
    RUNNING = "running"
    IDLE = "idle"
    STOPPED = "stopped"
    CRASHED = "crashed"
    UNKNOWN = "unknown"

class SignalType(Enum):
    """Trading signal types"""
    REVERSAL = "reversal"
    BOTTOM = "bottom"
    BREAKOUT = "breakout"
    VOLUME_SPIKE = "volume_spike"
    TEST = "test"

class SignalAction(Enum):
    """Trading actions"""
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"
    CLOSE = "CLOSE"

@dataclass
class QueueInfo:
    """Queue information data class"""
    name: str
    messages: int
    messages_ready: int
    messages_unacknowledged: int
    consumers: int
    state: QueueState
    memory: int
    message_rate_in: float
    message_rate_out: float
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
    
    @property
    def is_healthy(self) -> bool:
        """Check if queue is healthy"""
        return (
            self.state == QueueState.RUNNING and
            self.messages_unacknowledged < 1000 and  # Not too many unacked
            self.memory < 100_000_000  # Less than 100MB
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'name': self.name,
            'messages': self.messages,
            'messages_ready': self.messages_ready,
            'messages_unacknowledged': self.messages_unacknowledged,
            'consumers': self.consumers,
            'state': self.state.value,
            'memory': self.memory,
            'message_rate_in': self.message_rate_in,
            'message_rate_out': self.message_rate_out,
            'is_healthy': self.is_healthy,
            'timestamp': self.timestamp.isoformat()
        }

@dataclass
class TradingSignal:
    """Trading signal data class"""
    type: SignalType
    ticker: str
    price: float
    volume: float
    action: SignalAction
    confidence: float = 0.0
    metadata: Dict[str, Any] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if self.metadata is None:
            self.metadata = {}
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        data = {
            'type': self.type.value,
            'ticker': self.ticker,
            'price': self.price,
            'volume': self.volume,
            'action': self.action.value,
            'confidence': self.confidence,
            'metadata': self.metadata,
            'timestamp': self.timestamp.isoformat()
        }
        return json.dumps(data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'TradingSignal':
        """Create from JSON string"""
        data = json.loads(json_str)
        return cls(
            type=SignalType(data['type']),
            ticker=data['ticker'],
            price=data['price'],
            volume=data['volume'],
            action=SignalAction(data['action']),
            confidence=data.get('confidence', 0.0),
            metadata=data.get('metadata', {}),
            timestamp=datetime.fromisoformat(data['timestamp'])
        )

class RabbitMQAPI:
    """Enhanced RabbitMQ API with full async support"""
    
    def __init__(
        self,
        amqp_url: str = "amqp://guest:guest@localhost/",
        http_host: str = "localhost",
        http_port: int = 15672,
        http_user: str = "guest",
        http_password: str = "guest",
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: float = DEFAULT_RETRY_DELAY,
        max_backoff: int = DEFAULT_MAX_BACKOFF
    ):
        # AMQP configuration
        self.amqp_url = amqp_url
        self.amqp_connection: Optional[aio_pika.Connection] = None
        self.amqp_channel: Optional[aio_pika.Channel] = None
        self.connection_lock = asyncio.Lock()
        
        # HTTP API configuration
        self.http_base_url = f"http://{http_host}:{http_port}/api"
        self.http_auth = aiohttp.BasicAuth(http_user, http_password)
        self.http_session: Optional[aiohttp.ClientSession] = None
        
        # Retry configuration
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.max_backoff = max_backoff
        
        # Statistics
        self.stats = {
            'messages_sent': 0,
            'messages_received': 0,
            'connection_failures': 0,
            'api_calls': 0,
            'errors': 0
        }
        
        # Declared queues cache
        self.declared_queues: Dict[str, aio_pika.Queue] = {}
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
    
    async def connect(self):
        """Establish connections to RabbitMQ"""
        async with self.connection_lock:
            # Connect AMQP
            if not self.amqp_connection or self.amqp_connection.is_closed:
                try:
                    self.amqp_connection = await aio_pika.connect_robust(
                        self.amqp_url,
                        timeout=DEFAULT_CONNECTION_TIMEOUT
                    )
                    self.amqp_channel = await self.amqp_connection.channel()
                    await self.amqp_channel.set_qos(
                        prefetch_count=DEFAULT_PREFETCH_COUNT
                    )
                    logger.info("✅ Connected to RabbitMQ AMQP")
                except Exception as e:
                    self.stats['connection_failures'] += 1
                    logger.error(f"❌ Failed to connect to RabbitMQ AMQP: {e}")
                    raise
            
            # Create HTTP session
            if not self.http_session:
                timeout = aiohttp.ClientTimeout(total=DEFAULT_CONNECTION_TIMEOUT)
                self.http_session = aiohttp.ClientSession(
                    auth=self.http_auth,
                    timeout=timeout
                )
                logger.info("✅ Created HTTP session for RabbitMQ API")
    
    async def close(self):
        """Close all connections"""
        # Close AMQP
        if self.amqp_channel:
            await self.amqp_channel.close()
        if self.amqp_connection:
            await self.amqp_connection.close()
        
        # Close HTTP session
        if self.http_session:
            await self.http_session.close()
        
        logger.info("🔚 Closed RabbitMQ connections")
    
    async def ensure_connected(self):
        """Ensure connections are active"""
        if not self.amqp_connection or self.amqp_connection.is_closed:
            await self.connect()
    
    async def _retry_operation(
        self,
        operation: Callable,
        *args,
        **kwargs
    ) -> Any:
        """Execute operation with retry logic"""
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                return await operation(*args, **kwargs)
            except Exception as e:
                last_error = e
                self.stats['errors'] += 1
                
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    delay = min(delay, self.max_backoff)
                    logger.warning(
                        f"Operation failed (attempt {attempt + 1}/{self.max_retries}): {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Operation failed after {self.max_retries} attempts: {e}")
        
        raise last_error
    
    async def get_queue_info(self, queue_name: str) -> Optional[QueueInfo]:
        """
        Fetch detailed queue information
        
        Args:
            queue_name: Name of the queue
            
        Returns:
            QueueInfo object or None on failure
        """
        if not self.http_session:
            await self.connect()
        
        url = f"{self.http_base_url}/queues/%2f/{quote(queue_name)}"
        
        try:
            async def fetch():
                self.stats['api_calls'] += 1
                async with self.http_session.get(url) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    # Parse state
                    state_str = data.get("state", "unknown")
                    try:
                        state = QueueState(state_str)
                    except ValueError:
                        state = QueueState.UNKNOWN
                    
                    # Get message rates
                    msg_stats = data.get("message_stats", {})
                    
                    return QueueInfo(
                        name=queue_name,
                        messages=data.get("messages", 0),
                        messages_ready=data.get("messages_ready", 0),
                        messages_unacknowledged=data.get("messages_unacknowledged", 0),
                        consumers=data.get("consumers", 0),
                        state=state,
                        memory=data.get("memory", 0),
                        message_rate_in=msg_stats.get("publish_details", {}).get("rate", 0.0),
                        message_rate_out=msg_stats.get("deliver_details", {}).get("rate", 0.0)
                    )
            
            return await self._retry_operation(fetch)
            
        except Exception as e:
            logger.error(f"Failed to get queue info for {queue_name}: {e}")
            return None
    
    async def list_queues(self, vhost: str = "/") -> List[QueueInfo]:
        """
        List all queues in a vhost
        
        Args:
            vhost: Virtual host name
            
        Returns:
            List of QueueInfo objects
        """
        if not self.http_session:
            await self.connect()
        
        url = f"{self.http_base_url}/queues/{quote(vhost)}"
        
        try:
            async def fetch():
                self.stats['api_calls'] += 1
                async with self.http_session.get(url) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    queues = []
                    for q in data:
                        try:
                            state = QueueState(q.get("state", "unknown"))
                        except ValueError:
                            state = QueueState.UNKNOWN
                        
                        msg_stats = q.get("message_stats", {})
                        
                        queue_info = QueueInfo(
                            name=q["name"],
                            messages=q.get("messages", 0),
                            messages_ready=q.get("messages_ready", 0),
                            messages_unacknowledged=q.get("messages_unacknowledged", 0),
                            consumers=q.get("consumers", 0),
                            state=state,
                            memory=q.get("memory", 0),
                            message_rate_in=msg_stats.get("publish_details", {}).get("rate", 0.0),
                            message_rate_out=msg_stats.get("deliver_details", {}).get("rate", 0.0)
                        )
                        queues.append(queue_info)
                    
                    return queues
            
            return await self._retry_operation(fetch)
            
        except Exception as e:
            logger.error(f"Failed to list queues: {e}")
            return []
    
    async def purge_queue(self, queue_name: str) -> bool:
        """
        Purge all messages from a queue
        
        Args:
            queue_name: Name of the queue
            
        Returns:
            True on success, False on failure
        """
        if not self.http_session:
            await self.connect()
        
        url = f"{self.http_base_url}/queues/%2f/{quote(queue_name)}/contents"
        
        try:
            async def purge():
                self.stats['api_calls'] += 1
                async with self.http_session.delete(url) as response:
                    response.raise_for_status()
                    return True
            
            result = await self._retry_operation(purge)
            if result:
                logger.info(f"♻️ Purged queue: {queue_name}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to purge queue {queue_name}: {e}")
            return False
    
    async def declare_queue(
        self,
        queue_name: str,
        durable: bool = True,
        auto_delete: bool = False,
        arguments: Optional[Dict[str, Any]] = None
    ) -> aio_pika.Queue:
        """
        Declare a queue
        
        Args:
            queue_name: Name of the queue
            durable: Survive broker restart
            auto_delete: Delete when no consumers
            arguments: Additional queue arguments
            
        Returns:
            Queue object
        """
        await self.ensure_connected()
        
        if queue_name in self.declared_queues:
            return self.declared_queues[queue_name]
        
        queue = await self.amqp_channel.declare_queue(
            queue_name,
            durable=durable,
            auto_delete=auto_delete,
            arguments=arguments
        )
        
        self.declared_queues[queue_name] = queue
        logger.info(f"📦 Declared queue: {queue_name}")
        return queue
    
    async def publish_signal(
        self,
        signal: TradingSignal,
        queue_name: str = "signal_queue",
        persistent: bool = True
    ) -> bool:
        """
        Publish a trading signal to a queue
        
        Args:
            signal: TradingSignal object
            queue_name: Target queue name
            persistent: Make message persistent
            
        Returns:
            True on success, False on failure
        """
        try:
            await self.ensure_connected()
            
            # Declare queue if not exists
            queue = await self.declare_queue(queue_name)
            
            # Create message
            delivery_mode = (
                aio_pika.DeliveryMode.PERSISTENT 
                if persistent 
                else aio_pika.DeliveryMode.NOT_PERSISTENT
            )
            
            message = aio_pika.Message(
                body=signal.to_json().encode(),
                delivery_mode=delivery_mode,
                content_type="application/json",
                timestamp=datetime.now(),
                headers={
                    'signal_type': signal.type.value,
                    'ticker': signal.ticker,
                    'action': signal.action.value
                }
            )
            
            # Publish
            await self.amqp_channel.default_exchange.publish(
                message,
                routing_key=queue.name
            )
            
            self.stats['messages_sent'] += 1
            logger.info(
                f"📡 Published {signal.type.value} signal for {signal.ticker} "
                f"@ ${signal.price:.2f} to {queue_name}"
            )
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish signal: {e}")
            self.stats['errors'] += 1
            return False
    
    async def consume_signals(
        self,
        queue_name: str,
        callback: Callable[[TradingSignal], Any],
        auto_ack: bool = True
    ):
        """
        Consume signals from a queue
        
        Args:
            queue_name: Queue to consume from
            callback: Async function to process signals
            auto_ack: Automatically acknowledge messages
        """
        await self.ensure_connected()
        
        queue = await self.declare_queue(queue_name)
        
        async def process_message(message: aio_pika.IncomingMessage):
            async with message.process(ignore_processed=True):
                try:
                    # Parse signal
                    signal = TradingSignal.from_json(message.body.decode())
                    self.stats['messages_received'] += 1
                    
                    # Process with callback
                    await callback(signal)
                    
                    # Acknowledge if not auto-ack
                    if not auto_ack:
                        await message.ack()
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    self.stats['errors'] += 1
                    
                    # Reject and requeue on error
                    if not auto_ack:
                        await message.nack(requeue=True)
        
        # Start consuming
        await queue.consume(process_message, no_ack=auto_ack)
        logger.info(f"👂 Started consuming from {queue_name}")
    
    async def send_test_signal(
        self,
        ticker: str = "AAPL",
        signal_type: SignalType = SignalType.TEST,
        action: SignalAction = SignalAction.BUY
    ) -> bool:
        """
        Send a test signal
        
        Args:
            ticker: Stock ticker
            signal_type: Type of signal
            action: Trading action
            
        Returns:
            True on success, False on failure
        """
        test_signal = TradingSignal(
            type=signal_type,
            ticker=ticker,
            price=175.45,
            volume=32100,
            action=action,
            confidence=85.5,
            metadata={
                "test": True,
                "source": "rabbitmq_api"
            }
        )
        
        success = await self.publish_signal(test_signal)
        if success:
            logger.info(f"✅ Test signal sent for {ticker}")
        else:
            logger.error(f"❌ Failed to send test signal for {ticker}")
        
        return success
    
    async def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of RabbitMQ
        
        Returns:
            Dictionary with health information
        """
        if not self.http_session:
            await self.connect()
        
        health = {
            'amqp_connected': self.amqp_connection and not self.amqp_connection.is_closed,
            'http_connected': self.http_session is not None,
            'queues': [],
            'stats': self.stats.copy(),
            'timestamp': datetime.now().isoformat()
        }
        
        # Check common queues
        common_queues = ['signal_queue', 'dead_letter_queue']
        for queue_name in common_queues:
            info = await self.get_queue_info(queue_name)
            if info:
                health['queues'].append({
                    'name': queue_name,
                    'healthy': info.is_healthy,
                    'messages': info.messages,
                    'consumers': info.consumers
                })
        
        # Overall health
        health['healthy'] = (
            health['amqp_connected'] and 
            health['http_connected'] and
            all(q['healthy'] for q in health['queues'] if q)
        )
        
        return health
    
    def get_stats(self) -> Dict[str, int]:
        """Get usage statistics"""
        return self.stats.copy()
    
    async def reset_stats(self):
        """Reset statistics"""
        self.stats = {
            'messages_sent': 0,
            'messages_received': 0,
            'connection_failures': 0,
            'api_calls': 0,
            'errors': 0
        }
        logger.info("📊 Statistics reset")


# Synchronous wrappers for GUI compatibility
def get_queue_info_sync(queue_name: str, api_instance: Optional[RabbitMQAPI] = None) -> Optional[QueueInfo]:
    """Synchronous wrapper for get_queue_info"""
    import asyncio
    try:
        if api_instance is None:
            api_instance = rabbitmq_api
        
        # Create new event loop if none exists or if current one is running
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If loop is running, we need to run in a thread
                import concurrent.futures
                import threading
                
                def run_async():
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        return new_loop.run_until_complete(api_instance.get_queue_info(queue_name))
                    finally:
                        new_loop.close()
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_async)
                    return future.result(timeout=10)
            else:
                return loop.run_until_complete(api_instance.get_queue_info(queue_name))
        except RuntimeError:
            # No event loop in current thread
            return asyncio.run(api_instance.get_queue_info(queue_name))
    except Exception as e:
        logger.error(f"Error getting queue info synchronously: {e}")
        return None

def list_queues_sync(vhost: str = "/", api_instance: Optional[RabbitMQAPI] = None) -> List[QueueInfo]:
    """Synchronous wrapper for list_queues"""
    import asyncio
    try:
        if api_instance is None:
            api_instance = rabbitmq_api
        
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures
                
                def run_async():
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        return new_loop.run_until_complete(api_instance.list_queues(vhost))
                    finally:
                        new_loop.close()
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_async)
                    return future.result(timeout=10)
            else:
                return loop.run_until_complete(api_instance.list_queues(vhost))
        except RuntimeError:
            return asyncio.run(api_instance.list_queues(vhost))
    except Exception as e:
        logger.error(f"Error listing queues synchronously: {e}")
        return []

# Convenience functions for backward compatibility
_default_api: Optional[RabbitMQAPI] = None

async def get_rabbitmq_api() -> RabbitMQAPI:
    """Get or create default RabbitMQ API instance"""
    global _default_api
    if _default_api is None:
        _default_api = RabbitMQAPI()
        await _default_api.connect()
    return _default_api

# Example usage and testing
if __name__ == "__main__":
    async def signal_handler(signal: TradingSignal):
        """Example signal handler"""
        print(f"Received signal: {signal.type.value} for {signal.ticker} @ ${signal.price:.2f}")
    
    async def test_rabbitmq_api():
        """Test the RabbitMQ API"""
        
        # Use context manager for automatic cleanup
        async with RabbitMQAPI() as api:
            print("\n=== Testing RabbitMQ API ===")
            
            # Get health status
            print("\n📊 Health Status:")
            health = await api.get_health_status()
            print(f"  Overall: {'✅ Healthy' if health['healthy'] else '❌ Unhealthy'}")
            print(f"  AMQP: {'Connected' if health['amqp_connected'] else 'Disconnected'}")
            print(f"  HTTP: {'Connected' if health['http_connected'] else 'Disconnected'}")
            
            # List queues
            print("\n📦 Queues:")
            queues = await api.list_queues()
            for queue in queues[:5]:  # Show first 5
                print(f"  {queue.name}: {queue.messages} messages, {queue.consumers} consumers")
            
            # Get specific queue info
            print("\n📈 Signal Queue Info:")
            info = await api.get_queue_info("signal_queue")
            if info:
                print(f"  Messages: {info.messages}")
                print(f"  Ready: {info.messages_ready}")
                print(f"  Unacked: {info.messages_unacknowledged}")
                print(f"  Consumers: {info.consumers}")
                print(f"  State: {info.state.value}")
                print(f"  Healthy: {'Yes' if info.is_healthy else 'No'}")
            
            # Send test signals
            print("\n📡 Sending Test Signals:")
            
            # Different signal types
            signals = [
                TradingSignal(
                    type=SignalType.REVERSAL,
                    ticker="AAPL",
                    price=175.50,
                    volume=1000000,
                    action=SignalAction.BUY,
                    confidence=85.0
                ),
                TradingSignal(
                    type=SignalType.BOTTOM,
                    ticker="MSFT",
                    price=405.25,
                    volume=500000,
                    action=SignalAction.BUY,
                    confidence=92.0
                ),
                TradingSignal(
                    type=SignalType.BREAKOUT,
                    ticker="NVDA",
                    price=850.00,
                    volume=2000000,
                    action=SignalAction.SELL,
                    confidence=78.5
                )
            ]
            
            for signal in signals:
                success = await api.publish_signal(signal)
                if success:
                    print(f"  ✅ Sent {signal.type.value} signal for {signal.ticker}")
            
            # Test signal
            await api.send_test_signal("TSLA", SignalType.TEST, SignalAction.HOLD)
            
            # Consume signals (for demonstration)
            print("\n👂 Starting consumer (5 seconds)...")
            
            # Create consumer task
            consumer_task = asyncio.create_task(
                api.consume_signals("signal_queue", signal_handler)
            )
            
            # Let it run for 5 seconds
            await asyncio.sleep(5)
            
            # Cancel consumer
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
            
            # Show statistics
            print("\n📊 Statistics:")
            stats = api.get_stats()
            for key, value in stats.items():
                print(f"  {key}: {value}")
            
            # Purge test queue (cleanup)
            if input("\nPurge signal_queue? (y/n): ").lower() == 'y':
                success = await api.purge_queue("signal_queue")
                print(f"  {'✅ Purged' if success else '❌ Failed to purge'}")
    
    # Run test
    asyncio.run(test_rabbitmq_api())

# Global instance for backward compatibility
rabbitmq_api = RabbitMQAPI()

# Add synchronous methods to the global instance for compatibility
rabbitmq_api.get_queue_info_sync = lambda queue_name: get_queue_info_sync(queue_name, rabbitmq_api)
rabbitmq_api.list_queues_sync = lambda vhost="/": list_queues_sync(vhost, rabbitmq_api)