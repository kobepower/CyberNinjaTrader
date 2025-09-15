# backend/app/utils/realtime_data.py

import json
import asyncio
import websockets
import os
import logging
import time
import random
from dataclasses import dataclass, field
from typing import Callable, Optional, Dict, List, Set, Any, Union
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, deque
import aiohttp
from contextlib import asynccontextmanager

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
FINNHUB_API_KEY = os.environ.get('FINNHUB_API_KEY', 'your_api_key_here')
WEBSOCKET_URI = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
MAX_SUBSCRIPTIONS_PER_CONNECTION = 50  # Finnhub limit
HEARTBEAT_INTERVAL = 30  # seconds
RECONNECT_DELAY = 5  # seconds
MAX_RECONNECT_ATTEMPTS = 10
MESSAGE_QUEUE_SIZE = 10000

# Fun Zen quotes (keeping the Easter egg!)
ZEN_QUOTES = [
    "🧘‍♂️ Inner peace level: Waiting for market movement...",
    "🌸 Breathe in... breathe out... stocks will come.",
    "💰 Chakras aligned. Wallet? Not yet.",
    "🕉️ Namaste, Nasdaq.",
    "🌿 Market silence is also a trade... of patience.",
    "🌼 Stillness is bullish — eventually.",
    "☯️ The market flows like water, be like water.",
    "🎋 In stillness, opportunity reveals itself.",
]

class MessageType(Enum):
    """WebSocket message types"""
    TRADE = "trade"
    QUOTE = "quote"
    NEWS = "news"
    PING = "ping"
    ERROR = "error"
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"

class SubscriptionStatus(Enum):
    """Subscription status"""
    PENDING = "pending"
    ACTIVE = "active"
    FAILED = "failed"
    UNSUBSCRIBED = "unsubscribed"

@dataclass
class Trade:
    """Trade data structure"""
    symbol: str
    timestamp: int
    price: float
    volume: float
    conditions: List[str] = field(default_factory=list)
    
    @property
    def datetime(self) -> datetime:
        """Convert timestamp to datetime"""
        return datetime.fromtimestamp(self.timestamp / 1000)  # Finnhub uses milliseconds
    
    @property
    def is_valid(self) -> bool:
        """Validate trade data"""
        return (
            self.symbol and 
            self.price > 0 and 
            self.volume >= 0 and
            self.timestamp > 0
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'symbol': self.symbol,
            'timestamp': self.timestamp,
            'datetime': self.datetime.isoformat(),
            'price': self.price,
            'volume': self.volume,
            'conditions': self.conditions
        }

@dataclass
class Quote:
    """Quote data structure"""
    symbol: str
    timestamp: int
    bid: float
    ask: float
    bid_size: Optional[int] = None
    ask_size: Optional[int] = None
    
    @property
    def datetime(self) -> datetime:
        """Convert timestamp to datetime"""
        return datetime.fromtimestamp(self.timestamp / 1000)
    
    @property
    def spread(self) -> float:
        """Calculate bid-ask spread"""
        return self.ask - self.bid
    
    @property
    def mid_price(self) -> float:
        """Calculate mid price"""
        return (self.bid + self.ask) / 2
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'symbol': self.symbol,
            'timestamp': self.timestamp,
            'datetime': self.datetime.isoformat(),
            'bid': self.bid,
            'ask': self.ask,
            'bid_size': self.bid_size,
            'ask_size': self.ask_size,
            'spread': self.spread,
            'mid_price': self.mid_price
        }

class RealtimeDataClient:
    """Enhanced real-time data client with connection management"""
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        max_subscriptions: int = MAX_SUBSCRIPTIONS_PER_CONNECTION,
        heartbeat_interval: int = HEARTBEAT_INTERVAL,
        enable_quotes: bool = True,
        enable_news: bool = False,
        zen_mode: bool = True  # Keep the fun!
    ):
        self.api_key = api_key or FINNHUB_API_KEY
        self.uri = f"wss://ws.finnhub.io?token={self.api_key}"
        self.max_subscriptions = max_subscriptions
        self.heartbeat_interval = heartbeat_interval
        self.enable_quotes = enable_quotes
        self.enable_news = enable_news
        self.zen_mode = zen_mode
        
        # WebSocket management
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.connection_lock = asyncio.Lock()
        self.connected = False
        self.reconnect_attempts = 0
        
        # Subscriptions
        self.subscriptions: Dict[str, SubscriptionStatus] = {}
        self.trade_handlers: Dict[str, List[Callable]] = defaultdict(list)
        self.quote_handlers: Dict[str, List[Callable]] = defaultdict(list)
        self.news_handlers: List[Callable] = []
        
        # Message queue
        self.message_queue: asyncio.Queue = asyncio.Queue(maxsize=MESSAGE_QUEUE_SIZE)
        
        # Tasks
        self.tasks: List[asyncio.Task] = []
        self.running = False
        
        # Statistics
        self.stats = {
            'messages_received': 0,
            'trades_processed': 0,
            'quotes_processed': 0,
            'errors': 0,
            'reconnects': 0,
            'last_message_time': None,
            'uptime_start': None
        }
        
        # Rate limiting
        self.last_subscribe_time = 0
        self.subscribe_delay = 0.1  # 100ms between subscriptions
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.disconnect()
    
    async def connect(self):
        """Establish WebSocket connection"""
        async with self.connection_lock:
            if self.connected:
                return
            
            try:
                logger.info(f"🔌 Connecting to Finnhub WebSocket...")
                self.websocket = await websockets.connect(
                    self.uri,
                    ping_interval=self.heartbeat_interval,
                    ping_timeout=10,
                    close_timeout=10
                )
                
                self.connected = True
                self.running = True
                self.reconnect_attempts = 0
                self.stats['uptime_start'] = datetime.now()
                
                # Start background tasks
                self.tasks = [
                    asyncio.create_task(self._message_processor()),
                    asyncio.create_task(self._heartbeat_monitor()),
                    asyncio.create_task(self._receive_messages())
                ]
                
                logger.info("✅ Connected to Finnhub WebSocket")
                
                # Resubscribe to existing tickers
                await self._resubscribe_all()
                
            except Exception as e:
                self.connected = False
                logger.error(f"❌ Failed to connect: {e}")
                raise
    
    async def disconnect(self):
        """Disconnect from WebSocket"""
        self.running = False
        
        # Cancel all tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Close WebSocket
        if self.websocket:
            await self.websocket.close()
        
        self.connected = False
        self.websocket = None
        logger.info("🔌 Disconnected from Finnhub WebSocket")
    
    async def _reconnect(self):
        """Handle reconnection with exponential backoff"""
        if self.reconnect_attempts >= MAX_RECONNECT_ATTEMPTS:
            logger.error(f"Max reconnection attempts ({MAX_RECONNECT_ATTEMPTS}) reached")
            return
        
        self.reconnect_attempts += 1
        delay = RECONNECT_DELAY * (2 ** (self.reconnect_attempts - 1))
        delay = min(delay, 300)  # Cap at 5 minutes
        
        logger.info(f"🔄 Reconnecting in {delay}s (attempt {self.reconnect_attempts}/{MAX_RECONNECT_ATTEMPTS})")
        await asyncio.sleep(delay)
        
        try:
            await self.connect()
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            await self._reconnect()
    
    async def _receive_messages(self):
        """Receive messages from WebSocket"""
        while self.running:
            try:
                if not self.websocket:
                    await asyncio.sleep(1)
                    continue
                
                message = await self.websocket.recv()
                self.stats['messages_received'] += 1
                self.stats['last_message_time'] = datetime.now()
                
                # Queue message for processing
                if not self.message_queue.full():
                    await self.message_queue.put(message)
                else:
                    logger.warning("Message queue full, dropping message")
                    
            except websockets.exceptions.ConnectionClosed:
                logger.warning("Connection closed, attempting reconnection...")
                self.connected = False
                self.stats['reconnects'] += 1
                await self._reconnect()
            except Exception as e:
                logger.error(f"Error receiving message: {e}")
                self.stats['errors'] += 1
    
    async def _message_processor(self):
        """Process queued messages"""
        while self.running:
            try:
                # Get message with timeout
                message = await asyncio.wait_for(
                    self.message_queue.get(),
                    timeout=1.0
                )
                
                await self._handle_message(message)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                self.stats['errors'] += 1
    
    async def _handle_message(self, message: str):
        """Handle incoming WebSocket message"""
        try:
            data = json.loads(message)
            msg_type = data.get("type")
            
            if msg_type == MessageType.PING.value:
                await self._handle_ping()
            elif msg_type == MessageType.TRADE.value:
                await self._handle_trades(data)
            elif msg_type == MessageType.QUOTE.value:
                await self._handle_quotes(data)
            elif msg_type == MessageType.NEWS.value:
                await self._handle_news(data)
            elif msg_type == MessageType.ERROR.value:
                logger.error(f"Server error: {data.get('msg', 'Unknown error')}")
                self.stats['errors'] += 1
            else:
                logger.debug(f"Unknown message type: {msg_type}")
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
            self.stats['errors'] += 1
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            self.stats['errors'] += 1
    
    async def _handle_ping(self):
        """Handle ping message"""
        if self.zen_mode:
            zen_quote = random.choice(ZEN_QUOTES)
            logger.info(f"🧘 {zen_quote}")
    
    async def _handle_trades(self, data: Dict):
        """Handle trade messages"""
        if "data" not in data:
            return
        
        for item in data["data"]:
            try:
                trade = Trade(
                    symbol=item["s"],
                    timestamp=item["t"],
                    price=float(item["p"]),
                    volume=float(item["v"]),
                    conditions=item.get("c", [])
                )
                
                if not trade.is_valid:
                    logger.warning(f"Invalid trade data: {item}")
                    continue
                
                self.stats['trades_processed'] += 1
                
                # Call handlers for this symbol
                if trade.symbol in self.trade_handlers:
                    for handler in self.trade_handlers[trade.symbol]:
                        try:
                            if asyncio.iscoroutinefunction(handler):
                                await handler(trade)
                            else:
                                await asyncio.get_event_loop().run_in_executor(
                                    None, handler, trade
                                )
                        except Exception as e:
                            logger.error(f"Trade handler error for {trade.symbol}: {e}")
                
                # Fun message for big prices
                if trade.price > 1000 and self.zen_mode:
                    logger.info(f"🚀 To the moon! {trade.symbol} @ ${trade.price:,.2f}")
                    
            except (KeyError, ValueError) as e:
                logger.error(f"Failed to parse trade: {e}")
                self.stats['errors'] += 1
    
    async def _handle_quotes(self, data: Dict):
        """Handle quote messages"""
        if "data" not in data or not self.enable_quotes:
            return
        
        for item in data["data"]:
            try:
                quote = Quote(
                    symbol=item["s"],
                    timestamp=item["t"],
                    bid=float(item.get("b", 0)),
                    ask=float(item.get("a", 0)),
                    bid_size=item.get("bv"),
                    ask_size=item.get("av")
                )
                
                self.stats['quotes_processed'] += 1
                
                # Call handlers
                if quote.symbol in self.quote_handlers:
                    for handler in self.quote_handlers[quote.symbol]:
                        try:
                            if asyncio.iscoroutinefunction(handler):
                                await handler(quote)
                            else:
                                await asyncio.get_event_loop().run_in_executor(
                                    None, handler, quote
                                )
                        except Exception as e:
                            logger.error(f"Quote handler error for {quote.symbol}: {e}")
                            
            except (KeyError, ValueError) as e:
                logger.error(f"Failed to parse quote: {e}")
                self.stats['errors'] += 1
    
    async def _handle_news(self, data: Dict):
        """Handle news messages"""
        if not self.enable_news:
            return
        
        # Process news (implementation depends on your needs)
        for handler in self.news_handlers:
            try:
                await handler(data)
            except Exception as e:
                logger.error(f"News handler error: {e}")
    
    async def _heartbeat_monitor(self):
        """Monitor connection health"""
        while self.running:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                
                # Check last message time
                if self.stats['last_message_time']:
                    time_since_last = (datetime.now() - self.stats['last_message_time']).seconds
                    if time_since_last > self.heartbeat_interval * 2:
                        logger.warning(f"No messages for {time_since_last}s, connection might be stale")
                        # Could trigger reconnection here if needed
                
                # Log statistics periodically
                if self.stats['messages_received'] % 1000 == 0:
                    self._log_statistics()
                    
            except Exception as e:
                logger.error(f"Heartbeat monitor error: {e}")
    
    async def subscribe_to_trades(
        self,
        ticker: str,
        handler: Callable[[Trade], Any]
    ) -> bool:
        """
        Subscribe to trade updates for a ticker
        
        Args:
            ticker: Stock symbol
            handler: Callback function for trades
            
        Returns:
            True on success, False on failure
        """
        ticker = ticker.upper()
        
        # Check subscription limit
        if len(self.subscriptions) >= self.max_subscriptions:
            logger.error(f"Maximum subscriptions ({self.max_subscriptions}) reached")
            return False
        
        # Add handler
        self.trade_handlers[ticker].append(handler)
        
        # Subscribe if not already subscribed
        if ticker not in self.subscriptions:
            await self._subscribe_ticker(ticker)
        
        return ticker in self.subscriptions
    
    async def subscribe_to_quotes(
        self,
        ticker: str,
        handler: Callable[[Quote], Any]
    ) -> bool:
        """
        Subscribe to quote updates for a ticker
        
        Args:
            ticker: Stock symbol
            handler: Callback function for quotes
            
        Returns:
            True on success, False on failure
        """
        if not self.enable_quotes:
            logger.warning("Quote subscriptions are disabled")
            return False
        
        ticker = ticker.upper()
        self.quote_handlers[ticker].append(handler)
        
        # Subscribe if needed
        if ticker not in self.subscriptions:
            await self._subscribe_ticker(ticker)
        
        return ticker in self.subscriptions
    
    async def _subscribe_ticker(self, ticker: str):
        """Send subscription request for a ticker"""
        if not self.websocket or not self.connected:
            logger.warning(f"Cannot subscribe to {ticker}, not connected")
            return
        
        # Rate limiting
        time_since_last = time.time() - self.last_subscribe_time
        if time_since_last < self.subscribe_delay:
            await asyncio.sleep(self.subscribe_delay - time_since_last)
        
        try:
            # Send subscription message
            message = {
                "type": "subscribe",
                "symbol": ticker
            }
            
            await self.websocket.send(json.dumps(message))
            self.subscriptions[ticker] = SubscriptionStatus.ACTIVE
            self.last_subscribe_time = time.time()
            
            logger.info(f"📊 Subscribed to {ticker}")
            
        except Exception as e:
            logger.error(f"Failed to subscribe to {ticker}: {e}")
            self.subscriptions[ticker] = SubscriptionStatus.FAILED
    
    async def unsubscribe(self, ticker: str):
        """Unsubscribe from a ticker"""
        ticker = ticker.upper()
        
        if ticker not in self.subscriptions:
            return
        
        if self.websocket and self.connected:
            try:
                message = {
                    "type": "unsubscribe",
                    "symbol": ticker
                }
                await self.websocket.send(json.dumps(message))
                logger.info(f"📊 Unsubscribed from {ticker}")
            except Exception as e:
                logger.error(f"Failed to unsubscribe from {ticker}: {e}")
        
        # Remove handlers and subscription
        self.trade_handlers.pop(ticker, None)
        self.quote_handlers.pop(ticker, None)
        self.subscriptions[ticker] = SubscriptionStatus.UNSUBSCRIBED
    
    async def _resubscribe_all(self):
        """Resubscribe to all tickers after reconnection"""
        for ticker in list(self.subscriptions.keys()):
            if self.subscriptions[ticker] != SubscriptionStatus.UNSUBSCRIBED:
                await self._subscribe_ticker(ticker)
                await asyncio.sleep(self.subscribe_delay)
    
    def _log_statistics(self):
        """Log current statistics"""
        uptime = (datetime.now() - self.stats['uptime_start']).total_seconds() if self.stats['uptime_start'] else 0
        
        logger.info(
            f"📊 Stats - Messages: {self.stats['messages_received']:,} | "
            f"Trades: {self.stats['trades_processed']:,} | "
            f"Quotes: {self.stats['quotes_processed']:,} | "
            f"Errors: {self.stats['errors']} | "
            f"Uptime: {uptime:.0f}s"
        )
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get current statistics"""
        stats = self.stats.copy()
        if stats['uptime_start']:
            stats['uptime_seconds'] = (datetime.now() - stats['uptime_start']).total_seconds()
        stats['active_subscriptions'] = len([s for s in self.subscriptions.values() if s == SubscriptionStatus.ACTIVE])
        return stats

# Backward compatibility function
async def subscribe_to_ticker(
    ticker: str,
    on_trade: Callable[[Trade], None],
    max_retries: int = 5,
    initial_delay: float = 1
):
    """
    Legacy function for backward compatibility
    
    Args:
        ticker: Stock symbol
        on_trade: Trade handler callback
        max_retries: Max retry attempts
        initial_delay: Initial retry delay
    """
    client = RealtimeDataClient()
    
    for attempt in range(max_retries):
        try:
            await client.connect()
            await client.subscribe_to_trades(ticker, on_trade)
            
            # Keep running
            while client.connected:
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Error in subscription (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                delay = initial_delay * (2 ** attempt)
                await asyncio.sleep(delay)
            else:
                logger.error(f"Max retries reached for {ticker}")
                break
        finally:
            await client.disconnect()

# Example usage
if __name__ == "__main__":
    async def handle_trade(trade: Trade):
        print(f"📈 {trade.symbol}: ${trade.price:.2f} x {trade.volume:.0f} @ {trade.datetime}")
    
    async def handle_quote(quote: Quote):
        print(f"💱 {quote.symbol}: Bid ${quote.bid:.2f} / Ask ${quote.ask:.2f} (spread: ${quote.spread:.4f})")
    
    async def main():
        # Using the new client
        async with RealtimeDataClient() as client:
            # Subscribe to multiple tickers
            await client.subscribe_to_trades("AAPL", handle_trade)
            await client.subscribe_to_trades("MSFT", handle_trade)
            await client.subscribe_to_quotes("AAPL", handle_quote)
            
            # Run for a while
            await asyncio.sleep(60)
            
            # Show statistics
            stats = client.get_statistics()
            print("\n📊 Session Statistics:")
            for key, value in stats.items():
                print(f"  {key}: {value}")
    
    # Run
    asyncio.run(main()