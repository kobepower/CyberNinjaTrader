import finnhub
import yfinance as yf
import logging
import asyncio
import aiohttp
from typing import Optional, Dict, List, Any, Union, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import lru_cache
from cachetools import TTLCache
from ratelimit import limits, sleep_and_retry
import time
from enum import Enum
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
FINNHUB_RATE_LIMIT = 30  # calls per minute
YFINANCE_RATE_LIMIT = 60  # calls per minute
CACHE_TTL = 60  # seconds
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 0.5
THREAD_POOL_SIZE = 5

class DataSource(Enum):
    """Data source enumeration"""
    FINNHUB = "finnhub"
    YFINANCE = "yfinance"
    CACHE = "cache"
    UNKNOWN = "unknown"

@dataclass
class PriceQuote:
    """Complete price quote data"""
    ticker: str
    price: float
    bid: Optional[float] = None
    ask: Optional[float] = None
    volume: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    open: Optional[float] = None
    prev_close: Optional[float] = None
    timestamp: datetime = None
    source: DataSource = DataSource.UNKNOWN
    
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
    def spread_pct(self) -> Optional[float]:
        """Calculate bid-ask spread percentage"""
        if self.bid and self.ask and self.price > 0:
            return (self.spread / self.price) * 100
        return None
    
    @property
    def age_seconds(self) -> float:
        """Get age of quote in seconds"""
        return (datetime.now() - self.timestamp).total_seconds()
    
    @property
    def is_stale(self, max_age: int = 60) -> bool:
        """Check if quote is stale"""
        return self.age_seconds > max_age
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'ticker': self.ticker,
            'price': self.price,
            'bid': self.bid,
            'ask': self.ask,
            'volume': self.volume,
            'high': self.high,
            'low': self.low,
            'open': self.open,
            'prev_close': self.prev_close,
            'timestamp': self.timestamp.isoformat(),
            'source': self.source.value,
            'spread': self.spread,
            'spread_pct': self.spread_pct,
            'age_seconds': self.age_seconds
        }

class PriceError(Exception):
    """Custom exception for price fetching errors"""
    pass

class PriceUtils:
    """Enhanced price utility class with caching and async support"""
    
    def __init__(
        self,
        finnhub_api_key: Optional[str] = None,
        cache_ttl: int = CACHE_TTL,
        max_retries: int = MAX_RETRIES
    ):
        # API clients
        self.finnhub_client = finnhub.Client(api_key=finnhub_api_key) if finnhub_api_key else None
        self.finnhub_api_key = finnhub_api_key
        
        # Caching
        self.cache = TTLCache(maxsize=1000, ttl=cache_ttl)
        
        # Configuration
        self.max_retries = max_retries
        
        # Thread pool for sync operations in async context
        self.thread_pool = ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE)
        
        # Statistics
        self.stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'finnhub_calls': 0,
            'yfinance_calls': 0,
            'errors': 0
        }
    
    @sleep_and_retry
    @limits(calls=FINNHUB_RATE_LIMIT, period=60)
    def _finnhub_quote(self, ticker: str) -> Dict[str, Any]:
        """Rate-limited Finnhub quote fetch"""
        if not self.finnhub_client:
            raise PriceError("Finnhub client not initialized")
        
        self.stats['finnhub_calls'] += 1
        return self.finnhub_client.quote(ticker)
    
    @sleep_and_retry
    @limits(calls=YFINANCE_RATE_LIMIT, period=60)
    def _yfinance_quote(self, ticker: str) -> Dict[str, Any]:
        """Rate-limited yfinance quote fetch"""
        self.stats['yfinance_calls'] += 1
        stock = yf.Ticker(ticker)
        
        # Use fast_info for better performance
        try:
            fast_info = stock.fast_info
            return {
                'price': fast_info.get('lastPrice', 0),
                'volume': fast_info.get('lastVolume', 0),
                'high': fast_info.get('dayHigh', 0),
                'low': fast_info.get('dayLow', 0),
                'open': fast_info.get('open', 0),
                'prev_close': fast_info.get('previousClose', 0)
            }
        except:
            # Fallback to regular info if fast_info not available
            info = stock.info
            return {
                'price': info.get('currentPrice', info.get('regularMarketPrice', 0)),
                'volume': info.get('volume', 0),
                'high': info.get('dayHigh', 0),
                'low': info.get('dayLow', 0),
                'open': info.get('open', 0),
                'prev_close': info.get('previousClose', 0)
            }
    
    def fetch_price_sync(self, ticker: str, use_cache: bool = True) -> PriceQuote:
        """
        Synchronous price fetch with fallback
        
        Args:
            ticker: Stock symbol
            use_cache: Whether to use cached data
            
        Returns:
            PriceQuote object
            
        Raises:
            PriceError if all sources fail
        """
        ticker = ticker.upper()
        
        # Check cache first
        if use_cache and ticker in self.cache:
            self.stats['cache_hits'] += 1
            quote = self.cache[ticker]
            quote.source = DataSource.CACHE
            logger.debug(f"Cache hit for {ticker}")
            return quote
        
        self.stats['cache_misses'] += 1
        
        # Try Finnhub first
        if self.finnhub_client:
            try:
                data = self._finnhub_quote(ticker)
                
                quote = PriceQuote(
                    ticker=ticker,
                    price=data.get('c', 0),  # Current price
                    high=data.get('h', 0),    # High
                    low=data.get('l', 0),     # Low
                    open=data.get('o', 0),    # Open
                    prev_close=data.get('pc', 0),  # Previous close
                    timestamp=datetime.fromtimestamp(data.get('t', time.time())),
                    source=DataSource.FINNHUB
                )
                
                if quote.price > 0:
                    self.cache[ticker] = quote
                    logger.info(f"Fetched {ticker} from Finnhub: ${quote.price:.2f}")
                    return quote
                    
            except Exception as e:
                logger.warning(f"Finnhub failed for {ticker}: {e}")
        
        # Fallback to yfinance
        try:
            data = self._yfinance_quote(ticker)
            
            quote = PriceQuote(
                ticker=ticker,
                price=data['price'],
                volume=data.get('volume'),
                high=data.get('high'),
                low=data.get('low'),
                open=data.get('open'),
                prev_close=data.get('prev_close'),
                source=DataSource.YFINANCE
            )
            
            # Estimate bid/ask from last price
            if quote.price > 0:
                spread = quote.price * 0.0001  # 0.01% spread estimate
                quote.bid = quote.price - spread
                quote.ask = quote.price + spread
                
                self.cache[ticker] = quote
                logger.info(f"Fetched {ticker} from yfinance: ${quote.price:.2f}")
                return quote
                
        except Exception as e:
            logger.error(f"YFinance failed for {ticker}: {e}")
        
        self.stats['errors'] += 1
        raise PriceError(f"Failed to fetch price for {ticker} from all sources")
    
    async def fetch_price(
        self,
        ticker: str,
        use_cache: bool = True,
        max_staleness: int = 60
    ) -> PriceQuote:
        """
        Asynchronous price fetch
        
        Args:
            ticker: Stock symbol
            use_cache: Whether to use cached data
            max_staleness: Maximum age of cached data in seconds
            
        Returns:
            PriceQuote object
        """
        ticker = ticker.upper()
        
        # Check cache with staleness
        if use_cache and ticker in self.cache:
            quote = self.cache[ticker]
            if quote.age_seconds <= max_staleness:
                self.stats['cache_hits'] += 1
                quote.source = DataSource.CACHE
                return quote
        
        # Run sync fetch in thread pool
        loop = asyncio.get_event_loop()
        
        for attempt in range(self.max_retries):
            try:
                quote = await loop.run_in_executor(
                    self.thread_pool,
                    self.fetch_price_sync,
                    ticker,
                    False  # Don't use cache in retry
                )
                return quote
                
            except PriceError as e:
                if attempt < self.max_retries - 1:
                    delay = INITIAL_RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"Attempt {attempt + 1} failed for {ticker}, retrying in {delay}s")
                    await asyncio.sleep(delay)
                else:
                    raise
    
    async def fetch_multiple(
        self,
        tickers: List[str],
        use_cache: bool = True,
        batch_size: int = 10
    ) -> Dict[str, Union[PriceQuote, PriceError]]:
        """
        Fetch prices for multiple tickers
        
        Args:
            tickers: List of stock symbols
            use_cache: Whether to use cached data
            batch_size: Number of concurrent requests
            
        Returns:
            Dictionary mapping ticker to PriceQuote or PriceError
        """
        results = {}
        
        # Process in batches to avoid overwhelming APIs
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            
            tasks = [
                self.fetch_price(ticker, use_cache)
                for ticker in batch
            ]
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for ticker, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    results[ticker] = PriceError(str(result))
                    logger.error(f"Failed to fetch {ticker}: {result}")
                else:
                    results[ticker] = result
            
            # Small delay between batches
            if i + batch_size < len(tickers):
                await asyncio.sleep(0.5)
        
        return results
    
    def get_spread_info(self, ticker: str) -> Optional[Tuple[float, float]]:
        """
        Get bid-ask spread information
        
        Args:
            ticker: Stock symbol
            
        Returns:
            Tuple of (spread, spread_percentage) or None
        """
        try:
            quote = self.fetch_price_sync(ticker)
            if quote.spread is not None:
                return (quote.spread, quote.spread_pct)
        except PriceError:
            pass
        return None
    
    def get_day_range(self, ticker: str) -> Optional[Tuple[float, float, float]]:
        """
        Get day's price range
        
        Args:
            ticker: Stock symbol
            
        Returns:
            Tuple of (low, high, range) or None
        """
        try:
            quote = self.fetch_price_sync(ticker)
            if quote.low and quote.high:
                return (quote.low, quote.high, quote.high - quote.low)
        except PriceError:
            pass
        return None
    
    def get_change_from_open(self, ticker: str) -> Optional[Tuple[float, float]]:
        """
        Get change from open
        
        Args:
            ticker: Stock symbol
            
        Returns:
            Tuple of (change, change_percentage) or None
        """
        try:
            quote = self.fetch_price_sync(ticker)
            if quote.open and quote.price:
                change = quote.price - quote.open
                change_pct = (change / quote.open) * 100
                return (change, change_pct)
        except PriceError:
            pass
        return None
    
    def get_stats(self) -> Dict[str, int]:
        """Get usage statistics"""
        return self.stats.copy()
    
    def clear_cache(self):
        """Clear the price cache"""
        self.cache.clear()
        logger.info("Price cache cleared")
    
    def close(self):
        """Clean up resources"""
        self.thread_pool.shutdown(wait=True)
        self.clear_cache()

# Global instance for convenience
_default_utils: Optional[PriceUtils] = None

def init_price_utils(finnhub_api_key: Optional[str] = None) -> PriceUtils:
    """Initialize the default price utils instance"""
    global _default_utils
    _default_utils = PriceUtils(finnhub_api_key)
    return _default_utils

def get_price_utils() -> PriceUtils:
    """Get or create the default price utils instance"""
    global _default_utils
    if _default_utils is None:
        _default_utils = PriceUtils()
    return _default_utils

# Backward compatibility functions
def fetch_price(
    ticker: str,
    finnhub_client: Optional[finnhub.Client] = None
) -> float:
    """
    Legacy function for backward compatibility
    
    Args:
        ticker: Stock symbol
        finnhub_client: Optional Finnhub client (ignored, uses internal)
        
    Returns:
        Current price or 0 on error
    """
    try:
        utils = get_price_utils()
        
        # If finnhub_client provided and utils doesn't have one, initialize
        if finnhub_client and not utils.finnhub_client:
            # Extract API key from client if possible
            utils.finnhub_client = finnhub_client
        
        quote = utils.fetch_price_sync(ticker)
        return quote.price
        
    except Exception as e:
        logger.error(f"Failed to fetch price for {ticker}: {e}")
        return 0.0

async def fetch_price_async(ticker: str) -> float:
    """
    Async version for backward compatibility
    
    Args:
        ticker: Stock symbol
        
    Returns:
        Current price or 0 on error
    """
    try:
        utils = get_price_utils()
        quote = await utils.fetch_price(ticker)
        return quote.price
    except Exception as e:
        logger.error(f"Failed to fetch price for {ticker}: {e}")
        return 0.0

# Example usage and testing
if __name__ == "__main__":
    async def test_price_utils():
        """Test the enhanced price utils"""
        
        # Initialize with your API key
        utils = PriceUtils(finnhub_api_key="your_api_key_here")
        
        print("\n=== Testing Single Ticker ===")
        try:
            quote = await utils.fetch_price("AAPL")
            print(f"AAPL Quote:")
            print(f"  Price: ${quote.price:.2f}")
            print(f"  Bid/Ask: ${quote.bid:.2f}/${quote.ask:.2f}" if quote.bid else "  Bid/Ask: N/A")
            print(f"  Volume: {quote.volume:,.0f}" if quote.volume else "  Volume: N/A")
            print(f"  Day Range: ${quote.low:.2f} - ${quote.high:.2f}" if quote.low else "  Day Range: N/A")
            print(f"  Source: {quote.source.value}")
            print(f"  Age: {quote.age_seconds:.1f}s")
        except PriceError as e:
            print(f"Error: {e}")
        
        print("\n=== Testing Multiple Tickers ===")
        tickers = ["AAPL", "MSFT", "GOOGL", "INVALID"]
        results = await utils.fetch_multiple(tickers)
        
        for ticker, result in results.items():
            if isinstance(result, PriceError):
                print(f"{ticker}: ERROR - {result}")
            else:
                print(f"{ticker}: ${result.price:.2f} ({result.source.value})")
        
        print("\n=== Testing Cache ===")
        # First call - will fetch from API
        start = time.time()
        quote1 = await utils.fetch_price("AAPL")
        time1 = time.time() - start
        print(f"First call: {time1:.3f}s")
        
        # Second call - should use cache
        start = time.time()
        quote2 = await utils.fetch_price("AAPL")
        time2 = time.time() - start
        print(f"Second call (cached): {time2:.3f}s")
        print(f"Speed improvement: {time1/time2:.1f}x")
        
        print("\n=== Statistics ===")
        stats = utils.get_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        print("\n=== Testing Backward Compatibility ===")
        # Test legacy function
        price = fetch_price("AAPL")
        print(f"Legacy fetch_price: ${price:.2f}")
        
        # Test async legacy function
        price_async = await fetch_price_async("AAPL")
        print(f"Legacy fetch_price_async: ${price_async:.2f}")
        
        # Cleanup
        utils.close()
    
    # Run test
    asyncio.run(test_price_utils())