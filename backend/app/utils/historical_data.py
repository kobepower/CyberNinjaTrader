# backend/app/data/historical_data.py

import yfinance as yf
import asyncio
import logging
import time
from typing import Optional, Dict, List, Any, Tuple
from datetime import datetime, timedelta, date
from dataclasses import dataclass, field
from functools import lru_cache
from cachetools import TTLCache
from contextlib import asynccontextmanager
import pandas as pd
import numpy as np
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
import aiohttp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_CACHE_TTL = 3600  # 1 hour
DEFAULT_MAX_RETRIES = 3
DEFAULT_INITIAL_DELAY = 1.0
DEFAULT_THREAD_POOL_SIZE = 5
MAX_BATCH_SIZE = 50  # YFinance limit

class DataInterval(Enum):
    """Supported data intervals"""
    MINUTE_1 = "1m"
    MINUTE_2 = "2m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    MINUTE_30 = "30m"
    MINUTE_60 = "60m"
    MINUTE_90 = "90m"
    HOUR = "1h"
    DAY = "1d"
    WEEK = "1wk"
    MONTH = "1mo"

class DataPeriod(Enum):
    """Supported data periods"""
    DAY_1 = "1d"
    DAY_5 = "5d"
    MONTH_1 = "1mo"
    MONTH_3 = "3mo"
    MONTH_6 = "6mo"
    YEAR_1 = "1y"
    YEAR_2 = "2y"
    YEAR_5 = "5y"
    YEAR_10 = "10y"
    YTD = "ytd"
    MAX = "max"

@dataclass
class HistoricalBar:
    """Represents a single OHLCV bar"""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    ticker: str
    interval: str = "1d"
    
    @property
    def range(self) -> float:
        """Calculate the bar's range (high - low)"""
        return self.high - self.low
    
    @property
    def change(self) -> float:
        """Calculate the bar's change (close - open)"""
        return self.close - self.open
    
    @property
    def change_percent(self) -> float:
        """Calculate the bar's percentage change"""
        if self.open == 0:
            return 0.0
        return (self.change / self.open) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'timestamp': self.timestamp.isoformat(),
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'ticker': self.ticker,
            'interval': self.interval,
            'range': self.range,
            'change': self.change,
            'change_percent': self.change_percent
        }

@dataclass
class MarketProfile:
    """Market profile data for a ticker"""
    ticker: str
    support_levels: List[float] = field(default_factory=list)
    resistance_levels: List[float] = field(default_factory=list)
    volume_profile: Dict[float, float] = field(default_factory=dict)
    vwap: Optional[float] = None
    point_of_control: Optional[float] = None  # Price level with highest volume
    value_area_high: Optional[float] = None  # Upper bound of 70% volume area
    value_area_low: Optional[float] = None   # Lower bound of 70% volume area
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'ticker': self.ticker,
            'support_levels': self.support_levels,
            'resistance_levels': self.resistance_levels,
            'vwap': self.vwap,
            'point_of_control': self.point_of_control,
            'value_area_high': self.value_area_high,
            'value_area_low': self.value_area_low
        }

class HistoricalDataFetcher:
    """Enhanced historical data fetcher with caching and batch processing"""
    
    def __init__(
        self,
        cache_ttl: int = DEFAULT_CACHE_TTL,
        max_retries: int = DEFAULT_MAX_RETRIES,
        initial_delay: float = DEFAULT_INITIAL_DELAY,
        thread_pool_size: int = DEFAULT_THREAD_POOL_SIZE
    ):
        self.cache = TTLCache(maxsize=1000, ttl=cache_ttl)
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.thread_pool = ThreadPoolExecutor(max_workers=thread_pool_size)
        self.session_cache: Dict[str, yf.Ticker] = {}
        
    def _get_ticker(self, symbol: str) -> yf.Ticker:
        """Get or create a ticker object (cached for session)"""
        if symbol not in self.session_cache:
            self.session_cache[symbol] = yf.Ticker(symbol)
        return self.session_cache[symbol]
    
    def _cache_key(self, ticker: str, period: str, interval: str) -> str:
        """Generate cache key"""
        return f"{ticker}:{period}:{interval}"
    
    async def _fetch_with_retry(
        self,
        func: callable,
        *args,
        **kwargs
    ) -> Optional[Any]:
        """Execute function with exponential backoff retry"""
        for attempt in range(self.max_retries):
            try:
                result = await asyncio.get_event_loop().run_in_executor(
                    self.thread_pool,
                    func,
                    *args
                )
                return result
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{self.max_retries} failed: {e}")
                if attempt < self.max_retries - 1:
                    delay = self.initial_delay * (2 ** attempt)
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Max retries reached, operation failed")
                    return None
    
    async def get_historical_bars(
        self,
        ticker: str,
        period: str = "10d",
        interval: str = "1d",
        prepost: bool = False,
        actions: bool = False
    ) -> List[HistoricalBar]:
        """
        Fetch historical OHLCV bars for a ticker
        
        Args:
            ticker: Stock symbol
            period: Time period (e.g., "1d", "5d", "1mo")
            interval: Bar interval (e.g., "1m", "5m", "1d")
            prepost: Include pre/post market data
            actions: Include dividends and splits
            
        Returns:
            List of HistoricalBar objects
        """
        cache_key = self._cache_key(ticker, period, interval)
        
        # Check cache
        if cache_key in self.cache:
            logger.debug(f"Returning cached data for {ticker}")
            return self.cache[cache_key]
        
        def fetch():
            try:
                data = yf.download(
                    ticker,
                    period=period,
                    interval=interval,
                    auto_adjust=False,
                    prepost=prepost,
                    actions=actions
                )
                
                if data.empty:
                    logger.warning(f"No data returned for {ticker}")
                    return []
                
                bars = []
                for idx, row in data.iterrows():
                    bar = HistoricalBar(
                        timestamp=idx.to_pydatetime() if hasattr(idx, 'to_pydatetime') else idx,
                        open=float(row['Open']),
                        high=float(row['High']),
                        low=float(row['Low']),
                        close=float(row['Close']),
                        volume=float(row['Volume']),
                        ticker=ticker,
                        interval=interval
                    )
                    bars.append(bar)
                
                return bars
            except Exception as e:
                logger.error(f"Error fetching data for {ticker}: {e}")
                raise
        
        bars = await self._fetch_with_retry(fetch)
        if bars:
            self.cache[cache_key] = bars
            logger.info(f"Fetched {len(bars)} bars for {ticker}")
        
        return bars or []
    
    async def get_previous_day_data(
        self,
        ticker: str
    ) -> Optional[Dict[str, float]]:
        """
        Get previous trading day's OHLCV data
        
        Returns:
            Dictionary with open, high, low, close, volume
        """
        bars = await self.get_historical_bars(ticker, period="5d", interval="1d")
        
        if len(bars) < 2:
            logger.warning(f"Insufficient data for {ticker}")
            return None
        
        # Get the second to last bar (previous day)
        prev_bar = bars[-2]
        
        return {
            'open': prev_bar.open,
            'high': prev_bar.high,
            'low': prev_bar.low,
            'close': prev_bar.close,
            'volume': prev_bar.volume,
            'range': prev_bar.range,
            'change': prev_bar.change,
            'change_percent': prev_bar.change_percent
        }
    
    async def get_multiple_tickers(
        self,
        tickers: List[str],
        period: str = "1d",
        interval: str = "1d"
    ) -> Dict[str, List[HistoricalBar]]:
        """
        Fetch data for multiple tickers in parallel
        
        Args:
            tickers: List of stock symbols
            period: Time period
            interval: Bar interval
            
        Returns:
            Dictionary mapping ticker to list of bars
        """
        # Process in batches to respect API limits
        results = {}
        
        for i in range(0, len(tickers), MAX_BATCH_SIZE):
            batch = tickers[i:i + MAX_BATCH_SIZE]
            tasks = [
                self.get_historical_bars(ticker, period, interval)
                for ticker in batch
            ]
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for ticker, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Error fetching {ticker}: {result}")
                    results[ticker] = []
                else:
                    results[ticker] = result
            
            # Small delay between batches
            if i + MAX_BATCH_SIZE < len(tickers):
                await asyncio.sleep(0.5)
        
        return results
    
    async def calculate_support_resistance(
        self,
        ticker: str,
        period: str = "1mo",
        interval: str = "1d",
        sensitivity: float = 0.02
    ) -> Tuple[List[float], List[float]]:
        """
        Calculate support and resistance levels
        
        Args:
            ticker: Stock symbol
            period: Time period for analysis
            interval: Bar interval
            sensitivity: Price clustering sensitivity (0.02 = 2%)
            
        Returns:
            Tuple of (support_levels, resistance_levels)
        """
        bars = await self.get_historical_bars(ticker, period, interval)
        
        if len(bars) < 10:
            logger.warning(f"Insufficient data for support/resistance calculation")
            return [], []
        
        highs = [bar.high for bar in bars]
        lows = [bar.low for bar in bars]
        closes = [bar.close for bar in bars]
        
        # Find local peaks and troughs
        support_levels = []
        resistance_levels = []
        
        for i in range(2, len(bars) - 2):
            # Check for local low (potential support)
            if (lows[i] < lows[i-1] and lows[i] < lows[i-2] and 
                lows[i] < lows[i+1] and lows[i] < lows[i+2]):
                support_levels.append(lows[i])
            
            # Check for local high (potential resistance)
            if (highs[i] > highs[i-1] and highs[i] > highs[i-2] and 
                highs[i] > highs[i+1] and highs[i] > highs[i+2]):
                resistance_levels.append(highs[i])
        
        # Cluster nearby levels
        def cluster_levels(levels: List[float], sensitivity: float) -> List[float]:
            if not levels:
                return []
            
            levels = sorted(levels)
            clustered = []
            current_cluster = [levels[0]]
            
            for level in levels[1:]:
                if (level - current_cluster[-1]) / current_cluster[-1] < sensitivity:
                    current_cluster.append(level)
                else:
                    # Add average of cluster
                    clustered.append(sum(current_cluster) / len(current_cluster))
                    current_cluster = [level]
            
            if current_cluster:
                clustered.append(sum(current_cluster) / len(current_cluster))
            
            return clustered
        
        support_levels = cluster_levels(support_levels, sensitivity)
        resistance_levels = cluster_levels(resistance_levels, sensitivity)
        
        logger.info(f"Found {len(support_levels)} support and {len(resistance_levels)} resistance levels for {ticker}")
        
        return support_levels, resistance_levels
    
    async def calculate_market_profile(
        self,
        ticker: str,
        period: str = "1mo",
        interval: str = "5m",
        price_bins: int = 50
    ) -> MarketProfile:
        """
        Calculate market profile (volume profile) for a ticker
        
        Args:
            ticker: Stock symbol
            period: Time period
            interval: Bar interval (smaller = more accurate)
            price_bins: Number of price levels for volume profile
            
        Returns:
            MarketProfile object
        """
        bars = await self.get_historical_bars(ticker, period, interval)
        
        if not bars:
            logger.warning(f"No data for market profile calculation")
            return MarketProfile(ticker=ticker)
        
        # Calculate price range
        all_prices = []
        volumes = []
        volume_weighted_sum = 0
        total_volume = 0
        
        for bar in bars:
            # Approximate volume distribution across the bar's range
            avg_price = (bar.high + bar.low + bar.close) / 3
            all_prices.append(avg_price)
            volumes.append(bar.volume)
            volume_weighted_sum += avg_price * bar.volume
            total_volume += bar.volume
        
        if total_volume == 0:
            return MarketProfile(ticker=ticker)
        
        # Calculate VWAP
        vwap = volume_weighted_sum / total_volume
        
        # Create volume profile
        min_price = min(bar.low for bar in bars)
        max_price = max(bar.high for bar in bars)
        price_range = max_price - min_price
        
        if price_range == 0:
            return MarketProfile(ticker=ticker, vwap=vwap)
        
        # Create price bins
        bin_size = price_range / price_bins
        volume_profile = {}
        
        for i in range(price_bins):
            bin_price = min_price + (i + 0.5) * bin_size
            volume_profile[bin_price] = 0
        
        # Distribute volume across bins
        for bar in bars:
            # Find bins that this bar spans
            start_bin = int((bar.low - min_price) / bin_size)
            end_bin = min(int((bar.high - min_price) / bin_size), price_bins - 1)
            
            if end_bin >= start_bin:
                volume_per_bin = bar.volume / (end_bin - start_bin + 1)
                for bin_idx in range(start_bin, end_bin + 1):
                    bin_price = min_price + (bin_idx + 0.5) * bin_size
                    if bin_price in volume_profile:
                        volume_profile[bin_price] += volume_per_bin
        
        # Find point of control (highest volume price)
        if volume_profile:
            poc = max(volume_profile, key=volume_profile.get)
        else:
            poc = vwap
        
        # Calculate value area (70% of volume)
        sorted_prices = sorted(volume_profile.items(), key=lambda x: x[1], reverse=True)
        cumulative_volume = 0
        target_volume = total_volume * 0.7
        value_area_prices = []
        
        for price, vol in sorted_prices:
            cumulative_volume += vol
            value_area_prices.append(price)
            if cumulative_volume >= target_volume:
                break
        
        if value_area_prices:
            value_area_low = min(value_area_prices)
            value_area_high = max(value_area_prices)
        else:
            value_area_low = min_price
            value_area_high = max_price
        
        # Calculate support/resistance
        support_levels, resistance_levels = await self.calculate_support_resistance(
            ticker, period, "1d"
        )
        
        return MarketProfile(
            ticker=ticker,
            support_levels=support_levels[:5],  # Top 5 levels
            resistance_levels=resistance_levels[:5],
            volume_profile=dict(sorted(volume_profile.items())[:20]),  # Top 20 price levels
            vwap=vwap,
            point_of_control=poc,
            value_area_high=value_area_high,
            value_area_low=value_area_low
        )
    
    async def get_intraday_bars(
        self,
        ticker: str,
        interval: str = "5m"
    ) -> List[HistoricalBar]:
        """
        Get today's intraday bars
        
        Args:
            ticker: Stock symbol
            interval: Bar interval (1m, 5m, 15m, etc.)
            
        Returns:
            List of intraday bars
        """
        return await self.get_historical_bars(
            ticker,
            period="1d",
            interval=interval,
            prepost=True  # Include pre/post market
        )
    
    async def get_moving_average(
        self,
        ticker: str,
        window: int,
        period: str = "3mo",
        interval: str = "1d",
        ma_type: str = "SMA"
    ) -> Optional[float]:
        """
        Calculate moving average
        
        Args:
            ticker: Stock symbol
            window: MA window (e.g., 20, 50, 200)
            period: Historical period
            interval: Bar interval
            ma_type: Type of MA (SMA, EMA)
            
        Returns:
            Current moving average value
        """
        bars = await self.get_historical_bars(ticker, period, interval)
        
        if len(bars) < window:
            logger.warning(f"Insufficient data for {window}-period MA")
            return None
        
        closes = [bar.close for bar in bars]
        
        if ma_type.upper() == "SMA":
            return sum(closes[-window:]) / window
        elif ma_type.upper() == "EMA":
            # Calculate EMA
            prices = pd.Series(closes)
            ema = prices.ewm(span=window, adjust=False).mean()
            return float(ema.iloc[-1])
        else:
            logger.error(f"Unknown MA type: {ma_type}")
            return None
    
    async def close(self):
        """Clean up resources"""
        self.thread_pool.shutdown(wait=True)
        self.session_cache.clear()
        self.cache.clear()


# Convenience functions for backward compatibility
_default_fetcher = None

def get_default_fetcher() -> HistoricalDataFetcher:
    """Get or create default fetcher instance"""
    global _default_fetcher
    if _default_fetcher is None:
        _default_fetcher = HistoricalDataFetcher()
    return _default_fetcher

async def get_previous_day_low(ticker: str) -> Optional[float]:
    """
    Get previous day's low price
    
    Args:
        ticker: Stock symbol
        
    Returns:
        Previous day's low price or None
    """
    fetcher = get_default_fetcher()
    data = await fetcher.get_previous_day_data(ticker)
    return data['low'] if data else None

async def get_previous_day_high(ticker: str) -> Optional[float]:
    """
    Get previous day's high price
    
    Args:
        ticker: Stock symbol
        
    Returns:
        Previous day's high price or None
    """
    fetcher = get_default_fetcher()
    data = await fetcher.get_previous_day_data(ticker)
    return data['high'] if data else None

async def get_previous_day_close(ticker: str) -> Optional[float]:
    """
    Get previous day's close price
    
    Args:
        ticker: Stock symbol
        
    Returns:
        Previous day's close price or None
    """
    fetcher = get_default_fetcher()
    data = await fetcher.get_previous_day_data(ticker)
    return data['close'] if data else None

# Example usage and testing
if __name__ == "__main__":
    async def test_historical_data():
        """Test the historical data fetcher"""
        fetcher = HistoricalDataFetcher()
        
        # Test single ticker
        print("\n=== Testing Previous Day Data ===")
        prev_data = await fetcher.get_previous_day_data("AAPL")
        if prev_data:
            print(f"AAPL Previous Day:")
            for key, value in prev_data.items():
                print(f"  {key}: {value:.2f}" if isinstance(value, float) else f"  {key}: {value}")
        
        # Test multiple tickers
        print("\n=== Testing Multiple Tickers ===")
        tickers = ["AAPL", "MSFT", "GOOGL"]
        multi_data = await fetcher.get_multiple_tickers(tickers, period="5d")
        for ticker, bars in multi_data.items():
            if bars:
                latest = bars[-1]
                print(f"{ticker}: Close=${latest.close:.2f}, Volume={latest.volume:,.0f}")
        
        # Test support/resistance
        print("\n=== Testing Support/Resistance ===")
        support, resistance = await fetcher.calculate_support_resistance("AAPL")
        print(f"AAPL Support levels: {[f'${s:.2f}' for s in support[:3]]}")
        print(f"AAPL Resistance levels: {[f'${r:.2f}' for r in resistance[:3]]}")
        
        # Test market profile
        print("\n=== Testing Market Profile ===")
        profile = await fetcher.calculate_market_profile("AAPL", period="5d", interval="5m")
        print(f"AAPL Market Profile:")
        print(f"  VWAP: ${profile.vwap:.2f}" if profile.vwap else "  VWAP: N/A")
        print(f"  Point of Control: ${profile.point_of_control:.2f}" if profile.point_of_control else "  POC: N/A")
        print(f"  Value Area: ${profile.value_area_low:.2f} - ${profile.value_area_high:.2f}" 
              if profile.value_area_low else "  Value Area: N/A")
        
        # Test moving averages
        print("\n=== Testing Moving Averages ===")
        sma20 = await fetcher.get_moving_average("AAPL", 20)
        sma50 = await fetcher.get_moving_average("AAPL", 50)
        ema20 = await fetcher.get_moving_average("AAPL", 20, ma_type="EMA")
        
        print(f"AAPL Moving Averages:")
        print(f"  SMA(20): ${sma20:.2f}" if sma20 else "  SMA(20): N/A")
        print(f"  SMA(50): ${sma50:.2f}" if sma50 else "  SMA(50): N/A")
        print(f"  EMA(20): ${ema20:.2f}" if ema20 else "  EMA(20): N/A")
        
        # Test backward compatibility
        print("\n=== Testing Backward Compatibility ===")
        low = await get_previous_day_low("AAPL")
        high = await get_previous_day_high("AAPL")
        close = await get_previous_day_close("AAPL")
        print(f"AAPL Previous Day (using convenience functions):")
        print(f"  Low: ${low:.2f}" if low else "  Low: N/A")
        print(f"  High: ${high:.2f}" if high else "  High: N/A")
        print(f"  Close: ${close:.2f}" if close else "  Close: N/A")
        
        # Cleanup
        await fetcher.close()
    
    # Run test
    asyncio.run(test_historical_data())