"""
Enhanced Reversal Detection System
Combines price action, volume, and technical indicators for high-probability reversals
"""

import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum
from collections import deque
import statistics

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ReversalType(Enum):
    """Types of reversals"""
    BULLISH = "bullish"  # Bottom reversal (going up)
    BEARISH = "bearish"  # Top reversal (going down)
    NONE = "none"

class CandlePattern(Enum):
    """Candlestick patterns"""
    HAMMER = "hammer"
    INVERTED_HAMMER = "inverted_hammer"
    DOJI = "doji"
    BULLISH_ENGULFING = "bullish_engulfing"
    BEARISH_ENGULFING = "bearish_engulfing"
    MORNING_STAR = "morning_star"
    EVENING_STAR = "evening_star"
    SHOOTING_STAR = "shooting_star"
    NONE = "none"

@dataclass
class Bar:
    """Enhanced bar/candle data structure"""
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: Optional[Any] = None
    
    @property
    def body(self) -> float:
        """Candle body size"""
        return abs(self.close - self.open)
    
    @property
    def range(self) -> float:
        """High to low range"""
        return self.high - self.low
    
    @property
    def upper_wick(self) -> float:
        """Upper shadow/wick"""
        return self.high - max(self.open, self.close)
    
    @property
    def lower_wick(self) -> float:
        """Lower shadow/wick"""
        return min(self.open, self.close) - self.low
    
    @property
    def body_ratio(self) -> float:
        """Body as percentage of range"""
        if self.range == 0:
            return 0
        return self.body / self.range
    
    @property
    def is_bullish(self) -> bool:
        """Is a bullish (green) candle"""
        return self.close > self.open
    
    @property
    def is_bearish(self) -> bool:
        """Is a bearish (red) candle"""
        return self.close < self.open
    
    @property
    def is_doji(self) -> bool:
        """Is a doji (small body)"""
        return self.body_ratio < 0.1
    
    def to_dict(self) -> Dict[str, float]:
        """Convert to dictionary"""
        return {
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume
        }

@dataclass
class ReversalSignal:
    """Reversal signal with confidence and metadata"""
    type: ReversalType
    confidence: float  # 0-100
    pattern: CandlePattern
    support_level: Optional[float] = None
    resistance_level: Optional[float] = None
    volume_confirmation: bool = False
    trend_confirmation: bool = False
    momentum_confirmation: bool = False
    metadata: Optional[Dict[str, Any]] = None

class ReversalDetector:
    """Enhanced reversal detection system"""
    
    def __init__(
        self,
        lookback_period: int = 20,
        volume_factor: float = 1.5,
        support_tolerance: float = 0.003,  # 0.3%
        min_trend_strength: float = 0.02,  # 2% trend
        rsi_oversold: float = 30,
        rsi_overbought: float = 70
    ):
        self.lookback_period = lookback_period
        self.volume_factor = volume_factor
        self.support_tolerance = support_tolerance
        self.min_trend_strength = min_trend_strength
        self.rsi_oversold = rsi_oversold
        self.rsi_overbought = rsi_overbought
        
        # Historical data storage
        self.bars: deque = deque(maxlen=lookback_period)
        self.volumes: deque = deque(maxlen=lookback_period)
        self.closes: deque = deque(maxlen=lookback_period)
    
    def add_bar(self, bar: Bar):
        """Add a bar to history"""
        self.bars.append(bar)
        self.volumes.append(bar.volume)
        self.closes.append(bar.close)
    
    def detect_reversal(
        self,
        current_bar: Dict[str, float],
        prev_day_low: Optional[float] = None,
        prev_day_high: Optional[float] = None
    ) -> ReversalSignal:
        """
        Main reversal detection method
        
        Args:
            current_bar: Current bar data
            prev_day_low: Previous day's low (support)
            prev_day_high: Previous day's high (resistance)
            
        Returns:
            ReversalSignal with type and confidence
        """
        # Convert to Bar object
        bar = Bar(**current_bar)
        self.add_bar(bar)
        
        # Need minimum history
        if len(self.bars) < 3:
            return ReversalSignal(ReversalType.NONE, 0, CandlePattern.NONE)
        
        # Detect candlestick pattern
        pattern = self._detect_candle_pattern()
        
        # Check for bullish reversal
        bullish_signal = self._check_bullish_reversal(bar, prev_day_low, pattern)
        
        # Check for bearish reversal
        bearish_signal = self._check_bearish_reversal(bar, prev_day_high, pattern)
        
        # Return the stronger signal
        if bullish_signal.confidence > bearish_signal.confidence:
            return bullish_signal
        else:
            return bearish_signal
    
    def _check_bullish_reversal(
        self,
        bar: Bar,
        support_level: Optional[float],
        pattern: CandlePattern
    ) -> ReversalSignal:
        """Check for bullish (bottom) reversal"""
        confidence = 0.0
        confirmations = {}
        
        # 1. Price at support (30 points)
        if support_level and self._is_at_support(bar, support_level):
            confidence += 30
            confirmations['at_support'] = True
        
        # 2. Bullish candlestick pattern (25 points)
        if pattern in [CandlePattern.HAMMER, CandlePattern.BULLISH_ENGULFING, 
                       CandlePattern.MORNING_STAR]:
            confidence += 25
            confirmations['pattern'] = pattern.value
        
        # 3. Volume confirmation (20 points)
        if self._has_volume_surge(bar):
            confidence += 20
            confirmations['volume_surge'] = True
        
        # 4. Oversold condition (15 points)
        rsi = self._calculate_rsi()
        if rsi and rsi < self.rsi_oversold:
            confidence += 15
            confirmations['oversold'] = True
        
        # 5. Trend reversal setup (10 points)
        if self._is_downtrend() and self._has_bullish_divergence():
            confidence += 10
            confirmations['trend_reversal'] = True
        
        return ReversalSignal(
            type=ReversalType.BULLISH if confidence > 50 else ReversalType.NONE,
            confidence=min(100, confidence),
            pattern=pattern,
            support_level=support_level,
            volume_confirmation=confirmations.get('volume_surge', False),
            trend_confirmation=confirmations.get('trend_reversal', False),
            momentum_confirmation=confirmations.get('oversold', False),
            metadata=confirmations
        )
    
    def _check_bearish_reversal(
        self,
        bar: Bar,
        resistance_level: Optional[float],
        pattern: CandlePattern
    ) -> ReversalSignal:
        """Check for bearish (top) reversal"""
        confidence = 0.0
        confirmations = {}
        
        # 1. Price at resistance (30 points)
        if resistance_level and self._is_at_resistance(bar, resistance_level):
            confidence += 30
            confirmations['at_resistance'] = True
        
        # 2. Bearish candlestick pattern (25 points)
        if pattern in [CandlePattern.SHOOTING_STAR, CandlePattern.BEARISH_ENGULFING,
                       CandlePattern.EVENING_STAR]:
            confidence += 25
            confirmations['pattern'] = pattern.value
        
        # 3. Volume confirmation (20 points)
        if self._has_volume_surge(bar):
            confidence += 20
            confirmations['volume_surge'] = True
        
        # 4. Overbought condition (15 points)
        rsi = self._calculate_rsi()
        if rsi and rsi > self.rsi_overbought:
            confidence += 15
            confirmations['overbought'] = True
        
        # 5. Trend reversal setup (10 points)
        if self._is_uptrend() and self._has_bearish_divergence():
            confidence += 10
            confirmations['trend_reversal'] = True
        
        return ReversalSignal(
            type=ReversalType.BEARISH if confidence > 50 else ReversalType.NONE,
            confidence=min(100, confidence),
            pattern=pattern,
            resistance_level=resistance_level,
            volume_confirmation=confirmations.get('volume_surge', False),
            trend_confirmation=confirmations.get('trend_reversal', False),
            momentum_confirmation=confirmations.get('overbought', False),
            metadata=confirmations
        )
    
    def _detect_candle_pattern(self) -> CandlePattern:
        """Detect candlestick patterns"""
        if len(self.bars) < 1:
            return CandlePattern.NONE
        
        current = self.bars[-1]
        
        # Hammer: Small body at top, long lower wick
        if (current.lower_wick > current.body * 2 and 
            current.upper_wick < current.body * 0.5):
            return CandlePattern.HAMMER
        
        # Shooting Star: Small body at bottom, long upper wick
        if (current.upper_wick > current.body * 2 and
            current.lower_wick < current.body * 0.5):
            return CandlePattern.SHOOTING_STAR
        
        # Doji: Very small body
        if current.is_doji:
            return CandlePattern.DOJI
        
        # Engulfing patterns (need 2 bars)
        if len(self.bars) >= 2:
            prev = self.bars[-2]
            
            # Bullish Engulfing
            if (prev.is_bearish and current.is_bullish and
                current.open <= prev.close and current.close >= prev.open):
                return CandlePattern.BULLISH_ENGULFING
            
            # Bearish Engulfing
            if (prev.is_bullish and current.is_bearish and
                current.open >= prev.close and current.close <= prev.open):
                return CandlePattern.BEARISH_ENGULFING
        
        return CandlePattern.NONE
    
    def _is_at_support(self, bar: Bar, support: float) -> bool:
        """Check if bar is at support level"""
        distance = abs(bar.low - support) / support
        return distance <= self.support_tolerance
    
    def _is_at_resistance(self, bar: Bar, resistance: float) -> bool:
        """Check if bar is at resistance level"""
        distance = abs(bar.high - resistance) / resistance
        return distance <= self.support_tolerance
    
    def _has_volume_surge(self, bar: Bar) -> bool:
        """Check for volume surge"""
        if len(self.volumes) < 5:
            return False
        
        avg_volume = statistics.mean(self.volumes[:-1])  # Exclude current
        return bar.volume > avg_volume * self.volume_factor
    
    def _calculate_rsi(self, period: int = 14) -> Optional[float]:
        """Calculate RSI (Relative Strength Index)"""
        if len(self.closes) < period + 1:
            return None
        
        gains = []
        losses = []
        
        for i in range(1, min(period + 1, len(self.closes))):
            change = self.closes[-i] - self.closes[-i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        avg_gain = statistics.mean(gains) if gains else 0
        avg_loss = statistics.mean(losses) if losses else 0
        
        if avg_loss == 0:
            return 100
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def _is_uptrend(self) -> bool:
        """Check if in uptrend"""
        if len(self.closes) < 5:
            return False
        
        # Simple trend: current price > average of last N prices
        avg_price = statistics.mean(list(self.closes)[:-1])
        current_price = self.closes[-1]
        
        return (current_price - avg_price) / avg_price > self.min_trend_strength
    
    def _is_downtrend(self) -> bool:
        """Check if in downtrend"""
        if len(self.closes) < 5:
            return False
        
        avg_price = statistics.mean(list(self.closes)[:-1])
        current_price = self.closes[-1]
        
        return (avg_price - current_price) / avg_price > self.min_trend_strength
    
    def _has_bullish_divergence(self) -> bool:
        """Check for bullish divergence (price making lower lows, RSI making higher lows)"""
        if len(self.bars) < 5:
            return False
        
        # Simplified: Check if price is near recent low but RSI is higher
        recent_low_price = min(bar.low for bar in list(self.bars)[-5:])
        current_price = self.bars[-1].low
        
        if abs(current_price - recent_low_price) / recent_low_price < 0.01:
            # Price near recent low, check if RSI is improving
            rsi = self._calculate_rsi()
            if rsi and rsi > 35:  # RSI not extremely oversold
                return True
        
        return False
    
    def _has_bearish_divergence(self) -> bool:
        """Check for bearish divergence (price making higher highs, RSI making lower highs)"""
        if len(self.bars) < 5:
            return False
        
        recent_high_price = max(bar.high for bar in list(self.bars)[-5:])
        current_price = self.bars[-1].high
        
        if abs(current_price - recent_high_price) / recent_high_price < 0.01:
            # Price near recent high, check if RSI is weakening
            rsi = self._calculate_rsi()
            if rsi and rsi < 65:  # RSI not extremely overbought
                return True
        
        return False

# Backward compatibility functions
def is_reversal(
    bar: Dict[str, float],
    prev_day_low: float,
    retracement_threshold: float = 0.2,
    prev_low_tolerance: float = 0.003,
    min_volume_factor: float = 1.5,
    average_volume: float = 1000
) -> bool:
    """
    Legacy reversal detection function (backward compatible)
    
    Args:
        bar: dict with keys 'open', 'high', 'low', 'close', 'volume'
        prev_day_low: Yesterday's low price
        retracement_threshold: Minimum retracement percentage
        prev_low_tolerance: Tolerance for prev_day_low match
        min_volume_factor: Minimum volume relative to average
        average_volume: Historical average volume
        
    Returns:
        bool: True if reversal criteria are met
    """
    if not all(key in bar for key in ['open', 'high', 'low', 'close', 'volume']):
        return False
    
    if not isinstance(prev_day_low, (int, float)):
        return False
    
    # Create Bar object
    bar_obj = Bar(**bar)
    
    # Quick checks using enhanced Bar properties
    price_range = bar_obj.range
    if price_range == 0:
        return False
    
    # Calculate metrics
    retracement = (bar_obj.close - bar_obj.low) / price_range
    min_volume = average_volume * min_volume_factor
    
    # Check conditions
    at_support = abs(bar_obj.low - prev_day_low) / prev_day_low <= prev_low_tolerance
    has_long_lower_wick = bar_obj.lower_wick >= 2 * bar_obj.body
    has_short_upper_wick = bar_obj.upper_wick <= 0.5 * bar_obj.body
    good_retracement = retracement >= retracement_threshold
    volume_surge = bar['volume'] >= min_volume
    
    # All conditions must be met
    return (
        at_support and
        has_long_lower_wick and
        has_short_upper_wick and
        good_retracement and
        volume_surge and
        bar_obj.is_bullish  # Must be bullish candle
    )

def is_confirmation(
    prev_bar: Dict[str, float],
    current_bar: Dict[str, float],
    min_body_factor: float = 0.001,
    min_upward_move: float = 0.001
) -> bool:
    """
    Legacy confirmation function (backward compatible)
    
    Args:
        prev_bar: Previous bar data
        current_bar: Current bar data
        min_body_factor: Minimum body size as fraction of price
        min_upward_move: Minimum upward move as fraction of prev close
        
    Returns:
        bool: True if confirmation criteria are met
    """
    if not all(key in prev_bar for key in ['open', 'high', 'low', 'close', 'volume']):
        return False
    
    if not all(key in current_bar for key in ['open', 'high', 'low', 'close', 'volume']):
        return False
    
    current = Bar(**current_bar)
    prev = Bar(**prev_bar)
    
    # Check confirmation criteria
    min_body_size = current.close * min_body_factor
    price_move = (current.close - prev.close) / prev.close
    
    return (
        current.is_bullish and  # Green candle
        current.low >= prev.low and  # Higher low
        current.body >= min_body_size and  # Decent body size
        price_move >= min_upward_move  # Upward price movement
    )

# Example usage
if __name__ == "__main__":
    # Create detector
    detector = ReversalDetector()
    
    # Example bars
    bars = [
        {'open': 100, 'high': 102, 'low': 99, 'close': 101, 'volume': 1000},
        {'open': 101, 'high': 101.5, 'low': 100, 'close': 100.5, 'volume': 900},
        {'open': 100.5, 'high': 101, 'low': 98, 'close': 100.8, 'volume': 1500},  # Potential hammer
    ]
    
    # Detect reversal
    for bar in bars:
        signal = detector.detect_reversal(bar, prev_day_low=99, prev_day_high=103)
        
        if signal.type != ReversalType.NONE:
            print(f"🎯 {signal.type.value.upper()} REVERSAL DETECTED!")
            print(f"   Confidence: {signal.confidence:.1f}%")
            print(f"   Pattern: {signal.pattern.value}")
            print(f"   Volume Confirmation: {signal.volume_confirmation}")
            print(f"   Metadata: {signal.metadata}")
    
    # Test legacy function
    test_bar = {'open': 100, 'high': 101, 'low': 98.5, 'close': 100.5, 'volume': 1500}
    if is_reversal(test_bar, prev_day_low=99):
        print("Legacy reversal detected!")