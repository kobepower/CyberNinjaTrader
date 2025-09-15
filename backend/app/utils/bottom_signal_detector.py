import datetime
import argparse
import json
import os
import time
import math
from typing import Optional, Dict, List, Tuple
from collections import deque
import pytz
import yfinance as yf
import requests
import sqlite3
from contextlib import contextmanager
import numpy as np

class ImprovedBottomSignalDetector:
    def __init__(
        self,
        symbol: str,
        stop_loss_pct: float = 0.005,  # 0.5% stop (increased from 0.3%)
        upper_threshold_pct: float = 0.015,  # 1.5% target
        hold_duration_sec: int = 300,  # 5 minutes (reduced from 10)
        risk_per_trade_pct: float = 0.005,
        account_size: float = 60000.0,
        verbose: bool = True,
        finnhub_api_key: Optional[str] = None,
        timezone: str = "US/Eastern",
        max_daily_loss: float = 0.02,
        candle_period_sec: int = 60,  # 1-minute candles instead of 5
        strategy_config: Optional[Dict] = None  # User-configurable strategies
    ):
        self.symbol = symbol
        self.stop_loss_pct = stop_loss_pct
        self.upper_threshold_pct = upper_threshold_pct
        self.hold_duration_sec = hold_duration_sec
        self.risk_per_trade_pct = risk_per_trade_pct
        self.risk_per_trade = account_size * risk_per_trade_pct
        self.account_size = account_size
        self.verbose = verbose
        self.finnhub_api_key = finnhub_api_key
        self.timezone = pytz.timezone(timezone)
        self.max_daily_loss = max_daily_loss
        self.candle_period_sec = candle_period_sec
        
        # Multiple support levels tracking
        self.support_levels = []  # List of support levels
        self.resistance_levels = []
        self.intraday_low = None
        self.intraday_high = None
        self.yesterday_low = None
        self.yesterday_high = None
        self.yesterday_close = None
        
        # Enhanced tracking
        self.entry_price = None
        self.highest_close = None
        self.hold_start_time = None
        self.is_holding = False
        self.position_size = 0
        
        # Quick bounce detection
        self.recent_lows = deque(maxlen=5)  # Track last 5 candle lows
        self.bounce_detected = False
        self.last_bottom_time = None
        
        # Volume tracking - more adaptive
        self.volume_history = deque(maxlen=20)
        self.volume_spike_threshold = 1.2  # Reduced from 1.5x
        
        # Price action tracking
        self.price_history = deque(maxlen=50)
        self.current_bucket = None
        self.current_candle = None
        
        # Risk management
        self.daily_pnl = 0.0
        self.daily_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.last_trade_time = None
        
        # Strategy Configuration - User Toggleable
        self.strategy_config = strategy_config or self.get_default_strategy_config()
        
        # Database
        self.db_file = "support_cache.db"
        self.init_database()
        self.load_cached_levels()

    def get_default_strategy_config(self) -> Dict:
        """Default strategy configuration - all strategies enabled"""
        return {
            # Strategy Toggles (True/False)
            "enable_quick_bounce": True,
            "enable_support_touch": True,
            "enable_local_low_recovery": True,
            "enable_volume_analysis": True,
            "enable_rsi_oversold": True,
            "enable_price_action": True,
            "enable_market_regime_adjustment": True,
            
            # Strategy Weights/Points (adjustable by user)
            "quick_bounce_points": 30,
            "support_touch_points": 25,
            "local_low_recovery_points": 20,
            "volume_spike_points": 25,
            "volume_high_points": 15,
            "volume_elevated_points": 10,
            "rsi_oversold_points": 15,
            "strong_price_action_points": 15,
            "good_price_action_points": 10,
            "regime_bonus_points": 5,
            
            # Minimum score required to trigger signal
            "min_signal_score": 50,
            
            # Strategy Parameters (fine-tuning)
            "rsi_threshold": 40,
            "rsi_threshold_bearish": 45,
            "rsi_threshold_bullish": 35,
            "min_bounce_pct": 0.001,
            "min_bounce_pct_bearish": 0.0005,
            "min_bounce_pct_bullish": 0.002,
            "support_tolerance": 0.005,
            "support_tolerance_bearish": 0.007,
            "volume_spike_threshold": 1.2,
            "volume_spike_threshold_high_vol": 1.5,
            "price_action_strong_threshold": 70,
            "price_action_good_threshold": 40,
            
            # Advanced Settings
            "enable_market_hours_only": True,
            "enable_daily_loss_limit": True,
            "enable_trade_spacing": True,
            "min_trade_spacing_sec": 60,
            "log_near_misses": True,
            "near_miss_threshold": 30
        }

    def update_strategy_config(self, config_updates: Dict):
        """Update strategy configuration with user preferences"""
        if not isinstance(config_updates, dict):
            if self.verbose:
                print(f"[{self.symbol}] Invalid config format, using defaults")
            return
        
        # Update existing config with new values
        for key, value in config_updates.items():
            if key in self.strategy_config:
                old_value = self.strategy_config[key]
                self.strategy_config[key] = value
                if self.verbose:
                    print(f"[{self.symbol}] Config updated: {key} = {value} (was {old_value})")
            else:
                if self.verbose:
                    print(f"[{self.symbol}] Unknown config key ignored: {key}")

    def get_strategy_config_json(self) -> str:
        """Get current strategy configuration as JSON for easy saving/loading"""
        return json.dumps(self.strategy_config, indent=2)

    def load_strategy_config_from_json(self, json_string: str):
        """Load strategy configuration from JSON string"""
        try:
            config = json.loads(json_string)
            self.update_strategy_config(config)
        except json.JSONDecodeError as e:
            if self.verbose:
                print(f"[{self.symbol}] Error loading config JSON: {e}")

    def save_strategy_config_to_file(self, filename: str):
        """Save current strategy configuration to file"""
        try:
            with open(filename, 'w') as f:
                f.write(self.get_strategy_config_json())
            if self.verbose:
                print(f"[{self.symbol}] Strategy config saved to {filename}")
        except Exception as e:
            if self.verbose:
                print(f"[{self.symbol}] Error saving config: {e}")

    def load_strategy_config_from_file(self, filename: str):
        """Load strategy configuration from file"""
        try:
            with open(filename, 'r') as f:
                config_json = f.read()
            self.load_strategy_config_from_json(config_json)
            if self.verbose:
                print(f"[{self.symbol}] Strategy config loaded from {filename}")
        except Exception as e:
            if self.verbose:
                print(f"[{self.symbol}] Error loading config file: {e}")

    def print_current_config(self):
        """Print current strategy configuration in a readable format"""
        print(f"\n=== {self.symbol} Strategy Configuration ===")
        
        print("\n📊 Strategy Toggles:")
        toggles = {k: v for k, v in self.strategy_config.items() if k.startswith('enable_')}
        for key, value in toggles.items():
            strategy_name = key.replace('enable_', '').replace('_', ' ').title()
            status = "✅ ENABLED" if value else "❌ DISABLED"
            print(f"  {strategy_name}: {status}")
        
        print(f"\n🎯 Signal Requirements:")
        print(f"  Minimum Score Required: {self.strategy_config['min_signal_score']}/100")
        
        print(f"\n⚖️ Strategy Points:")
        points = {k: v for k, v in self.strategy_config.items() if k.endswith('_points')}
        for key, value in points.items():
            strategy_name = key.replace('_points', '').replace('_', ' ').title()
            print(f"  {strategy_name}: {value} points")
        
        print(f"\n🔧 Parameters:")
        params = {k: v for k, v in self.strategy_config.items() 
                 if not k.startswith('enable_') and not k.endswith('_points') 
                 and k != 'min_signal_score'}
        for key, value in params.items():
            param_name = key.replace('_', ' ').title()
            print(f"  {param_name}: {value}")
        print("=" * 50)

    def fetch_enhanced_support_levels(self) -> List[float]:
        """Fetch multiple support levels using different methods"""
        support_levels = []
        
        try:
            ticker = yf.Ticker(self.symbol)
            
            # Get different timeframes
            hist_5d = ticker.history(period="5d", interval="1h")
            hist_1mo = ticker.history(period="1mo", interval="1d")
            hist_today = ticker.history(period="1d", interval="1m")
            
            if len(hist_5d) > 0:
                # Yesterday's data
                if len(hist_1mo) >= 2:
                    self.yesterday_low = hist_1mo['Low'].iloc[-2]
                    self.yesterday_high = hist_1mo['High'].iloc[-2]
                    self.yesterday_close = hist_1mo['Close'].iloc[-2]
                    support_levels.append(self.yesterday_low)
                
                # Today's current low
                if len(hist_today) > 0:
                    self.intraday_low = hist_today['Low'].min()
                    self.intraday_high = hist_today['High'].max()
                    support_levels.append(self.intraday_low)
                
                # Key psychological levels (round numbers)
                current_price = hist_today['Close'].iloc[-1] if len(hist_today) > 0 else hist_5d['Close'].iloc[-1]
                psychological_level = math.floor(current_price / 1) * 1  # Round dollar
                support_levels.append(psychological_level)
                support_levels.append(psychological_level - 1)  # Next dollar down
                
                # Volume-weighted support (high volume areas)
                for i in range(len(hist_5d) - 1):
                    if hist_5d['Volume'].iloc[i] > hist_5d['Volume'].mean() * 1.5:
                        support_levels.append(hist_5d['Low'].iloc[i])
                
                # Recent swing lows
                lows = hist_5d['Low'].values
                for i in range(1, len(lows) - 1):
                    if lows[i] < lows[i-1] and lows[i] < lows[i+1]:
                        support_levels.append(lows[i])
                
            # Remove duplicates and sort
            self.support_levels = sorted(list(set(support_levels)))
            
            if self.verbose:
                print(f"[{self.symbol}] Support levels identified: {self.support_levels}")
                print(f"[{self.symbol}] Yesterday's Low: {self.yesterday_low}, Today's Low: {self.intraday_low}")
            
        except Exception as e:
            if self.verbose:
                print(f"[{self.symbol}] Error fetching enhanced levels: {e}")
        
        return self.support_levels

    def find_nearest_support(self, current_price: float) -> Optional[float]:
        """Find the nearest support level below current price"""
        if not self.support_levels:
            self.fetch_enhanced_support_levels()
        
        valid_supports = [s for s in self.support_levels if s < current_price]
        if valid_supports:
            return max(valid_supports)  # Nearest support below
        return None

    def detect_quick_bounce(self, candle: Dict) -> bool:
        """Detect quick bounces (wicks) that recover within the candle"""
        low = candle['low']
        close = candle['close']
        open_price = candle['open']
        high = candle['high']
        
        # Check for hammer pattern (long lower wick)
        body = abs(close - open_price)
        lower_wick = min(open_price, close) - low
        upper_wick = high - max(open_price, close)
        
        # Strong bounce if lower wick is 2x the body
        if lower_wick > body * 2 and upper_wick < body:
            return True
        
        # Check if we bounced from any known support
        for support in self.support_levels:
            if low <= support * 1.002 and close > support * 1.003:  # Hit and bounced
                return True
        
        return False

    def check_bottom_conditions_improved(self, candle: Dict) -> bool:
        """Improved bottom detection with user-configurable strategies"""
        low_price = candle['low']
        close_price = candle['close']
        volume = candle['volume']
        
        # Pre-filters based on configuration
        if self.strategy_config["enable_market_hours_only"] and not self.is_market_hours():
            return False
        if self.strategy_config["enable_daily_loss_limit"] and not self.should_trade():
            return False
        
        # Update recent lows
        self.recent_lows.append(low_price)
        
        # Get adaptive parameters based on market regime
        params = self.adaptive_parameters()
        
        # Enhanced analysis
        volume_analysis = self.enhanced_volume_analysis(candle)
        price_action = self.price_action_score(candle)
        
        signal_score = 0
        signal_reasons = []
        
        # Strategy 1: Quick wick bounce detection
        if self.strategy_config["enable_quick_bounce"] and self.detect_quick_bounce(candle):
            points = self.strategy_config["quick_bounce_points"]
            signal_score += points
            signal_reasons.append("QUICK_BOUNCE")
            if self.verbose:
                print(f"[{self.symbol}] Quick bounce detected (+{points} pts) at {low_price}")
        
        # Strategy 2: Touch of known support with bounce
        if self.strategy_config["enable_support_touch"]:
            nearest_support = self.find_nearest_support(close_price)
            if nearest_support:
                distance_to_support = (low_price - nearest_support) / nearest_support
                if abs(distance_to_support) <= params["support_tolerance"]:
                    bounce_pct = (close_price - low_price) / low_price
                    if bounce_pct >= params["min_bounce_pct"]:
                        points = self.strategy_config["support_touch_points"]
                        signal_score += points
                        signal_reasons.append("SUPPORT_TOUCH")
                        if self.verbose:
                            print(f"[{self.symbol}] Support touch (+{points} pts) at {nearest_support}, bounce: {bounce_pct:.3f}")
        
        # Strategy 3: New local low with recovery
        if self.strategy_config["enable_local_low_recovery"] and len(self.recent_lows) >= 3:
            recent_min = min(list(self.recent_lows)[:-1])
            if low_price < recent_min * 0.998:
                bounce_pct = (close_price - low_price) / low_price
                if bounce_pct >= params["min_bounce_pct"]:
                    points = self.strategy_config["local_low_recovery_points"]
                    signal_score += points
                    signal_reasons.append("LOCAL_LOW_RECOVERY")
                    if self.verbose:
                        print(f"[{self.symbol}] Local low recovery (+{points} pts) at {low_price}")
        
        # Strategy 4: Volume analysis
        if self.strategy_config["enable_volume_analysis"]:
            if volume_analysis["volume_score"] >= 75:
                points = self.strategy_config["volume_spike_points"]
                signal_score += points
                signal_reasons.append("VOLUME_SPIKE")
            elif volume_analysis["volume_score"] >= 50:
                points = self.strategy_config["volume_high_points"]
                signal_score += points
                signal_reasons.append("VOLUME_HIGH")
            elif volume_analysis["volume_score"] >= 25:
                points = self.strategy_config["volume_elevated_points"]
                signal_score += points
                signal_reasons.append("VOLUME_ELEVATED")
        
        # Strategy 5: RSI oversold
        if self.strategy_config["enable_rsi_oversold"]:
            rsi = self.calculate_rsi()
            if rsi and rsi < params["rsi_threshold"]:
                bounce_pct = (close_price - low_price) / low_price
                if bounce_pct >= params["min_bounce_pct"]:
                    points = self.strategy_config["rsi_oversold_points"]
                    signal_score += points
                    signal_reasons.append("RSI_OVERSOLD")
                    if self.verbose:
                        print(f"[{self.symbol}] RSI oversold (+{points} pts) at {rsi:.2f}")
        
        # Strategy 6: Price action strength
        if self.strategy_config["enable_price_action"]:
            strong_threshold = self.strategy_config["price_action_strong_threshold"]
            good_threshold = self.strategy_config["price_action_good_threshold"]
            
            if price_action >= strong_threshold:
                points = self.strategy_config["strong_price_action_points"]
                signal_score += points
                signal_reasons.append("STRONG_PRICE_ACTION")
            elif price_action >= good_threshold:
                points = self.strategy_config["good_price_action_points"]
                signal_score += points
                signal_reasons.append("GOOD_PRICE_ACTION")
        
        # Strategy 7: Market regime adjustment
        if self.strategy_config["enable_market_regime_adjustment"]:
            regime = self.get_market_regime()
            if regime.startswith("BEARISH"):
                points = self.strategy_config["regime_bonus_points"]
                signal_score += points
                signal_reasons.append("BEARISH_BONUS")
            elif regime.startswith("BULLISH"):
                points = self.strategy_config["regime_bonus_points"]
                signal_score -= points  # Penalty in bullish markets
                signal_reasons.append("BULLISH_PENALTY")
        
        # Check if signal meets minimum score requirement
        min_score_required = self.strategy_config["min_signal_score"]
        
        if signal_score >= min_score_required:
            if self.verbose:
                reasons_str = ", ".join(signal_reasons)
                print(f"[{self.symbol}] 🎯 BOTTOM SIGNAL: Score {signal_score}/100 (need {min_score_required})")
                print(f"[{self.symbol}] 📋 Reasons: {reasons_str}")
                print(f"[{self.symbol}] 📊 Market: {self.get_market_regime()}, Volume: {volume_analysis['volume_regime']}")
                print(f"[{self.symbol}] 💹 Price action: {price_action}/100")
            return True
        else:
            # Log near-misses if enabled
            if (self.strategy_config["log_near_misses"] and 
                signal_score >= self.strategy_config["near_miss_threshold"]):
                reasons_str = ", ".join(signal_reasons) if signal_reasons else "NONE"
                print(f"[{self.symbol}] ⚠️ Near miss: Score {signal_score}/{min_score_required}, Reasons: {reasons_str}")
        
        return False

    def calculate_rsi(self) -> Optional[float]:
        """Calculate RSI from price history"""
        if len(self.price_history) < self.rsi_period:
            return None
        
        prices = list(self.price_history)
        deltas = np.diff(prices)
        seed = deltas[:self.rsi_period]
        up = seed[seed >= 0].sum() / self.rsi_period
        down = -seed[seed < 0].sum() / self.rsi_period
        
        if down == 0:
            return 100
        
        rs = up / down
        rsi = 100 - (100 / (1 + rs))
        
        return rsi

    def process_trade(self, trade: Dict) -> Optional[Dict]:
        """Process incoming trade data with 1-minute candles"""
        timestamp = float(trade['timestamp'])
        price = float(trade['price'])
        volume = float(trade['volume'])
        
        # Use 1-minute buckets for faster response
        bucket_ts = math.floor(timestamp / self.candle_period_sec) * self.candle_period_sec
        
        # Check if we need to process the previous candle
        if self.current_bucket is not None and bucket_ts != self.current_bucket:
            # Process completed candle
            old_candle = {
                'timestamp': self.current_bucket + self.candle_period_sec,
                'open': self.current_candle['open'],
                'high': self.current_candle['high'],
                'low': self.current_candle['low'],
                'close': self.current_candle['close'],
                'volume': self.current_candle['volume']
            }
            
            # Update histories
            self.price_history.append(old_candle['close'])
            self.volume_history.append(old_candle['volume'])
            
            # Check for bottom signal
            if not self.is_holding:
                if self.check_bottom_conditions_improved(old_candle):
                    # Enter position with enhanced signal data
                    self.entry_price = old_candle['close']
                    stop_price = old_candle['low'] * (1 - self.stop_loss_pct)
                    self.position_size = self.calculate_position_size(self.entry_price, stop_price)
                    
                    if self.position_size > 0:
                        self.is_holding = True
                        self.hold_start_time = timestamp
                        self.highest_close = self.entry_price
                        self.last_trade_time = timestamp
                        
                        # Enhanced signal data
                        volume_analysis = self.enhanced_volume_analysis(old_candle)
                        price_action = self.price_action_score(old_candle)
                        regime = self.get_market_regime()
                        
                        entry_signal = {
                            "type": "ENTRY",
                            "symbol": self.symbol,
                            "entry_price": self.entry_price,
                            "position_size": self.position_size,
                            "stop_loss": stop_price,
                            "detected_support": self.find_nearest_support(self.entry_price),
                            "candle_low": old_candle['low'],
                            "volume_analysis": volume_analysis,
                            "price_action_score": price_action,
                            "market_regime": regime,
                            "confidence": min(100, (volume_analysis["volume_score"] + price_action) / 2),
                            "timestamp": datetime.datetime.fromtimestamp(timestamp).isoformat()
                        }
                        
                        # Cache the levels after successful entry
                        self.save_levels_to_cache()
                        
                        return entry_signal
            
            # Manage existing position
            elif self.is_holding:
                signal = self.manage_position(old_candle, timestamp)
                if signal:
                    return signal
        
        # Update current candle
        if self.current_bucket is None or bucket_ts != self.current_bucket:
            self.current_bucket = bucket_ts
            self.current_candle = {
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': volume
            }
        else:
            self.current_candle['high'] = max(self.current_candle['high'], price)
            self.current_candle['low'] = min(self.current_candle['low'], price)
            self.current_candle['close'] = price
            self.current_candle['volume'] += volume
        
        return None

    def manage_position(self, candle: Dict, timestamp: float) -> Optional[Dict]:
        """Improved position management"""
        close_price = candle['close']
        low_price = candle['low']
        
        # Update trailing stop
        if close_price > self.highest_close:
            self.highest_close = close_price
        
        # Dynamic exit conditions
        time_in_position = timestamp - self.hold_start_time
        profit_pct = (close_price - self.entry_price) / self.entry_price
        
        exit_signal = None
        exit_reason = ""
        
        # Tighter stop loss
        if low_price <= self.entry_price * (1 - self.stop_loss_pct):
            exit_signal = self.entry_price * (1 - self.stop_loss_pct)
            exit_reason = "STOP_LOSS"
        # Quick profit taking
        elif profit_pct >= 0.005 and time_in_position < 60:  # 0.5% in under 1 minute
            exit_signal = close_price
            exit_reason = "QUICK_PROFIT"
        # Normal target
        elif profit_pct >= self.upper_threshold_pct:
            exit_signal = close_price
            exit_reason = "TARGET"
        # Time exit
        elif time_in_position > self.hold_duration_sec:
            if profit_pct > 0:
                exit_signal = close_price
                exit_reason = "TIME_EXIT_PROFIT"
            elif time_in_position > self.hold_duration_sec * 2:
                exit_signal = close_price
                exit_reason = "TIME_EXIT_TIMEOUT"
        # Trailing stop for profits
        elif self.highest_close > self.entry_price * 1.003:
            trailing_stop = self.highest_close * 0.995
            if low_price <= trailing_stop:
                exit_signal = trailing_stop
                exit_reason = "TRAILING_STOP"
        
        if exit_signal:
            pnl = (exit_signal - self.entry_price) * self.position_size
            self.is_holding = False
            
            return {
                "type": "EXIT",
                "symbol": self.symbol,
                "exit_price": exit_signal,
                "entry_price": self.entry_price,
                "pnl": pnl,
                "exit_reason": exit_reason,
                "timestamp": datetime.datetime.fromtimestamp(timestamp).isoformat()
            }
        
        return None

    def calculate_position_size(self, entry_price: float, stop_price: float) -> int:
        """Calculate position size based on risk"""
        risk_per_share = abs(entry_price - stop_price)
        if risk_per_share == 0:
            return 0
        
        position_size = int(self.risk_per_trade / risk_per_share)
        max_position = int(self.account_size * 0.25 / entry_price)
        
        return min(position_size, max_position)

    def init_database(self):
        """Initialize SQLite database for caching support levels"""
        try:
            with sqlite3.connect(self.db_file) as conn:
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS support_levels (
                        symbol TEXT PRIMARY KEY,
                        levels TEXT,
                        timestamp REAL
                    )
                ''')
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT,
                        entry_price REAL,
                        exit_price REAL,
                        position_size INTEGER,
                        pnl REAL,
                        entry_time REAL,
                        exit_time REAL,
                        exit_reason TEXT
                    )
                ''')
        except Exception as e:
            if self.verbose:
                print(f"Database init error: {e}")

    def load_cached_levels(self):
        """Load cached support levels if recent"""
        try:
            with sqlite3.connect(self.db_file) as conn:
                cursor = conn.execute(
                    'SELECT levels, timestamp FROM support_levels WHERE symbol = ?',
                    (self.symbol,)
                )
                row = cursor.fetchone()
                if row and time.time() - row[1] < 3600:  # 1 hour cache
                    self.support_levels = json.loads(row[0])
                    if self.verbose:
                        print(f"[{self.symbol}] Loaded cached support levels: {self.support_levels}")
        except Exception as e:
            if self.verbose:
                print(f"Cache load error: {e}")

    def save_levels_to_cache(self):
        """Save support levels to cache"""
        try:
            with sqlite3.connect(self.db_file) as conn:
                conn.execute(
                    'INSERT OR REPLACE INTO support_levels (symbol, levels, timestamp) VALUES (?, ?, ?)',
                    (self.symbol, json.dumps(self.support_levels), time.time())
                )
        except Exception as e:
            if self.verbose:
                print(f"Cache save error: {e}")

    def log_trade(self, entry_data: Dict, exit_data: Dict = None):
        """Log trade to database for analysis"""
        try:
            with sqlite3.connect(self.db_file) as conn:
                if exit_data:
                    conn.execute('''
                        INSERT INTO trades (symbol, entry_price, exit_price, position_size, pnl, 
                                          entry_time, exit_time, exit_reason) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        self.symbol,
                        entry_data['entry_price'],
                        exit_data['exit_price'],
                        entry_data['position_size'],
                        exit_data['pnl'],
                        time.time(),
                        time.time(),
                        exit_data['exit_reason']
                    ))
        except Exception as e:
            if self.verbose:
                print(f"Trade log error: {e}")

    def get_market_regime(self) -> str:
        """Determine current market regime for adaptive parameters"""
        if len(self.price_history) < 20:
            return "UNKNOWN"
        
        prices = list(self.price_history)
        short_ma = sum(prices[-5:]) / 5
        long_ma = sum(prices[-20:]) / 20
        
        recent_volatility = np.std(prices[-10:]) / np.mean(prices[-10:])
        
        if short_ma > long_ma * 1.01:
            return "BULLISH_HIGH_VOL" if recent_volatility > 0.02 else "BULLISH"
        elif short_ma < long_ma * 0.99:
            return "BEARISH_HIGH_VOL" if recent_volatility > 0.02 else "BEARISH"
        else:
            return "SIDEWAYS_HIGH_VOL" if recent_volatility > 0.02 else "SIDEWAYS"

    def adaptive_parameters(self) -> Dict:
        """Adjust parameters based on market regime"""
        regime = self.get_market_regime()
        
        params = {
            "min_bounce_pct": 0.001,
            "volume_threshold": self.volume_spike_threshold,
            "rsi_threshold": 40,
            "support_tolerance": 0.005
        }
        
        if regime.startswith("BEARISH"):
            # More aggressive in bearish markets
            params["min_bounce_pct"] = 0.0005
            params["rsi_threshold"] = 45
            params["support_tolerance"] = 0.007
        elif regime.startswith("BULLISH"):
            # More selective in bullish markets
            params["min_bounce_pct"] = 0.002
            params["rsi_threshold"] = 35
        elif "HIGH_VOL" in regime:
            # Require stronger signals in high volatility
            params["min_bounce_pct"] = 0.003
            params["volume_threshold"] = 1.5
        
        return params

    def enhanced_volume_analysis(self, candle: Dict) -> Dict:
        """Enhanced volume analysis with multiple metrics"""
        volume = candle['volume']
        if not self.volume_history:
            return {"volume_score": 0, "volume_regime": "UNKNOWN"}
        
        avg_volume = sum(self.volume_history) / len(self.volume_history)
        volume_percentile = sum(1 for v in self.volume_history if v < volume) / len(self.volume_history)
        
        # Recent volume trend
        recent_volumes = list(self.volume_history)[-5:]
        volume_trend = "INCREASING" if len(recent_volumes) >= 3 and recent_volumes[-1] > recent_volumes[0] else "NORMAL"
        
        volume_score = 0
        if volume > avg_volume * 2:
            volume_score = 100
        elif volume > avg_volume * 1.5:
            volume_score = 75
        elif volume > avg_volume * 1.2:
            volume_score = 50
        elif volume_percentile > 0.8:
            volume_score = 25
        
        return {
            "volume_score": volume_score,
            "volume_regime": "HIGH" if volume_score >= 75 else "NORMAL",
            "volume_trend": volume_trend,
            "volume_percentile": volume_percentile
        }

    def price_action_score(self, candle: Dict) -> int:
        """Score price action strength (0-100)"""
        open_price = candle['open']
        high_price = candle['high']
        low_price = candle['low']
        close_price = candle['close']
        
        score = 0
        
        # Body vs wick analysis
        body = abs(close_price - open_price)
        total_range = high_price - low_price
        
        if total_range > 0:
            body_ratio = body / total_range
            lower_wick = min(open_price, close_price) - low_price
            upper_wick = high_price - max(open_price, close_price)
            
            # Hammer pattern (long lower wick, small body)
            if lower_wick > body * 2 and upper_wick < body:
                score += 40
            
            # Strong close (close near high)
            if (close_price - low_price) / total_range > 0.7:
                score += 30
            
            # Large body (decisive move)
            if body_ratio > 0.6:
                score += 20
            
            # Gap up close
            if close_price > open_price:
                score += 10
        
        return min(score, 100)

    @property
    def rsi_period(self) -> int:
        """Dynamic RSI period based on market conditions"""
        regime = self.get_market_regime()
        if "HIGH_VOL" in regime:
            return 10  # Faster RSI in volatile markets
        else:
            return 14  # Standard RSI

    def is_market_hours(self) -> bool:
        """Check if within market hours"""
        now = datetime.datetime.now(self.timezone)
        market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
        
        # Only trade during market hours on weekdays
        if now.weekday() >= 5:  # Weekend
            return False
        
        return market_open <= now <= market_close

    def should_trade(self) -> bool:
        """Pre-trade filters based on configuration"""
        # Check daily loss limit if enabled
        if self.strategy_config["enable_daily_loss_limit"]:
            if self.daily_pnl <= -self.account_size * self.max_daily_loss:
                if self.verbose:
                    print(f"[{self.symbol}] Daily loss limit reached: {self.daily_pnl}")
                return False
        
        # Check trade spacing if enabled
        if self.strategy_config["enable_trade_spacing"]:
            min_spacing = self.strategy_config["min_trade_spacing_sec"]
            if self.last_trade_time and time.time() - self.last_trade_time < min_spacing:
                if self.verbose:
                    remaining = min_spacing - (time.time() - self.last_trade_time)
                    print(f"[{self.symbol}] Trade spacing: {remaining:.0f}s remaining")
                return False
        
        return True

# Backward compatibility alias
BottomSignalDetector = ImprovedBottomSignalDetector
