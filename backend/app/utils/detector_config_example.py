#!/usr/bin/env python3
"""
Example script showing how to configure the Bottom Signal Detector
Users can customize which strategies to enable and their parameters
"""

from .bottom_signal_detector import ImprovedBottomSignalDetector

def example_conservative_config():
    """Conservative configuration - fewer signals, higher quality"""
    return {
        # Enable only the most reliable strategies
        "enable_quick_bounce": True,
        "enable_support_touch": True,
        "enable_local_low_recovery": False,  # Disabled - can be noisy
        "enable_volume_analysis": True,
        "enable_rsi_oversold": True,
        "enable_price_action": True,
        "enable_market_regime_adjustment": True,
        
        # Higher point values for enabled strategies
        "quick_bounce_points": 35,
        "support_touch_points": 30,
        "volume_spike_points": 30,
        "rsi_oversold_points": 20,
        "strong_price_action_points": 20,
        
        # Higher minimum score required
        "min_signal_score": 70,  # More selective
        
        # Stricter parameters
        "rsi_threshold": 35,  # Lower RSI threshold
        "min_bounce_pct": 0.002,  # Larger bounce required
        "support_tolerance": 0.003,  # Closer to support required
        
        # Conservative settings
        "log_near_misses": False,  # Less noise
        "min_trade_spacing_sec": 300  # 5 minutes between trades
    }

def example_aggressive_config():
    """Aggressive configuration - more signals, higher frequency"""
    return {
        # Enable all strategies
        "enable_quick_bounce": True,
        "enable_support_touch": True,
        "enable_local_low_recovery": True,
        "enable_volume_analysis": True,
        "enable_rsi_oversold": True,
        "enable_price_action": True,
        "enable_market_regime_adjustment": True,
        
        # Lower point values to trigger more easily
        "quick_bounce_points": 25,
        "support_touch_points": 20,
        "local_low_recovery_points": 15,
        "volume_elevated_points": 15,  # Even small volume gets points
        "rsi_oversold_points": 10,
        
        # Lower minimum score
        "min_signal_score": 35,  # Easier to trigger
        
        # More relaxed parameters
        "rsi_threshold": 45,  # Higher RSI threshold
        "min_bounce_pct": 0.0005,  # Smaller bounce accepted
        "support_tolerance": 0.008,  # Further from support OK
        
        # Aggressive settings
        "log_near_misses": True,
        "near_miss_threshold": 20,
        "min_trade_spacing_sec": 30  # 30 seconds between trades
    }

def example_volume_focused_config():
    """Volume-focused configuration - prioritizes volume signals"""
    return {
        # Focus on volume and support strategies
        "enable_quick_bounce": False,  # Disabled
        "enable_support_touch": True,
        "enable_local_low_recovery": False,  # Disabled
        "enable_volume_analysis": True,  # Main focus
        "enable_rsi_oversold": False,  # Disabled
        "enable_price_action": True,
        "enable_market_regime_adjustment": True,
        
        # Heavy weighting on volume
        "volume_spike_points": 40,  # Very high
        "volume_high_points": 25,
        "volume_elevated_points": 15,
        "support_touch_points": 20,
        "strong_price_action_points": 10,
        
        # Medium selectivity
        "min_signal_score": 50,
        
        # Volume-specific parameters
        "volume_spike_threshold": 1.1,  # Lower threshold for volume spikes
        "volume_spike_threshold_high_vol": 1.3
    }

def example_rsi_only_config():
    """RSI-only configuration - classic oversold strategy"""
    return {
        # Only RSI strategy enabled
        "enable_quick_bounce": False,
        "enable_support_touch": False,
        "enable_local_low_recovery": False,
        "enable_volume_analysis": False,
        "enable_rsi_oversold": True,  # Only this enabled
        "enable_price_action": False,
        "enable_market_regime_adjustment": True,  # Keep for adaptive RSI
        
        # All points from RSI
        "rsi_oversold_points": 60,  # High value
        "regime_bonus_points": 10,
        
        # Low minimum since only one strategy
        "min_signal_score": 40,
        
        # RSI-specific parameters
        "rsi_threshold": 30,  # Very oversold
        "rsi_threshold_bearish": 35,
        "rsi_threshold_bullish": 25,
        "min_bounce_pct": 0.001
    }

def main():
    """Demonstrate different configurations"""
    
    # Example 1: Conservative setup
    print("=== Conservative Configuration ===")
    conservative_detector = ImprovedBottomSignalDetector(
        symbol="NVDA",
        strategy_config=example_conservative_config(),
        verbose=True
    )
    conservative_detector.print_current_config()
    
    # Example 2: Aggressive setup
    print("\n=== Aggressive Configuration ===")
    aggressive_detector = ImprovedBottomSignalDetector(
        symbol="TSLA",
        strategy_config=example_aggressive_config(),
        verbose=True
    )
    aggressive_detector.print_current_config()
    
    # Example 3: Custom configuration via updates
    print("\n=== Custom Configuration (via updates) ===")
    custom_detector = ImprovedBottomSignalDetector(symbol="AAPL", verbose=True)
    
    # Start with defaults, then customize
    custom_updates = {
        "enable_volume_analysis": False,  # Disable volume
        "enable_rsi_oversold": False,     # Disable RSI
        "min_signal_score": 40,           # Lower threshold
        "quick_bounce_points": 50,        # Focus on quick bounces
        "support_touch_points": 50
    }
    
    custom_detector.update_strategy_config(custom_updates)
    custom_detector.print_current_config()
    
    # Example 4: Save and load configuration
    print("\n=== Save/Load Configuration ===")
    config_filename = "my_strategy_config.json"
    
    # Save aggressive config
    aggressive_detector.save_strategy_config_to_file(config_filename)
    print(f"Configuration saved to {config_filename}")
    
    # Load it into a new detector
    loaded_detector = ImprovedBottomSignalDetector(symbol="AMD", verbose=True)
    loaded_detector.load_strategy_config_from_file(config_filename)
    print("Configuration loaded from file")
    
    # Example 5: JSON configuration
    print("\n=== JSON Configuration ===")
    json_config = '''
    {
        "enable_quick_bounce": true,
        "enable_support_touch": false,
        "enable_volume_analysis": true,
        "min_signal_score": 60,
        "quick_bounce_points": 40,
        "volume_spike_points": 40,
        "log_near_misses": true
    }
    '''
    
    json_detector = ImprovedBottomSignalDetector(symbol="META", verbose=True)
    json_detector.load_strategy_config_from_json(json_config)
    print("Custom JSON configuration loaded")
    json_detector.print_current_config()

if __name__ == "__main__":
    main()