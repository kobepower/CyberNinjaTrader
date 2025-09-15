# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture Overview

This is a sophisticated financial trading application called "CyberNinja Trader Pro" with real-time market analysis, signal detection, and strategy execution capabilities. The application uses a multi-threaded PyQt5 GUI with RabbitMQ message queuing for real-time data processing.

### Core Components

**Main Application (`CyberNinjaTrader.py`)**
- Enhanced PyQt5 GUI with tabbed interface (Main, Reversal Radar, RabbitMQ Monitor, Strategy Engine, Performance)
- Resource management with circuit breakers, caching, and performance monitoring
- Real-time data fetching from Yahoo Finance and external APIs
- Multi-threaded architecture with signal/slot communication

**Backend Services (`backend/`)**
- `main.py` - Backend entry point with signal logging
- `utils/rabbitmq_api.py` - Comprehensive async RabbitMQ API with connection management
- `utils/bottom_signal_detector.py` - Advanced reversal detection with RSI, volume analysis
- `utils/reversal_sniper.py` - Enhanced reversal detection system with candlestick patterns
- `strategies/` - Trading strategy implementations and consumers

**GUI Components (`gui/`)**
- `reversal_radar.py` - Real-time reversal signal monitoring
- `strategy_engine_tab.py` - Strategy engine control with monitoring and analytics

**Data Processing**
- `generate_spreadsheet.py` - Advanced Excel export with market analysis and statistics
- Multiple `watchlist.txt` files for ticker management
- SQLite database for trade logging and caching (`support_cache.db`)

## Key Features

### Trading Signal Detection
- **Bottom Signal Detection**: RSI-based oversold conditions, volume confirmation, support level testing
- **Reversal Pattern Recognition**: Candlestick patterns (hammer, doji, engulfing patterns)
- **Multi-timeframe Analysis**: 5-minute candles with daily support/resistance levels
- **Volume Analysis**: Above-average volume confirmation for signal validity

### Risk Management
- Position sizing based on risk per trade (default 0.5% of account)
- Stop-loss management with trailing stops
- Daily loss limits (2% max daily loss)
- Maximum concurrent positions (default 3)

### Real-time Data Processing
- **RabbitMQ Integration**: Async message queuing for price feeds and signals
- **WebSocket Support**: Finnhub API integration for real-time market data
- **Caching System**: TTL-based caching with automatic cleanup
- **Circuit Breaker Pattern**: Connection resilience with retry logic

## Development Commands

### Running the Application
```bash
# Main GUI application
python CyberNinjaTrader.py

# Backend signal processing
python backend/main.py

# Strategy engine (if available)
python -m backend.app.strategies.strategy_consumer

# Generate market analysis spreadsheet
python generate_spreadsheet.py
```

### Testing and Development
```bash
# Test RabbitMQ connectivity
python backend/app/utils/rabbitmq_api.py

# Test bottom signal detection
python backend/app/utils/bottom_signal_detector.py --symbols AAPL NVDA MSFT

# Test reversal detection
python backend/app/utils/reversal_sniper.py
```

## Configuration

The application looks for configuration in multiple locations:
- `shared/config.json` (primary)
- `config.json` (fallback)
- Environment variables for API keys

### Required Configuration Structure
```json
{
  "rabbitmq": {
    "host": "localhost",
    "price_queue": "price_queue",
    "signal_queue": "signal_queue"
  },
  "tickers": ["AMD", "NVDA", "AMZN", "META", "TSLA", "PLTR", "AAPL"],
  "thresholds": {
    "volume_high": 150,
    "support_breach": 0.002,
    "resistance_near": 0.005
  }
}
```

## Dependencies

**Core Libraries:**
- `PyQt5` - GUI framework
- `yfinance` - Yahoo Finance API
- `pandas`, `numpy` - Data analysis
- `aio_pika` - Async RabbitMQ client
- `requests` - HTTP requests
- `websocket-client` - WebSocket connections

**Technical Analysis:**
- `pandas_ta` - Technical indicators
- Custom RSI, MACD, support/resistance calculations

## Signal Processing Architecture

### Signal Types
- `REVERSAL` - Price reversal at support/resistance
- `BOTTOM` - Bottom detection with volume confirmation  
- `BREAKOUT` - Price breakout above resistance
- `VOLUME_SPIKE` - Unusual volume activity
- `TEST` - Test signals for debugging

### Message Flow
1. **Market Data** → RabbitMQ Price Queue
2. **Signal Detection** → RabbitMQ Signal Queue
3. **Strategy Engine** → Position Management
4. **GUI Updates** → Real-time display

## Database Schema

### Trade Logging (`trade_logs.json`, SQLite)
```sql
CREATE TABLE trades (
    id INTEGER PRIMARY KEY,
    symbol TEXT,
    entry_price REAL,
    exit_price REAL,
    position_size INTEGER,
    pnl REAL,
    timestamp REAL
);

CREATE TABLE levels (
    symbol TEXT PRIMARY KEY,
    support REAL,
    resistance REAL,
    timestamp REAL
);
```

## Performance Monitoring

The application includes comprehensive performance monitoring:
- **Message Processing Time** - Signal handling latency
- **API Response Time** - External API call performance  
- **UI Update Latency** - GUI responsiveness
- **Memory Usage** - Resource consumption tracking
- **Thread Count** - Concurrency monitoring

## Error Handling

- Global exception handler with crash logging to `~/.cyberninja_trader/crash.log`
- Graceful degradation when external services unavailable
- Automatic retry with exponential backoff
- Circuit breaker pattern for external API calls

## Trading Hours

Optimal signal detection during:
- **Morning Session**: 9:30-11:30 AM ET
- **Afternoon Session**: 2:30-3:30 PM ET

## Important Notes

- The application uses real financial data and should be used responsibly
- All trading signals are for analysis purposes
- Position sizing assumes a $60,000 account by default
- Stop-loss is set at 0.3% initially with trailing stop logic
- Maximum daily loss limit is 2% of account size

## Watchlist Management

Ticker symbols are loaded from multiple locations:
- `backend/app/data/watchlist.txt`
- `app/data/watchlist.txt`
- `watchlist.txt`

Format: `TICKER:priority:notes` (priority and notes optional)