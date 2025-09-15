import sys
import json
import time
import logging
import asyncio
from pathlib import Path
from datetime import datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Optional, List, Tuple, Any

import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.formatting.rule import ColorScaleRule

# Optional dependencies with graceful fallbacks
try:
    import aio_pika
except ImportError:
    aio_pika = None
    logging.warning("aio_pika not available; RabbitMQ fetching disabled")

try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
except ImportError:
    yf = None
    pd = None
    np = None
    logging.warning("yfinance/pandas/numpy not available; advanced features disabled")

# ───────────────────────────── Configuration ─────────────────────────────
CACHE_TIMEOUT = 300  # 5 minutes
MAX_RETRIES = 3
RETRY_DELAY = 2
RABBITMQ_TIMEOUT = 3.0
MAX_TICKERS = 100  # Increased from 50
RSI_OVERSOLD = 30
RSI_OVERBOUGHT = 70

# Cache for market data
market_cache: Dict[str, Tuple[Dict, float]] = {}

# ───────────────────────────── Logging Setup ─────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("StockTracker")

# ───────────────────────────── Config Management ─────────────────────────────
def load_config(config_path: Path) -> dict:
    """Load configuration with comprehensive defaults matching your system."""
    default_config = {
        "rabbitmq": {
            "host": "localhost",
            "price_queue": "price_queue",
            "signal_queue": "signal_queue"
        },
        "tickers": [],  # Will load from watchlist if empty
        "thresholds": {
            "volume_high": 150,  # 150% of average
            "volume_normal": 80,  # 80% of average
            "support_breach": 0.002,  # 0.2% below support
            "resistance_near": 0.005  # 0.5% from resistance
        },
        "display": {
            "max_rows": 100,
            "refresh_interval": 60,
            "show_signals": True,
            "show_rsi": True
        }
    }
    
    try:
        if config_path.exists():
            with config_path.open("r", encoding="utf-8") as f:
                config = json.load(f)
            log.info(f"Loaded config from {config_path}")
            # Deep merge configurations
            return deep_merge(default_config, config)
        else:
            log.warning(f"Config not found at {config_path}, using defaults")
            return default_config
    except json.JSONDecodeError as e:
        log.error(f"Invalid JSON in config: {e}")
        return default_config

def deep_merge(base: dict, update: dict) -> dict:
    """Deep merge two dictionaries."""
    result = base.copy()
    for key, value in update.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result

def load_watchlist_tickers() -> List[str]:
    """Load tickers from standardized shared watchlist JSON."""
    try:
        # Import watchlist manager
        from watchlist_utils import SharedWatchlistManager
        watchlist_manager = SharedWatchlistManager()
        tickers = watchlist_manager.get_all_tickers(enabled_only=True)
        
        if tickers:
            log.info(f"Loaded {len(tickers)} tickers from shared watchlist")
            return tickers
    except Exception as e:
        log.error(f"Error loading from watchlist manager: {e}")
    
    # Fallback to default tickers
    fallback_tickers = ["AAPL", "NVDA", "AMZN", "META", "TSLA", "PLTR", "AMD", "GOOGL"]
    log.warning(f"Using fallback tickers: {fallback_tickers}")
    return fallback_tickers
    
    # This should not be reached due to the fallback above
    return fallback_tickers

# ───────────────────────────── Market Analysis ─────────────────────────────
def session_status_now() -> Tuple[str, str]:
    """
    Enhanced market session detection.
    Returns: (status_color, session_name)
    """
    now_et = datetime.now(ZoneInfo("America/New_York"))
    
    if now_et.weekday() >= 5:
        return ("Red", "Weekend")
    
    t = now_et.time()
    
    # More granular session detection
    if dtime(4, 0) <= t < dtime(9, 30):
        return ("Yellow", "Pre-Market")
    elif dtime(9, 30) <= t < dtime(10, 0):
        return ("Green", "Opening")
    elif dtime(10, 0) <= t < dtime(15, 30):
        return ("Green", "Regular")
    elif dtime(15, 30) <= t <= dtime(16, 0):
        return ("Yellow", "Closing")
    elif dtime(16, 0) < t <= dtime(20, 0):
        return ("Yellow", "After-Hours")
    else:
        return ("Red", "Closed")

def calculate_rsi(prices: List[float], period: int = 14) -> Optional[float]:
    """Calculate RSI matching your reversal detection logic."""
    if not prices or len(prices) < period + 1:
        return None
    
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    
    if avg_loss == 0:
        return 100.0
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return round(rsi, 2)

# ───────────────────────────── Data Fetching ─────────────────────────────
async def fetch_from_rabbitmq(host: str, queue_name: str, want_tickers: List[str], 
                             timeout_sec: float = RABBITMQ_TIMEOUT) -> Dict[str, Dict]:
    """Enhanced RabbitMQ fetching with signal queue support."""
    if aio_pika is None:
        return {}
    
    results = {}
    signal_data = {}
    
    try:
        connection = await asyncio.wait_for(
            aio_pika.connect_robust(f"amqp://guest:guest@{host}:5672/"),
            timeout=2
        )
        
        async with connection:
            channel = await connection.channel()
            
            # Fetch from price queue
            try:
                price_queue = await channel.declare_queue(queue_name, durable=True)
                
                # Non-blocking message collection
                messages_processed = 0
                max_messages = 1000  # Process up to 1000 messages
                
                while messages_processed < max_messages:
                    message = await price_queue.get(no_ack=True, fail=False)
                    if message is None:
                        break
                    
                    try:
                        payload = json.loads(message.body.decode("utf-8"))
                        ticker = payload.get("ticker", "").upper()
                        
                        if ticker in want_tickers:
                            results[ticker] = {
                                "price": payload.get("price"),
                                "volume": payload.get("volume", 0),
                                "bid": payload.get("bid"),
                                "ask": payload.get("ask"),
                                "timestamp": payload.get("timestamp", datetime.now().isoformat())
                            }
                    except Exception as e:
                        log.debug(f"Error processing message: {e}")
                    
                    messages_processed += 1
                
            except Exception as e:
                log.error(f"Price queue error: {e}")
            
            # Also check signal queue for recent signals
            try:
                signal_queue = await channel.declare_queue("signal_queue", durable=True)
                
                messages_processed = 0
                while messages_processed < 100:  # Fewer signal messages expected
                    message = await signal_queue.get(no_ack=True, fail=False)
                    if message is None:
                        break
                    
                    try:
                        payload = json.loads(message.body.decode("utf-8"))
                        ticker = payload.get("ticker", "").upper()
                        
                        if ticker in want_tickers:
                            signal_data[ticker] = {
                                "type": payload.get("type", payload.get("signal_type")),
                                "action": payload.get("action"),
                                "confidence": payload.get("confidence", 0),
                                "timestamp": payload.get("timestamp")
                            }
                    except Exception:
                        pass
                    
                    messages_processed += 1
                    
            except Exception:
                pass  # Signal queue is optional
                
    except asyncio.TimeoutError:
        log.warning("RabbitMQ connection timeout")
    except Exception as e:
        log.error(f"RabbitMQ error: {e}")
    
    # Merge signal data into results
    for ticker, sig in signal_data.items():
        if ticker in results:
            results[ticker]["signal"] = sig
        else:
            results[ticker] = {"signal": sig}
    
    return results

def fetch_yfinance_data(tickers: List[str]) -> Dict[str, Optional[Dict]]:
    """Enhanced yfinance fetching with technical indicators."""
    if yf is None or pd is None:
        return {t: None for t in tickers}
    
    results = {}
    current_time = time.time()
    
    # Check cache
    for ticker in tickers[:]:
        if ticker in market_cache:
            data, timestamp = market_cache[ticker]
            if current_time - timestamp < CACHE_TIMEOUT:
                results[ticker] = data
                tickers.remove(ticker)
    
    if not tickers:
        return results
    
    # Batch fetch remaining
    for attempt in range(MAX_RETRIES):
        try:
            # Create session with headers
            session = yf.Session(headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124'
            })
            
            for ticker in tickers:
                try:
                    stock = yf.Ticker(ticker, session=session)
                    
                    # Get today's data
                    today_data = stock.history(period="1d", interval="1m", prepost=True)
                    
                    # Get historical data for support/resistance
                    hist_data = stock.history(period="10d", interval="1d")
                    
                    if today_data.empty and hist_data.empty:
                        results[ticker] = None
                        continue
                    
                    result = {
                        "current_price": None,
                        "current_low": None,
                        "current_high": None,
                        "volume": None,
                        "avg_volume": None,
                        "support_level": None,
                        "resistance_level": None,
                        "rsi": None,
                        "price_change": None,
                        "price_change_pct": None
                    }
                    
                    # Today's data
                    if not today_data.empty:
                        last = today_data.iloc[-1]
                        result["current_price"] = float(last["Close"])
                        result["current_low"] = float(today_data["Low"].min())
                        result["current_high"] = float(today_data["High"].max())
                        result["volume"] = float(last["Volume"])
                        result["avg_volume"] = float(today_data["Volume"].mean())
                    
                    # Historical analysis
                    if len(hist_data) >= 2:
                        # Previous day support (low)
                        result["support_level"] = float(hist_data["Low"].iloc[-2])
                        
                        # Resistance from recent highs
                        result["resistance_level"] = float(hist_data["High"].tail(5).max())
                        
                        # Price change
                        prev_close = float(hist_data["Close"].iloc[-2])
                        if result["current_price"] and prev_close:
                            result["price_change"] = result["current_price"] - prev_close
                            result["price_change_pct"] = (result["price_change"] / prev_close) * 100
                        
                        # Calculate RSI
                        if len(hist_data) >= 15:
                            closes = hist_data["Close"].values.tolist()
                            if not today_data.empty:
                                closes.append(result["current_price"])
                            result["rsi"] = calculate_rsi(closes)
                    
                    # Cache the result
                    market_cache[ticker] = (result, current_time)
                    results[ticker] = result
                    
                except Exception as e:
                    log.warning(f"Error fetching {ticker}: {e}")
                    results[ticker] = None
            
            break  # Success
            
        except Exception as e:
            if attempt + 1 == MAX_RETRIES:
                log.error(f"yfinance failed after {MAX_RETRIES} attempts: {e}")
                for ticker in tickers:
                    if ticker not in results:
                        results[ticker] = None
            else:
                time.sleep(RETRY_DELAY * (2 ** attempt))
    
    return results

# ───────────────────────────── Signal Analysis ─────────────────────────────
def analyze_ticker_status(data: Dict[str, Any], config: Dict) -> Dict[str, str]:
    """Comprehensive ticker analysis matching your signal detection logic."""
    thresholds = config.get("thresholds", {})
    
    status = {
        "volume": "Unknown",
        "support": "Green",
        "resistance": "Green",
        "rsi": "Yellow",
        "momentum": "Yellow",
        "signal": "None",
        "overall": "Yellow"
    }
    
    # Volume analysis
    if data.get("volume") and data.get("avg_volume"):
        ratio = (data["volume"] / data["avg_volume"]) * 100
        if ratio >= thresholds.get("volume_high", 150):
            status["volume"] = "Green"
        elif ratio >= thresholds.get("volume_normal", 80):
            status["volume"] = "Yellow"
        else:
            status["volume"] = "Red"
    
    # Support breach detection
    if data.get("current_low") and data.get("support_level"):
        breach_threshold = 1 - thresholds.get("support_breach", 0.002)
        if data["current_low"] <= data["support_level"] * breach_threshold:
            status["support"] = "Red"
        elif data["current_low"] <= data["support_level"] * 1.01:
            status["support"] = "Yellow"
    
    # Resistance proximity
    if data.get("current_price") and data.get("resistance_level"):
        near_threshold = 1 - thresholds.get("resistance_near", 0.005)
        if data["current_price"] >= data["resistance_level"] * near_threshold:
            status["resistance"] = "Yellow"
    
    # RSI analysis
    if data.get("rsi"):
        rsi = data["rsi"]
        if rsi <= RSI_OVERSOLD:
            status["rsi"] = "Green"  # Oversold = potential buy
        elif rsi >= RSI_OVERBOUGHT:
            status["rsi"] = "Red"  # Overbought = potential sell
    
    # Momentum from price change
    if data.get("price_change_pct"):
        change = data["price_change_pct"]
        if change > 2:
            status["momentum"] = "Green"
        elif change < -2:
            status["momentum"] = "Red"
    
    # Signal status
    if "signal" in data and data["signal"]:
        sig = data["signal"]
        if sig.get("type") in ["REVERSAL", "BOTTOM"]:
            status["signal"] = "Green"
        elif sig.get("confidence", 0) > 80:
            status["signal"] = "Yellow"
    
    # Overall status calculation
    critical_statuses = [status["support"], status["volume"], status["rsi"]]
    if any(s == "Red" for s in critical_statuses):
        status["overall"] = "Red"
    elif any(s == "Green" for s in critical_statuses) and status["signal"] == "Green":
        status["overall"] = "Green"
    
    return status

# ───────────────────────────── Spreadsheet Generation ─────────────────────────────
def create_enhanced_spreadsheet(data: Dict, config: Dict, output_file: Path):
    """Create feature-rich Excel with multiple sheets and advanced formatting."""
    wb = openpyxl.Workbook()
    
    # Main tracking sheet
    ws_main = wb.active
    ws_main.title = "Live Tracker"
    
    # Signal sheet
    ws_signals = wb.create_sheet("Signals")
    
    # Statistics sheet
    ws_stats = wb.create_sheet("Statistics")
    
    # Styles
    header_font = Font(bold=True, size=12, color="FFFFFF")
    header_fill = PatternFill("solid", fgColor="2E4057")
    header_align = Alignment(horizontal="center", vertical="center")
    
    status_fills = {
        "Green": PatternFill("solid", fgColor="90EE90"),
        "Yellow": PatternFill("solid", fgColor="FFFFE0"),
        "Red": PatternFill("solid", fgColor="FFB6C1"),
        "Unknown": PatternFill("solid", fgColor="D3D3D3")
    }
    
    thin_border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    
    # Main sheet headers
    main_headers = [
        "Symbol", "Price", "Change %", "Volume", "Avg Volume", 
        "Support", "Resistance", "RSI", "Signal",
        "Vol Status", "Sup Status", "RSI Status", "Overall"
    ]
    
    for c, header in enumerate(main_headers, 1):
        cell = ws_main.cell(row=1, column=c, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_align
        cell.border = thin_border
    
    # Populate main sheet
    row_idx = 2
    signal_count = 0
    
    for ticker, ticker_data in sorted(data.items()):
        if not ticker_data:
            continue
        
        # Analyze status
        status = analyze_ticker_status(ticker_data, config)
        
        # Write data
        ws_main.cell(row=row_idx, column=1, value=ticker).font = Font(bold=True)
        
        # Price data
        price = ticker_data.get("current_price") or ticker_data.get("price")
        ws_main.cell(row=row_idx, column=2, value=price or "N/A")
        
        # Change percentage with color
        change_cell = ws_main.cell(row=row_idx, column=3)
        if ticker_data.get("price_change_pct"):
            change_val = round(ticker_data["price_change_pct"], 2)
            change_cell.value = f"{change_val:+.2f}%"
            if change_val > 0:
                change_cell.font = Font(color="008000")
            elif change_val < 0:
                change_cell.font = Font(color="FF0000")
        
        # Volume with formatting
        vol = ticker_data.get("volume")
        if vol:
            ws_main.cell(row=row_idx, column=4, value=int(vol)).number_format = "#,##0"
        
        avg_vol = ticker_data.get("avg_volume")
        if avg_vol:
            ws_main.cell(row=row_idx, column=5, value=int(avg_vol)).number_format = "#,##0"
        
        # Support/Resistance
        ws_main.cell(row=row_idx, column=6, value=ticker_data.get("support_level") or "N/A")
        ws_main.cell(row=row_idx, column=7, value=ticker_data.get("resistance_level") or "N/A")
        
        # RSI with color coding
        rsi_cell = ws_main.cell(row=row_idx, column=8)
        if ticker_data.get("rsi"):
            rsi_val = ticker_data["rsi"]
            rsi_cell.value = rsi_val
            if rsi_val <= RSI_OVERSOLD:
                rsi_cell.font = Font(color="008000", bold=True)
            elif rsi_val >= RSI_OVERBOUGHT:
                rsi_cell.font = Font(color="FF0000", bold=True)
        
        # Signal indicator
        signal_cell = ws_main.cell(row=row_idx, column=9)
        if ticker_data.get("signal"):
            sig = ticker_data["signal"]
            signal_cell.value = sig.get("type", "")
            signal_cell.font = Font(color="0000FF", bold=True)
            signal_count += 1
        
        # Status columns with color fills
        for col, status_key in enumerate(["volume", "support", "rsi", "overall"], 10):
            cell = ws_main.cell(row=row_idx, column=col, value=status[status_key])
            cell.fill = status_fills.get(status[status_key], status_fills["Unknown"])
            cell.alignment = Alignment(horizontal="center")
            cell.border = thin_border
        
        row_idx += 1
    
    # Adjust column widths
    for column in ws_main.columns:
        max_length = max(len(str(cell.value or "")) for cell in column)
        ws_main.column_dimensions[column[0].column_letter].width = min(max_length + 2, 20)
    
    # Freeze panes
    ws_main.freeze_panes = "A2"
    
    # Add conditional formatting for price column
    ws_main.conditional_formatting.add(
        f'B2:B{row_idx}',
        ColorScaleRule(
            start_type='min', start_color='FF0000',
            mid_type='percentile', mid_value=50, mid_color='FFFF00',
            end_type='max', end_color='00FF00'
        )
    )
    
    # Signal sheet
    signal_headers = ["Time", "Ticker", "Type", "Action", "Confidence", "Price"]
    for c, header in enumerate(signal_headers, 1):
        cell = ws_signals.cell(row=1, column=c, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_align
    
    # Statistics sheet
    stats_data = [
        ["Metric", "Value"],
        ["Total Tickers", len(data)],
        ["Active Signals", signal_count],
        ["Market Status", session_status_now()[1]],
        ["Last Update", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
    ]
    
    for r, row_data in enumerate(stats_data, 1):
        for c, value in enumerate(row_data, 1):
            cell = ws_stats.cell(row=r, column=c, value=value)
            if r == 1:
                cell.font = header_font
                cell.fill = header_fill
            else:
                cell.border = thin_border
    
    # Save workbook
    wb.save(output_file)
    log.info(f"Spreadsheet saved: {output_file}")

# ───────────────────────────── Main Execution ─────────────────────────────
async def generate_spreadsheet_async(config_path: Path, output_file: Path = None):
    """Async main function for spreadsheet generation."""
    config = load_config(config_path)
    
    # Load tickers from config or watchlist
    tickers = config.get("tickers", [])
    if not tickers:
        tickers = load_watchlist_tickers()
    
    # Limit ticker count
    tickers = [t.upper() for t in tickers[:config["display"].get("max_rows", MAX_TICKERS)]]
    
    if not tickers:
        raise ValueError("No tickers to process")
    
    log.info(f"Processing {len(tickers)} tickers")
    
    # Check market status
    market_status, session_name = session_status_now()
    log.info(f"Market: {session_name} ({market_status})")
    
    # Fetch data concurrently
    tasks = []
    
    # RabbitMQ data
    if aio_pika:
        tasks.append(fetch_from_rabbitmq(
            config["rabbitmq"]["host"],
            config["rabbitmq"]["price_queue"],
            tickers
        ))
    else:
        tasks.append(asyncio.create_task(asyncio.coroutine(lambda: {})()))
    
    # YFinance data (only during market hours to save API calls)
    if market_status != "Red":
        loop = asyncio.get_event_loop()
        tasks.append(loop.run_in_executor(None, fetch_yfinance_data, tickers))
    else:
        tasks.append(asyncio.create_task(asyncio.coroutine(lambda: {t: None for t in tickers})()))
    
    # Wait for all data
    rmq_data, yf_data = await asyncio.gather(*tasks)
    
    # Combine data sources
    combined_data = {}
    for ticker in tickers:
        ticker_data = {}
        
        # YFinance data (base)
        if yf_data.get(ticker):
            ticker_data.update(yf_data[ticker])
        
        # RabbitMQ data (overlay)
        if rmq_data.get(ticker):
            rmq = rmq_data[ticker]
            # Update with real-time data if available
            if rmq.get("price"):
                ticker_data["current_price"] = rmq["price"]
                ticker_data["price"] = rmq["price"]
            if rmq.get("volume"):
                ticker_data["volume"] = rmq["volume"]
            if rmq.get("bid"):
                ticker_data["bid"] = rmq["bid"]
            if rmq.get("ask"):
                ticker_data["ask"] = rmq["ask"]
            if rmq.get("signal"):
                ticker_data["signal"] = rmq["signal"]
        
        if ticker_data:
            combined_data[ticker] = ticker_data
    
    # Default output path
    if output_file is None:
        output_file = Path.cwd() / f"stock_tracker_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    # Generate spreadsheet
    create_enhanced_spreadsheet(combined_data, config, output_file)
    
    return output_file

def generate_spreadsheet(config_path: Path, output_file: Path = None):
    """Synchronous wrapper for compatibility."""
    try:
        # Setup event loop
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # Run async function
        result = loop.run_until_complete(
            generate_spreadsheet_async(config_path, output_file)
        )
        
        return result
        
    except Exception as e:
        log.error(f"Spreadsheet generation failed: {e}", exc_info=True)
        raise
    finally:
        # Don't close the loop if it's the main thread's loop
        try:
            if loop and not loop.is_running():
                loop.close()
        except:
            pass

# ───────────────────────────── Entry Point ─────────────────────────────
if __name__ == "__main__":
    # Setup paths
    here = Path(__file__).resolve().parent
    
    # Try shared config location first
    config_paths = [
        here / "shared" / "config.json",  # Primary shared location
        here / "config.json",  # Legacy fallback
        Path("shared/config.json"),  # Relative shared
        Path("config.json")  # Final fallback
    ]
    
    config_path = None
    for path in config_paths:
        if path.exists():
            config_path = path
            break
    
    if not config_path:
        config_path = config_paths[0]  # Use first as default
    
    # Command line arguments override
    if len(sys.argv) > 1:
        config_path = Path(sys.argv[1])
    
    output_path = None
    if len(sys.argv) > 2:
        output_path = Path(sys.argv[2])
    
    try:
        generate_spreadsheet(config_path, output_path)
        sys.exit(0)
    except Exception as e:
        log.error(f"Fatal error: {e}")
        sys.exit(1)