"""
Enhanced Signal Logger with Analytics, Persistence, and Alerting
Tracks signal performance, stores in database, and provides real-time analytics
"""

import asyncio
import json
import logging
import logging.handlers
import aio_pika
import signal as sys_signal
import sys
import os
import sqlite3
import aiofiles
from typing import Dict, Optional, List, Any, Set
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from enum import Enum
from collections import defaultdict, deque
import smtplib
from email.mime.text import MIMEText

# Configuration
LOG_DIR = Path("logs")
DB_PATH = Path("data/signals.db")
LOG_FILE = LOG_DIR / "signal_logger.log"
CRITICAL_LOG_FILE = LOG_DIR / "critical_signals.log"
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10MB
BACKUP_COUNT = 5

# Create directories
LOG_DIR.mkdir(exist_ok=True)
DB_PATH.parent.mkdir(exist_ok=True)

# Configure rotating file handler
def setup_logging():
    """Setup logging with rotation and multiple handlers"""
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )
    
    # Main rotating log
    main_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE,
        maxBytes=MAX_LOG_SIZE,
        backupCount=BACKUP_COUNT,
        encoding='utf-8'
    )
    main_handler.setFormatter(formatter)
    
    # Critical signals log
    critical_handler = logging.handlers.RotatingFileHandler(
        CRITICAL_LOG_FILE,
        maxBytes=MAX_LOG_SIZE,
        backupCount=BACKUP_COUNT,
        encoding='utf-8'
    )
    critical_handler.setLevel(logging.WARNING)
    critical_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Configure logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(main_handler)
    logger.addHandler(critical_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

class SignalPriority(Enum):
    """Signal priority levels"""
    CRITICAL = 1
    HIGH = 2
    MEDIUM = 3
    LOW = 4

class SignalStatus(Enum):
    """Signal processing status"""
    RECEIVED = "received"
    LOGGED = "logged"
    PROCESSED = "processed"
    EXECUTED = "executed"
    FAILED = "failed"
    EXPIRED = "expired"

@dataclass
class TradingSignal:
    """Trading signal data structure"""
    signal_id: str
    ticker: str
    action: str
    price: float
    timestamp: datetime
    priority: SignalPriority = SignalPriority.MEDIUM
    confidence: float = 0.0
    signal_type: Optional[str] = None
    volume: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    status: SignalStatus = SignalStatus.RECEIVED
    metadata: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        data['priority'] = self.priority.value
        data['status'] = self.status.value
        return data
    
    @classmethod
    def from_message(cls, message: Dict[str, Any]) -> 'TradingSignal':
        """Create from message dictionary"""
        # Extract fields with defaults
        signal_id = message.get('signal_id', str(datetime.now().timestamp()))
        ticker = message.get('ticker', 'UNKNOWN')
        action = message.get('action', 'UNKNOWN')
        price = float(message.get('price', 0))
        
        # Parse timestamp
        if 'timestamp' in message:
            timestamp = datetime.fromisoformat(message['timestamp'])
        else:
            timestamp = datetime.now()
        
        # Parse priority
        priority = SignalPriority.MEDIUM
        if 'priority' in message:
            try:
                priority = SignalPriority(message['priority'])
            except ValueError:
                pass
        
        return cls(
            signal_id=signal_id,
            ticker=ticker,
            action=action,
            price=price,
            timestamp=timestamp,
            priority=priority,
            confidence=float(message.get('confidence', 0)),
            signal_type=message.get('signal_type'),
            volume=message.get('volume'),
            stop_loss=message.get('stop_loss'),
            take_profit=message.get('take_profit'),
            metadata=message.get('metadata')
        )

class SignalDatabase:
    """SQLite database for signal persistence"""
    
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize database tables"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS signals (
                    signal_id TEXT PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    action TEXT NOT NULL,
                    price REAL NOT NULL,
                    timestamp TEXT NOT NULL,
                    priority INTEGER,
                    confidence REAL,
                    signal_type TEXT,
                    volume REAL,
                    stop_loss REAL,
                    take_profit REAL,
                    status TEXT,
                    metadata TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_ticker ON signals(ticker);
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_timestamp ON signals(timestamp);
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS signal_performance (
                    signal_id TEXT PRIMARY KEY,
                    entry_price REAL,
                    exit_price REAL,
                    pnl REAL,
                    pnl_percent REAL,
                    executed_at TEXT,
                    closed_at TEXT,
                    FOREIGN KEY (signal_id) REFERENCES signals(signal_id)
                )
            ''')
            
            conn.commit()
    
    async def save_signal(self, signal: TradingSignal):
        """Save signal to database"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT OR REPLACE INTO signals 
                (signal_id, ticker, action, price, timestamp, priority, 
                 confidence, signal_type, volume, stop_loss, take_profit, status, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                signal.signal_id,
                signal.ticker,
                signal.action,
                signal.price,
                signal.timestamp.isoformat(),
                signal.priority.value,
                signal.confidence,
                signal.signal_type,
                signal.volume,
                signal.stop_loss,
                signal.take_profit,
                signal.status.value,
                json.dumps(signal.metadata) if signal.metadata else None
            ))
            conn.commit()
    
    async def get_recent_signals(self, hours: int = 24) -> List[Dict]:
        """Get recent signals"""
        cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT * FROM signals 
                WHERE timestamp > ? 
                ORDER BY timestamp DESC
            ''', (cutoff,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    async def get_signal_stats(self) -> Dict[str, Any]:
        """Get signal statistics"""
        with sqlite3.connect(self.db_path) as conn:
            # Total signals
            total = conn.execute('SELECT COUNT(*) FROM signals').fetchone()[0]
            
            # By action
            cursor = conn.execute('''
                SELECT action, COUNT(*) as count 
                FROM signals 
                GROUP BY action
            ''')
            by_action = {row[0]: row[1] for row in cursor.fetchall()}
            
            # By ticker (top 10)
            cursor = conn.execute('''
                SELECT ticker, COUNT(*) as count 
                FROM signals 
                GROUP BY ticker 
                ORDER BY count DESC 
                LIMIT 10
            ''')
            top_tickers = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Success rate (if performance data exists)
            cursor = conn.execute('''
                SELECT 
                    COUNT(CASE WHEN pnl > 0 THEN 1 END) as wins,
                    COUNT(CASE WHEN pnl <= 0 THEN 1 END) as losses
                FROM signal_performance
            ''')
            row = cursor.fetchone()
            wins, losses = row[0] or 0, row[1] or 0
            
            return {
                'total_signals': total,
                'by_action': by_action,
                'top_tickers': top_tickers,
                'wins': wins,
                'losses': losses,
                'win_rate': (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0
            }

class SignalAnalyzer:
    """Real-time signal analysis and alerting"""
    
    def __init__(self):
        self.signal_counts = defaultdict(int)
        self.ticker_momentum = defaultdict(lambda: deque(maxlen=10))
        self.high_confidence_threshold = 80.0
        self.alert_callbacks = []
    
    async def analyze_signal(self, signal: TradingSignal) -> Dict[str, Any]:
        """Analyze signal for patterns and alerts"""
        analysis = {
            'ticker': signal.ticker,
            'action': signal.action,
            'alerts': []
        }
        
        # Track signal counts
        self.signal_counts[signal.ticker] += 1
        
        # Track momentum
        self.ticker_momentum[signal.ticker].append(signal.action)
        
        # Check for high confidence
        if signal.confidence >= self.high_confidence_threshold:
            analysis['alerts'].append({
                'type': 'HIGH_CONFIDENCE',
                'message': f'High confidence signal: {signal.ticker} {signal.action} @ {signal.price:.2f} ({signal.confidence:.1f}%)'
            })
        
        # Check for signal clustering
        if self.signal_counts[signal.ticker] >= 5:
            recent_actions = list(self.ticker_momentum[signal.ticker])
            if recent_actions.count(signal.action) >= 3:
                analysis['alerts'].append({
                    'type': 'SIGNAL_CLUSTER',
                    'message': f'Multiple {signal.action} signals for {signal.ticker}'
                })
        
        # Check for critical priority
        if signal.priority == SignalPriority.CRITICAL:
            analysis['alerts'].append({
                'type': 'CRITICAL_SIGNAL',
                'message': f'CRITICAL: {signal.ticker} {signal.action} @ {signal.price:.2f}'
            })
        
        # Risk checks
        if signal.stop_loss and signal.price:
            risk = abs(signal.price - signal.stop_loss) / signal.price
            if risk > 0.05:  # More than 5% risk
                analysis['alerts'].append({
                    'type': 'HIGH_RISK',
                    'message': f'High risk signal: {signal.ticker} risk={risk:.1%}'
                })
        
        return analysis

class SignalLogger:
    """Enhanced signal logger with analytics"""
    
    def __init__(
        self,
        queue_name: str = "signal_queue",
        enable_database: bool = True,
        enable_analytics: bool = True,
        enable_alerts: bool = True
    ):
        self.queue_name = queue_name
        self.enable_database = enable_database
        self.enable_analytics = enable_analytics
        self.enable_alerts = enable_alerts
        
        # Components
        self.db = SignalDatabase() if enable_database else None
        self.analyzer = SignalAnalyzer() if enable_analytics else None
        
        # Statistics
        self.stats = {
            'total_received': 0,
            'total_logged': 0,
            'total_errors': 0,
            'by_priority': defaultdict(int),
            'by_ticker': defaultdict(int),
            'start_time': datetime.now()
        }
        
        # Control
        self.stop_event = asyncio.Event()
    
    async def log_signal(self, data: bytes):
        """Process and log a signal"""
        try:
            # Parse message
            message = json.loads(data.decode('utf-8'))
            signal = TradingSignal.from_message(message)
            
            self.stats['total_received'] += 1
            self.stats['by_priority'][signal.priority.name] += 1
            self.stats['by_ticker'][signal.ticker] += 1
            
            # Log based on priority
            if signal.priority == SignalPriority.CRITICAL:
                logger.critical(
                    f"🚨 CRITICAL Signal: {signal.ticker} {signal.action} @ ${signal.price:.2f} "
                    f"(Confidence: {signal.confidence:.1f}%)"
                )
            elif signal.priority == SignalPriority.HIGH:
                logger.warning(
                    f"⚠️ HIGH Priority: {signal.ticker} {signal.action} @ ${signal.price:.2f}"
                )
            else:
                logger.info(
                    f"📈 Signal: {signal.ticker} {signal.action} @ ${signal.price:.2f}"
                )
            
            # Save to database
            if self.db:
                await self.db.save_signal(signal)
            
            # Analyze signal
            if self.analyzer:
                analysis = await self.analyzer.analyze_signal(signal)
                
                # Log alerts
                for alert in analysis['alerts']:
                    logger.warning(f"🔔 {alert['type']}: {alert['message']}")
            
            # Update status
            signal.status = SignalStatus.LOGGED
            self.stats['total_logged'] += 1
            
            # Log additional details
            if signal.stop_loss or signal.take_profit:
                logger.info(
                    f"   Risk Management: SL=${signal.stop_loss:.2f} TP=${signal.take_profit:.2f}"
                )
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode message: {e}")
            logger.debug(f"Raw data: {data}")
            self.stats['total_errors'] += 1
        except Exception as e:
            logger.error(f"Error processing signal: {e}")
            self.stats['total_errors'] += 1
    
    async def consume_signals(self, queue):
        """Consume signals from RabbitMQ queue"""
        try:
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        await self.log_signal(message.body)
                    
                    if self.stop_event.is_set():
                        break
                        
        except Exception as e:
            logger.error(f"Error in consume_signals: {e}")
    
    async def periodic_stats(self):
        """Log periodic statistics"""
        while not self.stop_event.is_set():
            await asyncio.sleep(300)  # Every 5 minutes
            
            runtime = (datetime.now() - self.stats['start_time']).seconds
            rate = self.stats['total_received'] / runtime if runtime > 0 else 0
            
            logger.info("=" * 60)
            logger.info("📊 SIGNAL STATISTICS")
            logger.info(f"Total Received: {self.stats['total_received']}")
            logger.info(f"Total Logged: {self.stats['total_logged']}")
            logger.info(f"Total Errors: {self.stats['total_errors']}")
            logger.info(f"Rate: {rate:.2f} signals/second")
            logger.info(f"By Priority: {dict(self.stats['by_priority'])}")
            logger.info(f"Top Tickers: {dict(sorted(self.stats['by_ticker'].items(), key=lambda x: x[1], reverse=True)[:5])}")
            
            if self.db:
                db_stats = await self.db.get_signal_stats()
                logger.info(f"Win Rate: {db_stats['win_rate']:.1f}%")
            
            logger.info("=" * 60)
    
    async def run(self):
        """Main run loop"""
        try:
            # Connect to RabbitMQ
            connection = await aio_pika.connect_robust("amqp://localhost/")
            channel = await connection.channel()
            queue = await channel.declare_queue(self.queue_name, durable=True)
            
            logger.info(f"✅ Connected to RabbitMQ, consuming from {self.queue_name}")
            
            # Start tasks
            tasks = [
                asyncio.create_task(self.consume_signals(queue)),
                asyncio.create_task(self.periodic_stats())
            ]
            
            # Wait for stop signal
            await self.stop_event.wait()
            
            # Cancel tasks
            for task in tasks:
                task.cancel()
            
            # Wait for cancellation
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # Close connection
            await connection.close()
            logger.info("RabbitMQ connection closed")
            
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
        finally:
            # Log final stats
            logger.info("=" * 60)
            logger.info("📊 FINAL STATISTICS")
            logger.info(f"Total Processed: {self.stats['total_logged']}")
            logger.info(f"Error Rate: {self.stats['total_errors'] / max(self.stats['total_received'], 1):.2%}")
            logger.info("=" * 60)
    
    def shutdown(self):
        """Trigger shutdown"""
        logger.info("🛑 Shutdown requested")
        self.stop_event.set()

# Global instance
signal_logger: Optional[SignalLogger] = None

def handle_signal(sig, frame):
    """Handle system signals"""
    if signal_logger:
        signal_logger.shutdown()

async def main():
    """Main entry point"""
    global signal_logger
    
    # Create logger
    signal_logger = SignalLogger(
        enable_database=True,
        enable_analytics=True,
        enable_alerts=True
    )
    
    # Run
    await signal_logger.run()
    
    logger.info("✅ Signal logger terminated")

if __name__ == "__main__":
    # Setup signal handlers
    for sig in (sys_signal.SIGINT, sys_signal.SIGTERM):
        sys_signal.signal(sig, handle_signal)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        logger.info("Goodbye!")