"""
Standardized Watchlist Management System for CyberNinja Trader Pro
Uses JSON format from shared/ folder for consistent watchlist and config management
"""

import os
import json
import logging
import threading
import shutil
from typing import List, Dict, Set, Optional, Any, Union
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from enum import Enum
import re
import yfinance as yf
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants - Updated to use shared/ folder
SHARED_DIR = Path(__file__).parent / 'shared'
WATCHLIST_PATH = SHARED_DIR / 'watchlist.json'
CONFIG_PATH = SHARED_DIR / 'config.json'
BACKUP_DIR = SHARED_DIR / 'backups'
MAX_BACKUPS = 10
VALID_TICKER_PATTERN = re.compile(r'^[A-Z]{1,5}(?:\.[A-Z]{1,2})?$')  # Supports AAPL, BRK.B etc

class TickerPriority(Enum):
    """Ticker priority levels"""
    HIGH = 1
    MEDIUM = 2
    LOW = 3

@dataclass
class TickerInfo:
    """Enhanced ticker information"""
    symbol: str
    priority: int = 2
    notes: str = ""
    sector: str = ""
    enabled: bool = True
    added_date: Optional[str] = None
    last_analysis: Optional[str] = None
    
    def __post_init__(self):
        if self.added_date is None:
            self.added_date = datetime.now().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)

class SharedWatchlistManager:
    """
    Standardized watchlist manager using shared/ folder JSON structure
    """
    
    def __init__(self, watchlist_path: Optional[Path] = None, config_path: Optional[Path] = None):
        self.watchlist_path = watchlist_path or WATCHLIST_PATH
        self.config_path = config_path or CONFIG_PATH
        self.lock = threading.RLock()
        self._watchlist_data: Optional[Dict] = None
        self._config_data: Optional[Dict] = None
        
        # Ensure shared directory exists
        SHARED_DIR.mkdir(exist_ok=True)
        BACKUP_DIR.mkdir(exist_ok=True)
        
        # Initialize files if they don't exist
        self._ensure_files_exist()
        
        # Load data
        self._load_watchlist()
        self._load_config()
    
    def _ensure_files_exist(self):
        """Ensure watchlist and config files exist with defaults"""
        if not self.watchlist_path.exists():
            default_watchlist = {
                "watchlist": {
                    "version": "1.0",
                    "last_updated": datetime.now().isoformat(),
                    "description": "Default watchlist for CyberNinja Trader Pro",
                    "tickers": []
                },
                "settings": {
                    "auto_update": True,
                    "max_concurrent_analysis": 10,
                    "priority_weighting": {
                        "1": "High priority - continuous monitoring",
                        "2": "Medium priority - regular monitoring",
                        "3": "Low priority - periodic monitoring"
                    }
                }
            }
            self._save_json(self.watchlist_path, default_watchlist)
            logger.info(f"Created default watchlist at {self.watchlist_path}")
        
        if not self.config_path.exists():
            # Use minimal config - the shared/config.json should be created separately
            logger.warning(f"Config file not found at {self.config_path}. Using defaults.")
    
    def _load_watchlist(self) -> Dict:
        """Load watchlist from JSON"""
        with self.lock:
            try:
                with open(self.watchlist_path, 'r', encoding='utf-8') as f:
                    self._watchlist_data = json.load(f)
                logger.info(f"Loaded watchlist from {self.watchlist_path}")
            except Exception as e:
                logger.error(f"Error loading watchlist: {e}")
                self._watchlist_data = {"watchlist": {"tickers": []}, "settings": {}}
            
            return self._watchlist_data
    
    def _load_config(self) -> Dict:
        """Load system config from JSON"""
        with self.lock:
            try:
                if self.config_path.exists():
                    with open(self.config_path, 'r', encoding='utf-8') as f:
                        self._config_data = json.load(f)
                    logger.info(f"Loaded config from {self.config_path}")
                else:
                    self._config_data = {}
                    logger.warning("Config file not found, using empty config")
            except Exception as e:
                logger.error(f"Error loading config: {e}")
                self._config_data = {}
            
            return self._config_data
    
    def _save_json(self, file_path: Path, data: Dict):
        """Save JSON data with backup"""
        with self.lock:
            try:
                # Create backup if file exists
                if file_path.exists():
                    backup_name = f"{file_path.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    backup_path = BACKUP_DIR / backup_name
                    shutil.copy2(file_path, backup_path)
                    
                    # Clean old backups
                    self._cleanup_backups(file_path.stem)
                
                # Save new data
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                
                logger.info(f"Saved data to {file_path}")
                
            except Exception as e:
                logger.error(f"Error saving to {file_path}: {e}")
                raise
    
    def _cleanup_backups(self, file_prefix: str):
        """Remove old backup files, keeping only MAX_BACKUPS"""
        try:
            backups = sorted([
                f for f in BACKUP_DIR.glob(f"{file_prefix}_*.json")
            ], key=lambda x: x.stat().st_mtime, reverse=True)
            
            for old_backup in backups[MAX_BACKUPS:]:
                old_backup.unlink()
                logger.debug(f"Removed old backup: {old_backup}")
                
        except Exception as e:
            logger.error(f"Error cleaning backups: {e}")
    
    def get_all_tickers(self, enabled_only: bool = True) -> List[str]:
        """Get all ticker symbols"""
        with self.lock:
            tickers = []
            if self._watchlist_data and 'watchlist' in self._watchlist_data:
                for ticker_info in self._watchlist_data['watchlist'].get('tickers', []):
                    if not enabled_only or ticker_info.get('enabled', True):
                        tickers.append(ticker_info['symbol'])
            return tickers
    
    def get_ticker_info(self, symbol: str) -> Optional[TickerInfo]:
        """Get detailed information for a ticker"""
        with self.lock:
            if self._watchlist_data and 'watchlist' in self._watchlist_data:
                for ticker_data in self._watchlist_data['watchlist'].get('tickers', []):
                    if ticker_data['symbol'] == symbol.upper():
                        return TickerInfo(**ticker_data)
            return None
    
    def get_tickers_by_priority(self, priority: int) -> List[str]:
        """Get tickers by priority level"""
        with self.lock:
            tickers = []
            if self._watchlist_data and 'watchlist' in self._watchlist_data:
                for ticker_info in self._watchlist_data['watchlist'].get('tickers', []):
                    if (ticker_info.get('priority', 2) == priority and 
                        ticker_info.get('enabled', True)):
                        tickers.append(ticker_info['symbol'])
            return tickers
    
    def get_high_priority_tickers(self) -> List[str]:
        """Get high priority tickers (priority 1)"""
        return self.get_tickers_by_priority(1)
    
    def add_ticker(self, symbol: str, priority: int = 2, notes: str = "", 
                  sector: str = "", enabled: bool = True) -> bool:
        """Add a new ticker to the watchlist"""
        with self.lock:
            symbol = symbol.upper().strip()
            
            # Validate ticker format
            if not VALID_TICKER_PATTERN.match(symbol):
                logger.error(f"Invalid ticker format: {symbol}")
                return False
            
            # Check if already exists
            if self.get_ticker_info(symbol):
                logger.warning(f"Ticker {symbol} already exists in watchlist")
                return False
            
            # Create ticker info
            ticker_info = TickerInfo(
                symbol=symbol,
                priority=priority,
                notes=notes,
                sector=sector,
                enabled=enabled
            ).to_dict()
            
            # Add to watchlist
            if 'watchlist' not in self._watchlist_data:
                self._watchlist_data['watchlist'] = {'tickers': []}
            
            self._watchlist_data['watchlist']['tickers'].append(ticker_info)
            self._watchlist_data['watchlist']['last_updated'] = datetime.now().isoformat()
            
            # Save
            try:
                self._save_json(self.watchlist_path, self._watchlist_data)
                logger.info(f"Added ticker {symbol} to watchlist")
                return True
            except Exception as e:
                logger.error(f"Failed to save after adding {symbol}: {e}")
                return False
    
    def remove_ticker(self, symbol: str) -> bool:
        """Remove a ticker from the watchlist"""
        with self.lock:
            symbol = symbol.upper().strip()
            
            if 'watchlist' not in self._watchlist_data:
                return False
            
            tickers = self._watchlist_data['watchlist'].get('tickers', [])
            original_length = len(tickers)
            
            # Remove ticker
            self._watchlist_data['watchlist']['tickers'] = [
                t for t in tickers if t['symbol'] != symbol
            ]
            
            if len(self._watchlist_data['watchlist']['tickers']) < original_length:
                self._watchlist_data['watchlist']['last_updated'] = datetime.now().isoformat()
                try:
                    self._save_json(self.watchlist_path, self._watchlist_data)
                    logger.info(f"Removed ticker {symbol} from watchlist")
                    return True
                except Exception as e:
                    logger.error(f"Failed to save after removing {symbol}: {e}")
                    return False
            
            logger.warning(f"Ticker {symbol} not found in watchlist")
            return False
    
    def update_ticker(self, symbol: str, **kwargs) -> bool:
        """Update ticker information"""
        with self.lock:
            symbol = symbol.upper().strip()
            
            if 'watchlist' not in self._watchlist_data:
                return False
            
            tickers = self._watchlist_data['watchlist'].get('tickers', [])
            
            for ticker in tickers:
                if ticker['symbol'] == symbol:
                    # Update allowed fields
                    for key, value in kwargs.items():
                        if key in ['priority', 'notes', 'sector', 'enabled']:
                            ticker[key] = value
                    
                    ticker['last_analysis'] = datetime.now().isoformat()
                    self._watchlist_data['watchlist']['last_updated'] = datetime.now().isoformat()
                    
                    try:
                        self._save_json(self.watchlist_path, self._watchlist_data)
                        logger.info(f"Updated ticker {symbol}")
                        return True
                    except Exception as e:
                        logger.error(f"Failed to save after updating {symbol}: {e}")
                        return False
            
            logger.warning(f"Ticker {symbol} not found for update")
            return False
    
    def get_watchlist_stats(self) -> Dict[str, Any]:
        """Get watchlist statistics"""
        with self.lock:
            if not self._watchlist_data or 'watchlist' not in self._watchlist_data:
                return {}
            
            tickers = self._watchlist_data['watchlist'].get('tickers', [])
            
            stats = {
                'total_tickers': len(tickers),
                'enabled_tickers': len([t for t in tickers if t.get('enabled', True)]),
                'by_priority': defaultdict(int),
                'by_sector': defaultdict(int),
                'last_updated': self._watchlist_data['watchlist'].get('last_updated', '')
            }
            
            for ticker in tickers:
                if ticker.get('enabled', True):
                    priority = ticker.get('priority', 2)
                    sector = ticker.get('sector', 'Unknown')
                    stats['by_priority'][priority] += 1
                    stats['by_sector'][sector] += 1
            
            return dict(stats)
    
    def validate_tickers(self, check_market: bool = False) -> Dict[str, Any]:
        """Validate all tickers in watchlist"""
        with self.lock:
            results = {
                'valid': [],
                'invalid': [],
                'errors': []
            }
            
            tickers = self.get_all_tickers(enabled_only=False)
            
            for symbol in tickers:
                try:
                    # Basic format validation
                    if not VALID_TICKER_PATTERN.match(symbol):
                        results['invalid'].append(f"{symbol}: Invalid format")
                        continue
                    
                    # Market validation (optional, slower)
                    if check_market:
                        try:
                            ticker = yf.Ticker(symbol)
                            info = ticker.info
                            if info.get('symbol'):
                                results['valid'].append(symbol)
                            else:
                                results['invalid'].append(f"{symbol}: Not found in market")
                        except Exception as e:
                            results['invalid'].append(f"{symbol}: Market check failed - {e}")
                    else:
                        results['valid'].append(symbol)
                        
                except Exception as e:
                    results['errors'].append(f"{symbol}: Validation error - {e}")
            
            return results
    
    def reload(self):
        """Reload watchlist and config from disk"""
        with self.lock:
            self._load_watchlist()
            self._load_config()
            logger.info("Reloaded watchlist and config from disk")
    
    def get_config_value(self, key_path: str, default: Any = None) -> Any:
        """Get config value using dot notation (e.g., 'rabbitmq.host')"""
        if not self._config_data:
            return default
        
        keys = key_path.split('.')
        value = self._config_data
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default

# Legacy functions for backward compatibility
def load_watchlist(file_path: str = None) -> List[str]:
    """Legacy function - load watchlist from shared JSON"""
    manager = SharedWatchlistManager()
    return manager.get_all_tickers()

def load_watchlist_tickers() -> List[str]:
    """Legacy function - load all enabled tickers"""
    manager = SharedWatchlistManager()
    return manager.get_all_tickers(enabled_only=True)

def get_watchlist_manager() -> SharedWatchlistManager:
    """Get singleton watchlist manager instance"""
    if not hasattr(get_watchlist_manager, '_instance'):
        get_watchlist_manager._instance = SharedWatchlistManager()
    return get_watchlist_manager._instance

# Example usage
if __name__ == "__main__":
    # Test the new watchlist manager
    manager = SharedWatchlistManager()
    
    print("=== CyberNinja Trader Pro - Watchlist Manager Test ===")
    print(f"Watchlist path: {manager.watchlist_path}")
    print(f"Config path: {manager.config_path}")
    
    # Get all tickers
    tickers = manager.get_all_tickers()
    print(f"All tickers ({len(tickers)}): {tickers}")
    
    # Get high priority tickers
    high_priority = manager.get_high_priority_tickers()
    print(f"High priority tickers: {high_priority}")
    
    # Get stats
    stats = manager.get_watchlist_stats()
    print(f"Watchlist stats: {json.dumps(stats, indent=2)}")
    
    # Test config access
    rabbitmq_host = manager.get_config_value('rabbitmq.host', 'localhost')
    print(f"RabbitMQ host from config: {rabbitmq_host}")