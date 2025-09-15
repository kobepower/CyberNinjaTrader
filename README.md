📈 CyberNinja Trader Pro

A sophisticated financial trading application with real-time market analysis, signal detection, and strategy execution.
Built with PyQt5, RabbitMQ, and async backends, it combines live data streaming with a professional GUI.

🚀 Features

Real-Time Data

Yahoo Finance + Finnhub API fallback

Async RabbitMQ message queues

Trading Signals

RSI-based bottom detection

Candlestick reversals (hammer, doji, engulfing)

Breakouts & unusual volume alerts

Risk Management

Stop-loss & trailing stop logic

Max daily loss limits (default 2%)

Account-based position sizing

GUI

Multi-tab PyQt5 interface

Reversal Radar, Strategy Engine, RabbitMQ Monitor

Performance metrics and watchdogs

⚙️ Installation
1. Clone the Repository
git clone https://github.com/YOUR-USERNAME/CyberNinjaTrader.git
cd CyberNinjaTrader

2. Setup Environment
✅ Option 1 – Conda (recommended)
conda create -n cyberninja python=3.10
conda activate cyberninja
pip install -r requirements.txt

⚡ Option 2 – Virtualenv
python -m venv venv
venv\Scripts\activate      # Windows
source venv/bin/activate   # Linux/Mac

pip install -r requirements.txt

▶️ Running the App

Main GUI:

python CyberNinjaTrader.py


Backend:

python backend/app/main.py


Strategy Engine (optional):

python -m backend.app.strategies.strategy_consumer

📂 Configuration

Main config lives in:

shared/config.json


Example:

{
  "rabbitmq": {
    "host": "localhost",
    "price_queue": "price_queue",
    "signal_queue": "signal_queue"
  },
  "api_keys": {
    "finnhub": "YOUR_FINNHUB_API_KEY"
  },
  "trading": {
    "account_size": 60000,
    "max_positions": 3
  }
}

🛠️ Development Tools

Test RabbitMQ:

python backend/app/utils/rabbitmq_api.py


Test signal detection:

python backend/app/utils/bottom_signal_detector.py --symbols AAPL NVDA MSFT


Generate Excel market sheet:

python generate_spreadsheet.py

📊 Database

SQLite file:

support_cache.db


Stores trade logs and cached support/resistance levels.

⏰ Market Hours

Morning: 09:30 – 11:30 ET

Afternoon: 14:30 – 15:30 ET

👥 Contributors

Kobe 🥷 – Lead developer, architect of CyberNinja Trader Pro

Claude (Anthropic AI) – Code generation, async backend support

ChatGPT (OpenAI) – Debugging, documentation, architecture support

📜 License

This project is licensed under the MIT License:

MIT License

Copyright (c) 2025 Kobe

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
