from app.utils.realtime_data import subscribe_to_ticker

def handle_candle(candle):
    print(f"{candle.symbol} → {candle.close} @ {candle.timestamp}")

subscribe_to_ticker("NVDA", handle_candle)

import time
while True:
    time.sleep(1)
