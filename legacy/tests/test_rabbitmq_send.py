"""
Unified RabbitMQ Test Suite
Complete testing tool for sending and receiving messages with various scenarios
"""

import aio_pika
import asyncio
import json
import random
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum
import argparse
import sys

class MessageType(Enum):
    """Types of test messages"""
    SIGNAL = "signal"
    PRICE = "price"
    ORDER = "order"
    ALERT = "alert"
    CUSTOM = "custom"

class TestScenario(Enum):
    """Test scenarios"""
    SIMPLE = "simple"
    BURST = "burst"
    REALISTIC = "realistic"
    ERROR = "error"
    LATENCY = "latency"

class RabbitMQTestSuite:
    """Complete RabbitMQ testing suite"""
    
    def __init__(self, amqp_url: str = "amqp://guest:guest@localhost/"):
        self.amqp_url = amqp_url
        self.tickers = ["AAPL", "MSFT", "GOOGL", "NVDA", "TSLA", "AMD", "META", "AMZN"]
        self.actions = ["BUY", "SELL", "HOLD", "STRONG_BUY", "STRONG_SELL"]
        
    async def send_test_signal(
        self,
        queue_name: str = "signal_queue",
        signal_type: str = "basic"
    ):
        """Send a single test signal"""
        connection = await aio_pika.connect_robust(self.amqp_url)
        channel = await connection.channel()
        
        # Generate signal based on type
        if signal_type == "basic":
            signal = {
                "ticker": "AAPL",
                "action": "BUY",
                "price": 187.23,
                "confidence": 0.93,
                "volume": 13500,
                "timestamp": datetime.now().isoformat()
            }
        elif signal_type == "complex":
            signal = self._generate_complex_signal()
        elif signal_type == "random":
            signal = self._generate_random_signal()
        else:
            signal = {"test": True, "type": signal_type}
        
        # Send message
        await channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps(signal).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key=queue_name
        )
        
        print(f"✅ Test signal sent to {queue_name}")
        print(f"   Signal: {json.dumps(signal, indent=2)}")
        
        await connection.close()
    
    async def send_batch(
        self,
        queue_name: str,
        count: int = 10,
        message_type: MessageType = MessageType.SIGNAL,
        delay: float = 0.1
    ):
        """Send a batch of test messages"""
        connection = await aio_pika.connect_robust(self.amqp_url)
        channel = await connection.channel()
        
        print(f"📤 Sending {count} {message_type.value} messages to {queue_name}")
        
        for i in range(count):
            # Generate message based on type
            if message_type == MessageType.SIGNAL:
                message = self._generate_random_signal()
            elif message_type == MessageType.PRICE:
                message = self._generate_price_update()
            elif message_type == MessageType.ORDER:
                message = self._generate_order()
            elif message_type == MessageType.ALERT:
                message = self._generate_alert()
            else:
                message = {"id": i, "test": True, "timestamp": datetime.now().isoformat()}
            
            # Add sequence number
            message["sequence"] = i + 1
            
            # Send message
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(message).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key=queue_name
            )
            
            if (i + 1) % 10 == 0:
                print(f"   Sent {i + 1}/{count} messages...")
            
            if delay > 0:
                await asyncio.sleep(delay)
        
        await connection.close()
        print(f"✅ Sent {count} messages successfully")
    
    async def test_scenario(
        self,
        queue_name: str,
        scenario: TestScenario
    ):
        """Run a specific test scenario"""
        print(f"🎯 Running scenario: {scenario.value}")
        
        if scenario == TestScenario.SIMPLE:
            # Send a few simple messages
            await self.send_batch(queue_name, count=5, delay=1)
            
        elif scenario == TestScenario.BURST:
            # Send rapid burst of messages
            await self.send_batch(queue_name, count=100, delay=0)
            
        elif scenario == TestScenario.REALISTIC:
            # Simulate realistic trading signals
            await self._realistic_trading_scenario(queue_name)
            
        elif scenario == TestScenario.ERROR:
            # Send messages with errors for testing error handling
            await self._error_scenario(queue_name)
            
        elif scenario == TestScenario.LATENCY:
            # Test latency measurement
            await self._latency_test(queue_name)
    
    async def _realistic_trading_scenario(self, queue_name: str):
        """Simulate realistic trading signal patterns"""
        connection = await aio_pika.connect_robust(self.amqp_url)
        channel = await connection.channel()
        
        print("📊 Simulating realistic trading day...")
        
        # Market open rush (9:30-10:00)
        print("  Market Open (high volume)...")
        for _ in range(20):
            signal = self._generate_random_signal()
            signal["session"] = "market_open"
            await self._send_message(channel, queue_name, signal)
            await asyncio.sleep(0.1)
        
        # Mid-day slow period
        print("  Mid-day (normal volume)...")
        for _ in range(10):
            signal = self._generate_random_signal()
            signal["session"] = "midday"
            await self._send_message(channel, queue_name, signal)
            await asyncio.sleep(0.5)
        
        # Closing rush
        print("  Market Close (high volume)...")
        for _ in range(15):
            signal = self._generate_random_signal()
            signal["session"] = "market_close"
            signal["priority"] = "HIGH"
            await self._send_message(channel, queue_name, signal)
            await asyncio.sleep(0.05)
        
        await connection.close()
        print("✅ Realistic scenario completed")
    
    async def _error_scenario(self, queue_name: str):
        """Send messages that might cause errors"""
        connection = await aio_pika.connect_robust(self.amqp_url)
        channel = await connection.channel()
        
        print("❌ Sending error test messages...")
        
        # Invalid JSON
        await channel.default_exchange.publish(
            aio_pika.Message(body=b"Invalid JSON{"),
            routing_key=queue_name
        )
        print("  Sent invalid JSON")
        
        # Missing required fields
        await self._send_message(channel, queue_name, {"incomplete": True})
        print("  Sent incomplete message")
        
        # Extreme values
        await self._send_message(channel, queue_name, {
            "ticker": "TEST",
            "price": -100,  # Negative price
            "volume": 999999999999,  # Huge volume
            "confidence": 150  # Invalid confidence
        })
        print("  Sent extreme values")
        
        # Empty message
        await channel.default_exchange.publish(
            aio_pika.Message(body=b""),
            routing_key=queue_name
        )
        print("  Sent empty message")
        
        await connection.close()
        print("✅ Error scenarios sent")
    
    async def _latency_test(self, queue_name: str):
        """Test message latency"""
        print("⏱️ Testing latency...")
        
        # Send timestamped messages
        connection = await aio_pika.connect_robust(self.amqp_url)
        channel = await connection.channel()
        
        for i in range(10):
            message = {
                "test": "latency",
                "id": i,
                "sent_at": datetime.now().isoformat(),
                "sent_timestamp": datetime.now().timestamp()
            }
            
            await self._send_message(channel, queue_name, message)
            print(f"  Sent latency test {i+1}/10")
            await asyncio.sleep(0.1)
        
        await connection.close()
        print("✅ Latency test messages sent")
        print("  Check receiver for latency measurements")
    
    async def _send_message(self, channel, queue_name: str, message: Dict):
        """Helper to send a message"""
        await channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps(message).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key=queue_name
        )
    
    def _generate_random_signal(self) -> Dict[str, Any]:
        """Generate a random trading signal"""
        ticker = random.choice(self.tickers)
        action = random.choice(self.actions)
        base_price = random.uniform(50, 500)
        
        signal = {
            "signal_id": f"sig_{datetime.now().timestamp():.0f}",
            "ticker": ticker,
            "action": action,
            "price": round(base_price, 2),
            "confidence": round(random.uniform(0.5, 1.0), 2),
            "volume": random.randint(1000, 100000),
            "timestamp": datetime.now().isoformat(),
            "signal_type": random.choice(["reversal", "breakout", "momentum", "mean_reversion"])
        }
        
        # Sometimes add stop loss and take profit
        if random.random() > 0.5:
            if action in ["BUY", "STRONG_BUY"]:
                signal["stop_loss"] = round(base_price * 0.98, 2)
                signal["take_profit"] = round(base_price * 1.03, 2)
            elif action in ["SELL", "STRONG_SELL"]:
                signal["stop_loss"] = round(base_price * 1.02, 2)
                signal["take_profit"] = round(base_price * 0.97, 2)
        
        return signal
    
    def _generate_complex_signal(self) -> Dict[str, Any]:
        """Generate a complex signal with all fields"""
        return {
            "signal_id": f"complex_{datetime.now().timestamp():.0f}",
            "ticker": "NVDA",
            "action": "BUY",
            "price": 850.50,
            "confidence": 0.92,
            "priority": 2,
            "signal_type": "breakout",
            "volume": 2500000,
            "stop_loss": 835.00,
            "take_profit": 875.00,
            "position_size": 100,
            "risk_reward_ratio": 2.5,
            "technical_indicators": {
                "rsi": 45.5,
                "macd": 1.25,
                "bollinger_position": "lower",
                "moving_average_50": 845.00,
                "moving_average_200": 820.00
            },
            "metadata": {
                "strategy": "breakout_v2",
                "timeframe": "5m",
                "market_conditions": "bullish",
                "news_sentiment": 0.75
            },
            "timestamp": datetime.now().isoformat()
        }
    
    def _generate_price_update(self) -> Dict[str, Any]:
        """Generate a price update message"""
        ticker = random.choice(self.tickers)
        price = random.uniform(50, 500)
        
        return {
            "ticker": ticker,
            "price": round(price, 2),
            "bid": round(price - 0.01, 2),
            "ask": round(price + 0.01, 2),
            "volume": random.randint(10000, 1000000),
            "high": round(price * 1.02, 2),
            "low": round(price * 0.98, 2),
            "open": round(price * random.uniform(0.98, 1.02), 2),
            "timestamp": datetime.now().isoformat()
        }
    
    def _generate_order(self) -> Dict[str, Any]:
        """Generate an order message"""
        return {
            "order_id": f"ord_{datetime.now().timestamp():.0f}",
            "ticker": random.choice(self.tickers),
            "side": random.choice(["buy", "sell"]),
            "quantity": random.randint(1, 1000),
            "price": round(random.uniform(50, 500), 2),
            "order_type": random.choice(["market", "limit", "stop"]),
            "status": "pending",
            "timestamp": datetime.now().isoformat()
        }
    
    def _generate_alert(self) -> Dict[str, Any]:
        """Generate an alert message"""
        alerts = [
            "Price crossed above resistance",
            "Volume spike detected",
            "RSI oversold",
            "MACD crossover",
            "Support level breach"
        ]
        
        return {
            "alert_id": f"alert_{datetime.now().timestamp():.0f}",
            "ticker": random.choice(self.tickers),
            "alert_type": random.choice(alerts),
            "severity": random.choice(["low", "medium", "high", "critical"]),
            "message": random.choice(alerts),
            "timestamp": datetime.now().isoformat()
        }

async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="RabbitMQ Test Suite")
    parser.add_argument(
        "action",
        choices=["send", "batch", "scenario"],
        help="Action to perform"
    )
    parser.add_argument(
        "--queue",
        default="signal_queue",
        help="Queue name"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of messages for batch"
    )
    parser.add_argument(
        "--type",
        choices=["signal", "price", "order", "alert", "custom"],
        default="signal",
        help="Message type"
    )
    parser.add_argument(
        "--scenario",
        choices=["simple", "burst", "realistic", "error", "latency"],
        default="simple",
        help="Test scenario"
    )
    parser.add_argument(
        "--signal-type",
        choices=["basic", "complex", "random"],
        default="basic",
        help="Signal type for send action"
    )
    
    args = parser.parse_args()
    
    # Create test suite
    suite = RabbitMQTestSuite()
    
    # Execute action
    if args.action == "send":
        await suite.send_test_signal(args.queue, args.signal_type)
    elif args.action == "batch":
        message_type = MessageType[args.type.upper()]
        await suite.send_batch(args.queue, args.count, message_type)
    elif args.action == "scenario":
        scenario = TestScenario[args.scenario.upper()]
        await suite.test_scenario(args.queue, scenario)

if __name__ == "__main__":
    # Quick test for direct execution
    if len(sys.argv) == 1:
        # No arguments, run simple test
        asyncio.run(RabbitMQTestSuite().send_test_signal())
    else:
        # Run with arguments
        asyncio.run(mai