"""
Enhanced RabbitMQ Testing Suite
Comprehensive tools for testing message queues, monitoring, and debugging
"""

import aio_pika
import asyncio
import json
import argparse
import sys
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum
from dataclasses import dataclass
import logging
from colorama import init, Fore, Style

# Initialize colorama for colored output
init(autoreset=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

class TestMode(Enum):
    """Testing modes"""
    CONSUME = "consume"
    PRODUCE = "produce"
    MONITOR = "monitor"
    PURGE = "purge"
    PEEK = "peek"
    STRESS = "stress"

@dataclass
class MessageStats:
    """Message statistics"""
    total: int = 0
    rate: float = 0.0
    errors: int = 0
    last_message: Optional[datetime] = None
    by_type: Dict[str, int] = None
    
    def __post_init__(self):
        if self.by_type is None:
            self.by_type = {}

class RabbitMQTester:
    """Comprehensive RabbitMQ testing tool"""
    
    def __init__(
        self,
        amqp_url: str = "amqp://guest:guest@localhost/",
        queue_name: str = "price_queue",
        verbose: bool = False
    ):
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.verbose = verbose
        self.stats = MessageStats()
        self.start_time = datetime.now()
        self.running = False
    
    async def consume_messages(
        self,
        limit: Optional[int] = None,
        format_json: bool = True,
        show_stats: bool = True
    ):
        """
        Consume and display messages from queue
        
        Args:
            limit: Maximum messages to consume (None = unlimited)
            format_json: Pretty print JSON messages
            show_stats: Show statistics periodically
        """
        print(f"{Fore.GREEN}📥 Starting consumer for queue: {self.queue_name}{Style.RESET_ALL}")
        
        connection = await aio_pika.connect_robust(self.amqp_url)
        channel = await connection.channel()
        queue = await channel.declare_queue(self.queue_name, durable=True)
        
        self.running = True
        stats_task = None
        
        try:
            if show_stats:
                stats_task = asyncio.create_task(self._show_periodic_stats())
            
            message_count = 0
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        message_count += 1
                        self.stats.total += 1
                        self.stats.last_message = datetime.now()
                        
                        # Decode message
                        body = message.body.decode()
                        
                        # Try to parse as JSON
                        try:
                            data = json.loads(body)
                            msg_type = data.get('type', data.get('ticker', 'unknown'))
                            self.stats.by_type[msg_type] = self.stats.by_type.get(msg_type, 0) + 1
                            
                            if format_json:
                                formatted = json.dumps(data, indent=2)
                                print(f"\n{Fore.CYAN}Message #{message_count}:{Style.RESET_ALL}")
                                print(f"{Fore.YELLOW}{formatted}{Style.RESET_ALL}")
                            else:
                                print(f"📨 #{message_count}: {body}")
                        except json.JSONDecodeError:
                            print(f"📨 #{message_count}: {body}")
                        
                        # Show headers if verbose
                        if self.verbose and message.headers:
                            print(f"  Headers: {dict(message.headers)}")
                        
                        # Check limit
                        if limit and message_count >= limit:
                            print(f"\n{Fore.GREEN}✅ Reached limit of {limit} messages{Style.RESET_ALL}")
                            break
                        
                        if not self.running:
                            break
                            
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}⚠️ Consumer interrupted{Style.RESET_ALL}")
        finally:
            self.running = False
            if stats_task:
                stats_task.cancel()
                try:
                    await stats_task
                except asyncio.CancelledError:
                    pass
            
            await connection.close()
            self._show_final_stats()
    
    async def produce_test_messages(
        self,
        count: int = 10,
        message_type: str = "price",
        delay: float = 0.1
    ):
        """
        Produce test messages to queue
        
        Args:
            count: Number of messages to send
            message_type: Type of test message (price, signal, custom)
            delay: Delay between messages in seconds
        """
        print(f"{Fore.GREEN}📤 Producing {count} test messages to {self.queue_name}{Style.RESET_ALL}")
        
        connection = await aio_pika.connect_robust(self.amqp_url)
        channel = await connection.channel()
        queue = await channel.declare_queue(self.queue_name, durable=True)
        
        try:
            for i in range(count):
                # Generate test message
                if message_type == "price":
                    message_data = {
                        "ticker": f"TEST{i % 5}",
                        "price": 100 + (i * 0.5),
                        "volume": 1000 * (i + 1),
                        "timestamp": datetime.now().isoformat()
                    }
                elif message_type == "signal":
                    message_data = {
                        "signal_id": f"sig_{i}",
                        "ticker": f"TEST{i % 5}",
                        "action": "buy" if i % 2 == 0 else "sell",
                        "price": 100 + (i * 0.5),
                        "confidence": 50 + (i % 50),
                        "timestamp": datetime.now().isoformat()
                    }
                else:
                    message_data = {
                        "id": i,
                        "test": True,
                        "message": f"Test message {i}",
                        "timestamp": datetime.now().isoformat()
                    }
                
                # Send message
                message = aio_pika.Message(
                    body=json.dumps(message_data).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                )
                
                await channel.default_exchange.publish(
                    message,
                    routing_key=self.queue_name
                )
                
                print(f"  ✅ Sent message {i+1}/{count}: {message_data.get('ticker', 'test')}")
                
                if delay > 0:
                    await asyncio.sleep(delay)
                    
        finally:
            await connection.close()
            print(f"{Fore.GREEN}✅ Produced {count} messages{Style.RESET_ALL}")
    
    async def monitor_queue(self, interval: int = 5):
        """
        Monitor queue statistics without consuming
        
        Args:
            interval: Update interval in seconds
        """
        print(f"{Fore.GREEN}📊 Monitoring queue: {self.queue_name}{Style.RESET_ALL}")
        
        connection = await aio_pika.connect_robust(self.amqp_url)
        channel = await connection.channel()
        
        try:
            while True:
                queue = await channel.declare_queue(self.queue_name, durable=True, passive=True)
                
                print(f"\n{Fore.CYAN}Queue Status at {datetime.now().strftime('%H:%M:%S')}:{Style.RESET_ALL}")
                print(f"  Messages: {queue.declaration_result.message_count}")
                print(f"  Consumers: {queue.declaration_result.consumer_count}")
                
                await asyncio.sleep(interval)
                
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}⚠️ Monitoring stopped{Style.RESET_ALL}")
        finally:
            await connection.close()
    
    async def peek_messages(self, count: int = 5):
        """
        Peek at messages without consuming them
        
        Args:
            count: Number of messages to peek at
        """
        print(f"{Fore.GREEN}👀 Peeking at {count} messages in {self.queue_name}{Style.RESET_ALL}")
        
        connection = await aio_pika.connect_robust(self.amqp_url)
        channel = await connection.channel()
        queue = await channel.declare_queue(self.queue_name, durable=True)
        
        messages_peeked = 0
        
        try:
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    messages_peeked += 1
                    
                    # Display message without acknowledging
                    body = message.body.decode()
                    try:
                        data = json.loads(body)
                        formatted = json.dumps(data, indent=2)
                        print(f"\n{Fore.CYAN}Message #{messages_peeked}:{Style.RESET_ALL}")
                        print(f"{Fore.YELLOW}{formatted}{Style.RESET_ALL}")
                    except json.JSONDecodeError:
                        print(f"📨 #{messages_peeked}: {body}")
                    
                    # Don't acknowledge - just cancel
                    await message.nack(requeue=True)
                    
                    if messages_peeked >= count:
                        break
                        
        finally:
            await connection.close()
            print(f"\n{Fore.GREEN}✅ Peeked at {messages_peeked} messages (not consumed){Style.RESET_ALL}")
    
    async def purge_queue(self, confirm: bool = True):
        """
        Purge all messages from queue
        
        Args:
            confirm: Ask for confirmation before purging
        """
        if confirm:
            response = input(f"{Fore.RED}⚠️ Are you sure you want to purge {self.queue_name}? (yes/no): {Style.RESET_ALL}")
            if response.lower() != 'yes':
                print("Cancelled")
                return
        
        connection = await aio_pika.connect_robust(self.amqp_url)
        channel = await connection.channel()
        queue = await channel.declare_queue(self.queue_name, durable=True)
        
        message_count = queue.declaration_result.message_count
        await queue.purge()
        
        await connection.close()
        print(f"{Fore.GREEN}✅ Purged {message_count} messages from {self.queue_name}{Style.RESET_ALL}")
    
    async def stress_test(
        self,
        messages_per_second: int = 100,
        duration: int = 10
    ):
        """
        Stress test the queue with high message volume
        
        Args:
            messages_per_second: Target message rate
            duration: Test duration in seconds
        """
        total_messages = messages_per_second * duration
        delay = 1.0 / messages_per_second if messages_per_second > 0 else 0
        
        print(f"{Fore.GREEN}🚀 Stress testing: {messages_per_second} msg/s for {duration}s{Style.RESET_ALL}")
        print(f"  Total messages: {total_messages}")
        
        connection = await aio_pika.connect_robust(self.amqp_url)
        channel = await connection.channel()
        queue = await channel.declare_queue(self.queue_name, durable=True)
        
        start_time = datetime.now()
        messages_sent = 0
        
        try:
            for i in range(total_messages):
                message_data = {
                    "test_id": i,
                    "timestamp": datetime.now().isoformat(),
                    "stress_test": True
                }
                
                message = aio_pika.Message(
                    body=json.dumps(message_data).encode()
                )
                
                await channel.default_exchange.publish(
                    message,
                    routing_key=self.queue_name
                )
                
                messages_sent += 1
                
                if messages_sent % 100 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    actual_rate = messages_sent / elapsed if elapsed > 0 else 0
                    print(f"  Progress: {messages_sent}/{total_messages} ({actual_rate:.1f} msg/s)")
                
                if delay > 0:
                    await asyncio.sleep(delay)
                    
        finally:
            await connection.close()
            
            elapsed = (datetime.now() - start_time).total_seconds()
            actual_rate = messages_sent / elapsed if elapsed > 0 else 0
            
            print(f"\n{Fore.GREEN}✅ Stress test completed:{Style.RESET_ALL}")
            print(f"  Messages sent: {messages_sent}")
            print(f"  Duration: {elapsed:.1f}s")
            print(f"  Actual rate: {actual_rate:.1f} msg/s")
    
    async def _show_periodic_stats(self):
        """Show statistics periodically"""
        while self.running:
            await asyncio.sleep(10)
            if self.stats.total > 0:
                self._show_stats()
    
    def _show_stats(self):
        """Display current statistics"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = self.stats.total / elapsed if elapsed > 0 else 0
        
        print(f"\n{Fore.CYAN}📊 Statistics:{Style.RESET_ALL}")
        print(f"  Total messages: {self.stats.total}")
        print(f"  Rate: {rate:.2f} msg/s")
        
        if self.stats.by_type:
            print(f"  By type: {dict(self.stats.by_type)}")
    
    def _show_final_stats(self):
        """Display final statistics"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        
        print(f"\n{Fore.GREEN}📊 Final Statistics:{Style.RESET_ALL}")
        print(f"  Total messages: {self.stats.total}")
        print(f"  Duration: {elapsed:.1f}s")
        
        if self.stats.total > 0:
            rate = self.stats.total / elapsed
            print(f"  Average rate: {rate:.2f} msg/s")
            
            if self.stats.by_type:
                print(f"  Message types:")
                for msg_type, count in sorted(self.stats.by_type.items()):
                    print(f"    {msg_type}: {count}")

async def main():
    """Main entry point with CLI"""
    parser = argparse.ArgumentParser(description="RabbitMQ Testing Suite")
    parser.add_argument(
        "mode",
        type=str,
        choices=["consume", "produce", "monitor", "peek", "purge", "stress"],
        help="Testing mode"
    )
    parser.add_argument(
        "--queue",
        type=str,
        default="price_queue",
        help="Queue name (default: price_queue)"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of messages (for produce/peek)"
    )
    parser.add_argument(
        "--type",
        type=str,
        default="price",
        choices=["price", "signal", "custom"],
        help="Message type for produce mode"
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=100,
        help="Messages per second for stress test"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=10,
        help="Duration in seconds for stress test"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose output"
    )
    
    args = parser.parse_args()
    
    # Create tester
    tester = RabbitMQTester(
        queue_name=args.queue,
        verbose=args.verbose
    )
    
    # Execute mode
    if args.mode == "consume":
        await tester.consume_messages()
    elif args.mode == "produce":
        await tester.produce_test_messages(
            count=args.count,
            message_type=args.type
        )
    elif args.mode == "monitor":
        await tester.monitor_queue()
    elif args.mode == "peek":
        await tester.peek_messages(count=args.count)
    elif args.mode == "purge":
        await tester.purge_queue()
    elif args.mode == "stress":
        await tester.stress_test(
            messages_per_second=args.rate,
            duration=args.duration
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Interrupted by user{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")