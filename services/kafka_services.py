from aiokafka import AIOKafkaConsumer #type: ignore
from utils.log import setup_logger
import os
import json
import asyncio
import uuid
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv('.env', override=True)
kafka_topic = os.getenv('KAFKA_TOPIC')
kafka_broker = os.getenv('KAFKA_BROKER') 
# kafka_broker = "localhost:19092"
logger = setup_logger(__name__)


class KafkaServices:

    def __init__(self) -> None:
        self.kafka_topic = kafka_topic
        self.kafka_broker = kafka_broker
        self.is_started = False
        self.consumer = None
        self.connection_retries = 0
        self.max_retries = 5
        self.retry_delay = 5  # seconds
        self.batch_size = 10  # Process messages in batches for better performance
        self._init_consumer()
        
        # Initialize data structures for distribution
        self._message_queue = asyncio.Queue()
        self._tag_subscribers = defaultdict(set)  # Dictionary storing subscribers by tag
        self._subscriber_queues = {}  # Dictionary storing message queue for each subscriber
        self._consumer_task = None
        self._distributor_task = None
        
    def _init_consumer(self):
        if not self.kafka_topic or not self.kafka_broker:
            logger.error("Kafka configuration missing: topic or broker not set in environment")
            return False
            
        try:
            self.consumer = AIOKafkaConsumer(
                self.kafka_topic, 
                bootstrap_servers=self.kafka_broker, 
                group_id='opc-group', 
                auto_offset_reset='latest',
                enable_auto_commit=True,
                max_poll_records=self.batch_size,
                consumer_timeout_ms=1000  # Short timeout for faster detection of client disconnects
            )
            logger.info(f"Initialized Kafka consumer for topic {self.kafka_topic} on broker {self.kafka_broker}")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            return False
        
    async def start(self):
        if not self.consumer:
            if not self._init_consumer():
                return False
                
        try:
            await self.consumer.start()
            self.is_started = True
            self.connection_retries = 0  # Reset retries on successful connection
            logger.info("Kafka consumer started successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to start Kafka consumer: {e}")
            return False
            
    async def stop(self):
        if self.consumer and self.is_started:
            try:
                await self.consumer.stop()
                self.is_started = False
                logger.info('Kafka consumer stopped')
                return True
            except Exception as e:
                logger.error(f"Error stopping Kafka consumer: {e}")
                return False
        return True

    async def start_background_tasks(self):
        """Start core tasks for consumption and distribution"""
        # Start task for consuming messages from Kafka
        if self._consumer_task is None or self._consumer_task.done():
            self._consumer_task = asyncio.create_task(self._consume_messages())
            logger.info("Started Kafka consumer task")
            
        # Start task for distributing messages to subscribers
        if self._distributor_task is None or self._distributor_task.done():
            self._distributor_task = asyncio.create_task(self._distribute_messages())
            logger.info("Started message distributor task")
            
    async def _consume_messages(self):
        """Consume messages from Kafka and put them in the queue"""
        if not self.is_started:
            success = await self.start()
            if not success:
                logger.error("Failed to start Kafka consumer")
                return
                
        logger.info("Consumer task is running and waiting for messages")
        try:
            while True:
                try:
                    async for message in self.consumer:
                        try:
                            message_value = message.value.decode('utf-8')
                            message_json = json.loads(message_value)
                            
                            # Put the message in the queue
                            logger.info(f"Received message from Kafka: {message_json.get('tag_id', 'unknown tag')}")
                            await self._message_queue.put(message_json)
                        except json.JSONDecodeError:
                            logger.error(f"Failed to parse message as JSON: {message_value}")
                        except Exception as e:
                            logger.error(f"Error processing Kafka message: {e}")
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error consuming Kafka messages: {e}")
                    await asyncio.sleep(5)  # Wait before trying again
        except asyncio.CancelledError:
            logger.info("Kafka consumer task cancelled")
            raise
    
    async def _distribute_messages(self):
        """Distribute messages from the queue to subscribers"""
        logger.info("Distributor task is running and waiting for messages")
        try:
            while True:
                # Wait for a new message
                message = await self._message_queue.get()
                
                # Extract tag ID
                tag_id = message.get("tag_id")
                if not tag_id:
                    logger.warning(f"Message without tag_id: {message}")
                    self._message_queue.task_done()
                    continue
                
                # IMPORTANT: Just forward the message as-is to subscribers
                # dashboard_services.py expects the raw message with tag_id
                
                # Send the message to subscribers of this tag
                subscribers = self._tag_subscribers.get(tag_id, set())
                
                if not subscribers:
                    logger.warning(f"No subscribers for tag {tag_id}")
                
                for subscriber_id in subscribers:
                    if subscriber_id in self._subscriber_queues:
                        await self._subscriber_queues[subscriber_id].put(message)
                    else:
                        logger.warning(f"Subscriber {subscriber_id} has no queue")
                
                self._message_queue.task_done()
        except asyncio.CancelledError:
            logger.info("Message distributor task cancelled")
            raise
            
    async def subscribe_to_tags(self, tags, max_queue_size=1000):
        """Subscribe to a set of tags"""
        # Create a unique ID for the subscriber
        subscriber_id = str(uuid.uuid4())
        
        # Create a queue for the subscriber
        self._subscriber_queues[subscriber_id] = asyncio.Queue(maxsize=max_queue_size)
        
        # Add the subscriber to all requested tags
        for tag in tags:
            self._tag_subscribers[tag].add(subscriber_id)
            logger.info(f"Added subscriber {subscriber_id} to tag {tag}")
            
        # Ensure core tasks are started
        await self.start_background_tasks()
        
        logger.info(f"Subscriber {subscriber_id} subscribed to {len(tags)} tags")
        return subscriber_id
        
    async def unsubscribe(self, subscriber_id):
        """Unsubscribe a subscriber"""
        # Remove the subscriber from all tags
        for tag, subscribers in self._tag_subscribers.items():
            if subscriber_id in subscribers:
                subscribers.remove(subscriber_id)
                
        # Remove the subscriber's queue
        if subscriber_id in self._subscriber_queues:
            self._subscriber_queues.pop(subscriber_id)
            
        logger.info(f"Subscriber {subscriber_id} unsubscribed")
        
    async def get_messages(self, subscriber_id, timeout=0.5):
        """
        Get available messages for the subscriber.
        Returns a list of available messages (can be empty).
        """
        if subscriber_id not in self._subscriber_queues:
            logger.warning(f"Attempted to get messages for unknown subscriber: {subscriber_id}")
            return []
            
        queue = self._subscriber_queues[subscriber_id]
        messages = []
        
        # Collect all available messages (maximum 20 messages)
        try:
            # Try to get the first message with timeout
            message = await asyncio.wait_for(queue.get(), timeout=timeout)
            messages.append(message)
            queue.task_done()
            
            # Collect any additional messages available immediately (without waiting)
            for _ in range(19):  # Maximum 19 additional messages
                if queue.empty():
                    break
                message = queue.get_nowait()
                messages.append(message)
                queue.task_done()
                
            if messages:
                logger.info(f"Retrieved {len(messages)} messages for subscriber {subscriber_id}")
        except asyncio.TimeoutError:
            # No messages available, this is normal
            pass
        except Exception as e:
            logger.error(f"Error getting messages for subscriber {subscriber_id}: {e}")
            
        return messages
    
    async def consume_forever(self):
        """
        Replaced with the new subscription model.
        Remains for compatibility with old code.
        """
        # Warning - use the new system
        logger.warning("consume_forever is deprecated. Use subscribe_to_tags and get_messages instead.")
        
        # Create a subscription for all tags (inefficient behavior)
        all_tags = list(self._tag_subscribers.keys())
        subscriber_id = await self.subscribe_to_tags(all_tags if all_tags else ["all"])
        
        try:
            while True:
                messages = await self.get_messages(subscriber_id)
                for message in messages:
                    yield message
                    
                # Short wait to avoid excessive resource consumption
                if not messages:
                    await asyncio.sleep(0.1)
        finally:
            # Unsubscribe when exiting
            await self.unsubscribe(subscriber_id)

kafka_services = KafkaServices()