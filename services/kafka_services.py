from aiokafka import AIOKafkaConsumer #type: ignore
from utils.log import setup_logger
import os
import json
import asyncio
from dotenv import load_dotenv

load_dotenv('.env', override=True)
kafka_topic = os.getenv('KAFKA_TOPIC')
kafka_broker = os.getenv('KAFKA_BROKER')
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

    async def consume_forever(self):
        """
        Generator that yields messages from Kafka.
        Designed to be non-blocking to allow for client disconnection detection.
        Yields a single message at a time and then returns control to the caller.
        """
        if not self.kafka_topic or not self.kafka_broker:
            logger.error("Kafka configuration missing: topic or broker not set in environment")
            return
            
        # Only attempt to get a few messages before yielding control back
        # to allow checking for client disconnection
        max_messages_per_batch = 10
        messages_processed = 0
            
        try:
            if not self.is_started:
                success = await self.start()
                if not success:
                    self.connection_retries += 1
                    if self.connection_retries > self.max_retries:
                        logger.error(f"Maximum connection retries ({self.max_retries}) reached")
                        return
                    wait_time = self.retry_delay * self.connection_retries
                    logger.info(f"Waiting {wait_time}s before retry {self.connection_retries}/{self.max_retries}")
                    await asyncio.sleep(wait_time)
                    return
            
            # Non-blocking check for messages with timeout
            try:
                async for message in self.consumer:
                    try:
                        message_value = message.value.decode('utf-8')
                        message_json = json.loads(message_value)
                        yield message_json
                        
                        messages_processed += 1
                        if messages_processed >= max_messages_per_batch:
                            break
                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse message as JSON: {message_value}")
                    except Exception as e:
                        logger.error(f"Error processing Kafka message: {e}")
            except asyncio.TimeoutError:
                # No messages available, just return control
                return
                
        except asyncio.CancelledError:
            logger.info("Kafka consumption cancelled")
            # Forward cancellation
            raise
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}")
            # Restart consumer after error
            await self.stop()
            self.is_started = False
            await asyncio.sleep(self.retry_delay)

kafka_services = KafkaServices()