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
# kafka_broker = os.getenv('KAFKA_BROKER')
kafka_broker = "localhost:19092"
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
        
        # إضافة هياكل البيانات للتوزيع
        self._message_queue = asyncio.Queue()
        self._tag_subscribers = defaultdict(set)  # قاموس يخزن المشتركين حسب التاج
        self._subscriber_queues = {}  # قاموس يخزن قائمة رسائل لكل مشترك
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
        """بدء المهام الأساسية للاستهلاك والتوزيع"""
        # بدء مهمة استهلاك الرسائل من كافكا
        if self._consumer_task is None or self._consumer_task.done():
            self._consumer_task = asyncio.create_task(self._consume_messages())
            
        # بدء مهمة توزيع الرسائل للمشتركين
        if self._distributor_task is None or self._distributor_task.done():
            self._distributor_task = asyncio.create_task(self._distribute_messages())
            
    async def _consume_messages(self):
        """استهلاك الرسائل من كافكا ووضعها في قائمة الانتظار"""
        if not self.is_started:
            success = await self.start()
            if not success:
                logger.error("Failed to start Kafka consumer")
                return
                
        try:
            while True:
                try:
                    async for message in self.consumer:
                        try:
                            message_value = message.value.decode('utf-8')
                            message_json = json.loads(message_value)
                            
                            # وضع الرسالة في قائمة الانتظار
                            await self._message_queue.put(message_json)
                        except json.JSONDecodeError:
                            logger.error(f"Failed to parse message as JSON")
                        except Exception as e:
                            logger.error(f"Error processing Kafka message: {e}")
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error consuming Kafka messages: {e}")
                    await asyncio.sleep(5)  # انتظار قبل المحاولة مرة أخرى
        except asyncio.CancelledError:
            logger.info("Kafka consumer task cancelled")
            raise
    
    async def _distribute_messages(self):
        """توزيع الرسائل من قائمة الانتظار للمشتركين"""
        try:
            while True:
                # انتظار رسالة جديدة
                message = await self._message_queue.get()
                
                # استخراج معرف التاج
                tag_id = message.get("tag_id")
                if not tag_id:
                    logger.warning(f"Message without tag_id: {message}")
                    self._message_queue.task_done()
                    continue
                
                # إرسال الرسالة للمشتركين في هذا التاج
                subscribers = self._tag_subscribers.get(tag_id, set())
                for subscriber_id in subscribers:
                    if subscriber_id in self._subscriber_queues:
                        await self._subscriber_queues[subscriber_id].put(message)
                
                self._message_queue.task_done()
        except asyncio.CancelledError:
            logger.info("Message distributor task cancelled")
            raise
            
    async def subscribe_to_tags(self, tags, max_queue_size=1000):
        """اشتراك في مجموعة من التاجات"""
        # إنشاء معرف فريد للمشترك
        subscriber_id = str(uuid.uuid4())
        
        # إنشاء قائمة انتظار للمشترك
        self._subscriber_queues[subscriber_id] = asyncio.Queue(maxsize=max_queue_size)
        
        # إضافة المشترك لكل التاجات المطلوبة
        for tag in tags:
            self._tag_subscribers[tag].add(subscriber_id)
            
        # ضمان بدء المهام الأساسية
        await self.start_background_tasks()
        
        logger.info(f"Subscriber {subscriber_id} subscribed to {len(tags)} tags")
        return subscriber_id
        
    async def unsubscribe(self, subscriber_id):
        """إلغاء اشتراك مشترك"""
        # إزالة المشترك من كل التاجات
        for tag, subscribers in self._tag_subscribers.items():
            if subscriber_id in subscribers:
                subscribers.remove(subscriber_id)
                
        # إزالة قائمة انتظار المشترك
        if subscriber_id in self._subscriber_queues:
            self._subscriber_queues.pop(subscriber_id)
            
        logger.info(f"Subscriber {subscriber_id} unsubscribed")
        
    async def get_messages(self, subscriber_id, timeout=0.5):
        """
        الحصول على الرسائل المتاحة للمشترك.
        يرجع قائمة بالرسائل المتاحة (ممكن تكون فارغة).
        """
        if subscriber_id not in self._subscriber_queues:
            logger.warning(f"Attempted to get messages for unknown subscriber: {subscriber_id}")
            return []
            
        queue = self._subscriber_queues[subscriber_id]
        messages = []
        
        # جمع كل الرسائل المتاحة (بحد أقصى 20 رسالة)
        try:
            # محاولة الحصول على الرسالة الأولى مع timeout
            message = await asyncio.wait_for(queue.get(), timeout=timeout)
            messages.append(message)
            queue.task_done()
            
            # جمع أي رسائل إضافية متاحة فورًا (بدون انتظار)
            for _ in range(19):  # الحد الأقصى 19 رسالة إضافية
                if queue.empty():
                    break
                message = queue.get_nowait()
                messages.append(message)
                queue.task_done()
        except asyncio.TimeoutError:
            # لا توجد رسائل متاحة، هذا طبيعي
            pass
        except Exception as e:
            logger.error(f"Error getting messages for subscriber {subscriber_id}: {e}")
            
        return messages
    
    async def consume_forever(self):
        """
        تم استبدالها بنموذج الاشتراك الجديد.
        تبقى للتوافق مع الكود القديم.
        """
        # تحذير - استخدم النظام الجديد
        logger.warning("consume_forever is deprecated. Use subscribe_to_tags and get_messages instead.")
        
        # إنشاء اشتراك لكل التاجات (سلوك غير فعال)
        all_tags = list(self._tag_subscribers.keys())
        subscriber_id = await self.subscribe_to_tags(all_tags if all_tags else ["all"])
        
        try:
            while True:
                messages = await self.get_messages(subscriber_id)
                for message in messages:
                    yield message
                    
                # انتظار قصير لتجنب الاستهلاك المفرط للموارد
                if not messages:
                    await asyncio.sleep(0.1)
        finally:
            # إلغاء الاشتراك عند الخروج
            await self.unsubscribe(subscriber_id)

kafka_services = KafkaServices()