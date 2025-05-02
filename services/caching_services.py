import redis
import json
from core.config import settings
from utils.log import setup_logger
from datetime import datetime

logger = setup_logger(__name__)

# Initialize Redis client
cache = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0)

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON Encoder to convert datetime objects to strings."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

async def get_cached_data(query_key):
    """Retrieve cached query results from Redis."""
    cached_result = await cache.get(query_key)
    if not cached_result:
        logger.warn(f"Cache miss for query key: {query_key}")
        return None
    try:
        return json.loads(cached_result)
    except json.JSONDecodeError as e:
        logger.danger(f"Error decoding cached data for key {query_key}: {e}")
        return None

async def set_cached_data(query_key, data, expiration=600):
    """Store query results in Redis with an expiration time."""
    try:
        serialized_data = json.dumps(data, cls=DateTimeEncoder)
        await cache.set(query_key, serialized_data, ex=expiration)
    except TypeError as e:
        logger.danger(f"Error serializing data for caching: {e}")