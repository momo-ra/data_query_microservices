from sqlalchemy import text, bindparam
from database import engine
from services.caching_services import get_cached_data, set_cached_data
from utils.log import setup_logger
from utils.convert_timestamp import convert_timestamp_format

logger = setup_logger(__name__)

def get_table_data(table_name: str, start_time: str = None, end_time: str = None, limit: int = 100):
    """Retrieve table data with filtering and caching."""
    query_key = f"{table_name}_{start_time}_{end_time}_{limit}"
    logger.info(f"Attempting to retrieve data for query key: {query_key}")
    cached_data = get_cached_data(query_key)

    if cached_data:
        logger.success(f"Cache hit for query key: {query_key}")
        return cached_data

    with engine.connect() as conn:
        if not table_name.isalnum() and '_' not in table_name:
            logger.error(f"Invalid table name attempt: {table_name}")
            return {"error": "Invalid table name"}

        query = f"SELECT * FROM {table_name}"
        conditions = []
        params = {}

        try:
            if start_time:
                start_time_converted = convert_timestamp_format(start_time)
                conditions.append("timestamp >= :start_time")
                params["start_time"] = start_time_converted
            if end_time:
                end_time_converted = convert_timestamp_format(end_time)
                conditions.append("timestamp <= :end_time")
                params["end_time"] = end_time_converted
        except ValueError as e:
            logger.error(f"Error in timestamp conversion: {e}")
            return {"error": str(e)}

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY timestamp DESC LIMIT :limit"
        params["limit"] = limit

        logger.info(f"Executing query: {query} with params: {params}")
        try:
            result = conn.execute(text(query), params)
            rows = result.mappings().all()
            data = [dict(row) for row in rows]
            set_cached_data(query_key, data)
            logger.success(f"Data retrieved for table {table_name}. Rows: {len(data)}")
            return data
        except Exception as e:
            logger.error(f"Error executing query for table {table_name}: {e}")
            return {"error": f"Database error retrieving data for {table_name}"} 