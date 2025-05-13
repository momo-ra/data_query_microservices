from sqlalchemy import text
from database import engine, SessionLocal
from services.caching_services import get_cached_data, set_cached_data
from utils.log import setup_logger
from utils.convert_timestamp import convert_timestamp_format
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from typing import Optional
from utils.response import success_response, error_response

logger = setup_logger(__name__)

# Get all tags
GET_ALL_TAGS = text("""
    SELECT id, name 
    FROM tag
    ORDER BY id
""")

# Get tag data by ID
GET_TAG_BY_ID = text("""
    SELECT *
    FROM tag
    WHERE id = :tag_id
""")

# Get historical tag data - Fix PostgreSQL parameter binding for arrays
GET_HISTORICAL_TAG_DATA = text("""
    SELECT t.id as tag_id, t.name as tag_name, ts.value, ts.timestamp
    FROM time_series ts
    JOIN tag t ON ts.tag_id = t.id
    WHERE ts.tag_id = ANY(:tag_ids)
      AND ts.timestamp BETWEEN :start_time AND :end_time
    ORDER BY ts.timestamp DESC
    LIMIT :limit
""")

# Get latest tag values - Fix PostgreSQL parameter binding for arrays
GET_LATEST_TAG_VALUES = text("""
    SELECT DISTINCT ON (tag_id) 
        tag_id, value, timestamp
    FROM time_series
    WHERE tag_id = ANY(:tag_ids)
    ORDER BY tag_id, timestamp DESC
""")

# Get trends data
GET_TRENDS_DATA = text("""
    SELECT tag_id, timestamp, value
    FROM time_series
    WHERE tag_id IN (:tag_id_1, :tag_id_2)
      AND timestamp BETWEEN :start_time AND :end_time
    ORDER BY tag_id, timestamp
""")

GET_POLLING_TAGS = text("""
    SELECT t.name as tag_name, t.id as tag_id, t.description as description, t.unit_of_measure as unit_of_measure
    FROM polling_tasks p 
    JOIN tag t ON t.id = p.tag_id
    WHERE p.is_active = true
    ORDER BY t.name
""")

async def get_tag_data_with_tag_id(tag_id: int):
    """Retrieve tag data with tag ID with caching."""
    if not tag_id:
        logger.warning("Tag ID is required for get_tag_data_with_tag_id")
        return {"error": "Tag ID is required"}

    query_key = f"tag_data_{tag_id}"
    logger.info(f"Attempting to retrieve data for query key: {query_key}")
    # cached_data = await get_cached_data(query_key)

    # if cached_data:
    #     logger.success(f"Cache hit for query key: {query_key}")
    #     return cached_data

    async with engine.connect() as conn:
        try:
            result = await conn.execute(GET_TAG_BY_ID, {"tag_id": tag_id})
            rows = result.mappings().all()
            data = [dict(row) for row in rows]
            # set_cached_data(query_key, data)
            logger.success(f"Data retrieved and cached for tag_id {tag_id}. Rows: {len(data)}")
            return success_response(data)
        except Exception as e:
            logger.error(f"Error executing query for tag_id {tag_id}: {e}")
            return {"error": f"Database error retrieving data for tag {tag_id}"}

async def get_all_tag_data():
    """Retrieve all tag data with caching."""
    query_key = "all_tag_data"
    logger.info(f"Attempting to retrieve data for query key: {query_key}")
    # cached_data = await get_cached_data(query_key)

    # if cached_data:
    #     logger.success(f"Cache hit for query key: {query_key}")
    #     return cached_data

    async with engine.connect() as conn:
        try:
            result = await conn.execute(GET_ALL_TAGS)
            print(result)
            rows = result.mappings().all()
            data = [dict(row) for row in rows]
            # await set_cached_data(query_key, data)
            logger.success(f"All tag data retrieved and cached. Rows: {len(data)}")
            return success_response(data)
        except Exception as e:
            logger.error(f"Error executing query for all tags: {e}")
            return {"error": "Database error retrieving all tags"}

async def get_trends_data(db: AsyncSession, tag_ids: dict, start_time: str, end_time: str):
    """Get trends data for two given tag IDs and time range (Async)."""
    tag_id_1 = tag_ids.get("tag_id_1")
    tag_id_2 = tag_ids.get("tag_id_2")

    if tag_id_1 is None or tag_id_2 is None:
        logger.error("Both tag_id_1 and tag_id_2 are required for trends.")
        return {"error": "Both tag_id_1 and tag_id_2 are required."}

    try:
        start_time_dt = datetime.fromisoformat(start_time.strip())
        end_time_dt = datetime.fromisoformat(end_time.strip())
    except ValueError as e:
        logger.error(f"Invalid timestamp format for trends: {e}")
        return {"error": f"Invalid timestamp format: {e}"}

    try:
        async with db as session:
            result = await session.execute(
                GET_TRENDS_DATA, 
                {
                    "tag_id_1": tag_id_1,
                    "tag_id_2": tag_id_2,
                    "start_time": start_time_dt,
                    "end_time": end_time_dt
                }
            )
            rows = result.mappings().all()

        data_by_tag = {str(tag_id_1): [], str(tag_id_2): []}
        for row in rows:
            row_dict = dict(row)
            current_tag_id = str(row_dict['tag_id'])
            if current_tag_id in data_by_tag:
                data_by_tag[current_tag_id].append(row_dict)

        logger.info(f"Trends data retrieved for tags {tag_id_1}, {tag_id_2}. Counts: {len(data_by_tag[str(tag_id_1)])}, {len(data_by_tag[str(tag_id_2)])}")
        return {
            "tag_data": data_by_tag,
            "tag_counts": {
                str(tag_id_1): len(data_by_tag[str(tag_id_1)]),
                str(tag_id_2): len(data_by_tag[str(tag_id_2)])
            }
        }
    except Exception as e:
        logger.error(f"Error executing trends query for tags {tag_id_1}, {tag_id_2}: {e}")
        return {"error": f"Database error retrieving trends data: {e}"}

async def get_historical_tag_data(
    tags: list, 
    start_dt: datetime, 
    end_dt: datetime, 
    user_id: Optional[int] = None
):
    """
    Retrieve historical data for a list of tags (by name) within a specific datetime range.
    
    Args:
        tags: List of tag IDs or names
        start_dt: Start date/time for data query
        end_dt: End date/time for data query
        user_id: Optional user ID for audit logging
        
    Returns:
        Dictionary of tag data grouped by tag
    """
    if not tags:
        logger.warning("No tags provided for get_historical_tag_data")
        return {}

    # Add user context to logs if available
    user_context = f" for user {user_id}" if user_id else ""
    logger.info(f"Fetching historical data{user_context} for tags: {tags} between {start_dt} and {end_dt}")
    
    try:
        # Process query using tag IDs
        numeric_tag_ids = [int(tag) for tag in tags if isinstance(tag, (int, str)) and (isinstance(tag, int) or tag.isdigit())]
        
        if not numeric_tag_ids:
            logger.warning(f"No valid tag IDs found in {tags}")
            return {}
            
        async with SessionLocal() as session:
            # Fetch the historical data for the tags
            result = await session.execute(
                GET_HISTORICAL_TAG_DATA,
                {
                    "tag_ids": numeric_tag_ids,
                    "start_time": start_dt,
                    "end_time": end_dt,
                    "limit": 1000  # Max number of data points to return
                }
            )
            
            tag_data_rows = result.mappings().all()
            logger.success(f"Retrieved {len(tag_data_rows)} historical data points{user_context} across {len(numeric_tag_ids)} tags")
            
            # Group by tag for easier consumption by frontend
            grouped_data = {}
            for row in tag_data_rows:
                tag_id = str(row['tag_id'])
                if tag_id not in grouped_data:
                    grouped_data[tag_id] = {
                        "tag_id": tag_id,
                        "name": row['tag_name'],
                        "values": []
                    }
                
                # Add this data point
                grouped_data[tag_id]["values"].append({
                    "timestamp": row['timestamp'].isoformat() if isinstance(row['timestamp'], datetime) else row['timestamp'],
                    "value": row['value']
                })
            
            # If we have no data for some tags, include them with empty arrays
            for tag_id in numeric_tag_ids:
                str_tag_id = str(tag_id)
                if str_tag_id not in grouped_data:
                    # Get tag name from the database
                    tag_result = await session.execute(
                        GET_TAG_BY_ID,
                        {"tag_id": tag_id}
                    )
                    tag_row = tag_result.first()
                    tag_name = tag_row[1] if tag_row else f"Unknown ({tag_id})"
                    
                    grouped_data[str_tag_id] = {
                        "tag_id": str_tag_id,
                        "name": tag_name,
                        "values": []
                    }
            
            # If requested time range returns no data, get the latest value before the range
            # This helps show "flat lines" for unchanged values
            tags_with_no_data = [tag_id for tag_id, data in grouped_data.items() if not data["values"]]
            if tags_with_no_data:
                logger.info(f"Fetching latest values before range for {len(tags_with_no_data)} empty tags")
                latest_result = await session.execute(
                    GET_LATEST_TAG_VALUES,
                    {"tag_ids": [int(tag_id) for tag_id in tags_with_no_data]}
                )
                latest_rows = latest_result.mappings().all()
                
                for row in latest_rows:
                    tag_id = str(row['tag_id'])
                    if tag_id in grouped_data:
                        grouped_data[tag_id]["values"].append({
                            "timestamp": row['timestamp'].isoformat() if isinstance(row['timestamp'], datetime) else row['timestamp'],
                            "value": row['value'],
                            "is_latest_before_range": True  # Flag for frontend
                        })
            
            return grouped_data
            
    except Exception as e:
        logger.error(f"Error in get_historical_tag_data{user_context}: {e}")
        # Return empty dict instead of error to avoid breaking the WebSocket connection
        return {}

async def get_polling_tags(db, current_user):
    try:
        if not current_user:
            return error_response("Token Not Valid, Token Required")
        async with db as session:
            result = await session.execute(GET_POLLING_TAGS)
            rows = result.mappings().all()
            
            formatted_tags = []
            for row in rows:
                # The query returns tag_id and tag_name columns
                tag = {
                    "id": str(row["tag_id"]),
                    "name": row["tag_name"],
                    "description": "",  # Default value
                    "timestamp": datetime.now().isoformat(),  # Current time as default
                    "value": "0",  # Default value
                    "unit_of_measure": ""  # Default value
                }
                formatted_tags.append(tag)
                
            return formatted_tags
    except Exception as e:
        error_msg = f"Error fetching polling tags: {str(e)}"
        logger.error(error_msg)
        return error_response(error_msg)
