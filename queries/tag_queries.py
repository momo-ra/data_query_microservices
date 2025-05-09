from sqlalchemy import text

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
