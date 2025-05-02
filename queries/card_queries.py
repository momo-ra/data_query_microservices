from sqlalchemy import text

# Card retrieval query
GET_USER_CARDS = text("""
    SELECT 
        cd.id, cd.start_time, cd.end_time, cd.is_active,
        t.id as tag_id, t.name as tag_name
    FROM card_data cd
    JOIN card_data_tags cdt ON cd.id = cdt.card_data_id
    JOIN tag t ON cdt.tag_id = t.id
    WHERE cd.user_id = :user_id AND cd.is_active = true
    ORDER BY cd.id
""")

# Card creation query
CREATE_CARD = text("""
    INSERT INTO card_data (user_id, start_time, end_time, is_active, created_at, updated_at)
    VALUES (:user_id, :start_time, :end_time, true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    RETURNING id
""")

# Tag-card association query
ADD_TAG_TO_CARD = text("""
    INSERT INTO card_data_tags (card_data_id, tag_id)
    VALUES (:card_id, :tag_id)
""")

# Card update query
UPDATE_CARD = text("""
    UPDATE card_data
    SET start_time = :start_time, 
        end_time = :end_time,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = :card_id
    RETURNING id
""")

# Delete tags from card query
DELETE_CARD_TAGS = text("""
    DELETE FROM card_data_tags
    WHERE card_data_id = :card_id
""")

# Soft delete card query
SOFT_DELETE_CARD = text("""
    UPDATE card_data
    SET is_active = false, updated_at = CURRENT_TIMESTAMP
    WHERE id = :card_id
    RETURNING id
""")

# Get card owner query
GET_CARD_OWNER = text("""
    SELECT user_id FROM card_data 
    WHERE id = :card_id
""")

# Get card data with tags query
GET_CARD_WITH_TAGS = text("""
    SELECT cd.*, t.id as tag_id, t.name as tag_name, cd.user_id as owner_id
    FROM card_data cd
    JOIN card_data_tags cdt ON cd.id = cdt.card_data_id
    JOIN tag t ON cdt.tag_id = t.id
    WHERE cd.id = :card_id AND cd.is_active = true
""")
