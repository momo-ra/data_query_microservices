from sqlalchemy import text

# Get user's active cards with tags for dashboard
GET_USER_ACTIVE_CARDS_WITH_TAGS = text("""
    SELECT 
        cd.id as card_id, 
        cd.start_time, 
        cd.end_time, 
        cd.is_active,
        t.id as tag_id, 
        t.name as tag_name
    FROM card_data cd
    JOIN card_data_tags cdt ON cd.id = cdt.card_data_id
    JOIN tag t ON cdt.tag_id = t.id
    WHERE cd.user_id = :user_id AND cd.is_active = true
    ORDER BY cd.id
""")
