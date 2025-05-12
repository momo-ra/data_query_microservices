from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from utils.log import setup_logger
from datetime import datetime

logger = setup_logger(__name__)

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

async def get_user_active_cards(db: AsyncSession, user_id: int):
    """Get all active cards for a user's dashboard"""
    try:
        result = await db.execute(GET_USER_ACTIVE_CARDS_WITH_TAGS, {"user_id": user_id})
        rows = result.mappings().all()
        
        # Group by card_id to collect all tags for each card
        cards_dict = {}
        for row in rows:
            card_id = row["card_id"]
            if card_id not in cards_dict:
                cards_dict[card_id] = {
                    "id": card_id,
                    "start_time": row["start_time"].isoformat() if row["start_time"] else None,
                    "end_time": row["end_time"].isoformat() if row["end_time"] else None,
                    "is_active": row["is_active"],
                    "tags": []
                }
            
            # Add the tag to the card
            cards_dict[card_id]["tags"].append({
                "id": row["tag_id"],
                "name": row["tag_name"]
            })
        
        # Convert dictionary to list
        cards_list = list(cards_dict.values())
        logger.info(f"Retrieved {len(cards_list)} active cards for user {user_id}")
        
        return cards_list
    except Exception as e:
        logger.error(f"Error getting active cards for user {user_id}: {e}")
        return []
