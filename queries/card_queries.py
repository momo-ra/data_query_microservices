from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from utils.log import setup_logger
from utils.time_utils import parse_relative_time
from utils.response_model import success_response, error_response
from middleware.permission_middleware import check_permission, can_access_card
from datetime import datetime

logger = setup_logger(__name__)

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

async def get_user_cards(db: AsyncSession, user_id: int, current_user: dict):
    """Retrieve all cards for a specific user with their associated tags"""
    try:
        # Permission check - allow if it's your own cards or if you have admin role
        auth_user_id = current_user.get("user_id")
        roles = current_user.get("roles", [])
        
        if auth_user_id != user_id and "admin" not in roles:
            has_permission = await check_permission("view_any_user_cards", db, auth_user_id)
            if not has_permission:
                return await error_response("Not authorized to view this user's cards", status_code=403)
        
        # Query all active cards for this user
        result = await db.execute(GET_USER_CARDS, {"user_id": user_id})
        rows = result.mappings().all()
        
        # Group by card_id to collect all tags for each card
        cards_dict = {}
        for row in rows:
            card_id = row["id"]
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
        logger.debug(f"Retrieved {len(cards_list)} cards for user {user_id}")
        
        response = await success_response(cards_list)
        return response
        
    except Exception as e:
        logger.error(f"Error getting cards for user {user_id}: {e}")
        response = await error_response(f"Database error: {str(e)}", status_code=500)
        return response

async def create_user_card(db: AsyncSession, user_id: int, card: dict, current_user: dict):
    """Create a new card for a user"""
    try:
        # Permission check - allow if it's your card or you have admin role
        auth_user_id = current_user.get("user_id")
        roles = current_user.get("roles", [])
        
        if auth_user_id != user_id and "admin" not in roles:
            has_permission = await check_permission("create_cards_for_any_user", db, auth_user_id)
            if not has_permission:
                response = await error_response("Not authorized to create cards for this user", status_code=403)
                return response
                
        # Validate input
        if "tags" not in card or not isinstance(card["tags"], list) or not card["tags"]:
            response = await error_response("Tags must be provided as a non-empty list", status_code=400)
            return response
            
        # Get graph_type_id or use default
        graph_type_id = card.get("graph_type_id", 1)  # Default to first graph type
        
        # Parse start and end times
        start_time = parse_relative_time(card.get("startTime", "-1h"))
        end_time = parse_relative_time(card.get("endTime", "now"))
        
        # Create card
        result = await db.execute(
            CREATE_CARD, 
            {
                "user_id": user_id,
                "start_time": start_time,
                "end_time": end_time,
                "graph_type_id": graph_type_id
            }
        )
        card_id = result.scalar_one()
        
        # Associate tags with the card
        for tag_id in card["tags"]:
            await db.execute(ADD_TAG_TO_CARD, {"card_id": card_id, "tag_id": tag_id})
        
        await db.commit()
        
        logger.success(f"Created new card {card_id} for user {user_id} with {len(card['tags'])} tags")
        response = await success_response({"id": card_id, "status": "created"})
        return response
    
    except Exception as e:
        await db.rollback()
        logger.error(f"Error creating card for user {user_id}: {e}")
        response = await error_response(f"Database error: {str(e)}", status_code=500)
        return response

async def update_user_card(db: AsyncSession, card_id: int, card: dict):
    """Update an existing card for a user - supports flexible field updates"""
    try:
        # Build dynamic update parameters
        update_fields = []
        update_params = {"card_id": card_id}
        
        # Handle time fields with parsing if provided
        if "startTime" in card or "start_time" in card:
            time_str = card.get("startTime", card.get("start_time", "-1h"))
            update_params["start_time"] = parse_relative_time(time_str)
            update_fields.append("start_time = :start_time")
        
        if "endTime" in card or "end_time" in card:
            time_str = card.get("endTime", card.get("end_time", "now"))
            update_params["end_time"] = parse_relative_time(time_str)
            update_fields.append("end_time = :end_time")
        
        # Handle other direct fields
        if "is_active" in card:
            update_params["is_active"] = card["is_active"]
            update_fields.append("is_active = :is_active")
            
        if "graph_type_id" in card:
            update_params["graph_type_id"] = card["graph_type_id"]
            update_fields.append("graph_type_id = :graph_type_id")
        
        # Always update the updated_at timestamp
        update_fields.append("updated_at = CURRENT_TIMESTAMP")
        
        # Only perform update if we have fields to update
        if update_fields:
            update_query = f"""
                UPDATE card_data
                SET {', '.join(update_fields)}
                WHERE id = :card_id
                RETURNING id
            """
            
            result = await db.execute(text(update_query), update_params)
            updated_id = result.scalar_one_or_none()
            
            if not updated_id:
                response = await error_response(f"Card with ID {card_id} not found", status_code=404)
                return response
        
        # Only update tags if provided
        if "tags" in card and isinstance(card["tags"], list):
            # Delete existing tag associations
            await db.execute(DELETE_CARD_TAGS, {"card_id": card_id})
            
            # Associate new tags with the card
            for tag_id in card["tags"]:
                await db.execute(ADD_TAG_TO_CARD, {"card_id": card_id, "tag_id": tag_id})
            
            logger.success(f"Updated card {card_id} with {len(card['tags'])} tags")
        
        await db.commit()
        
        # Prepare result message with updated fields
        updated_fields = []
        if "startTime" in card or "start_time" in card:
            updated_fields.append("start_time")
        if "endTime" in card or "end_time" in card:
            updated_fields.append("end_time")
        if "is_active" in card:
            updated_fields.append(f"is_active={card['is_active']}")
        if "graph_type_id" in card:
            updated_fields.append(f"graph_type_id={card['graph_type_id']}")
        if "tags" in card:
            updated_fields.append("tags")
            
        result_msg = f"Updated card {card_id}: {', '.join(updated_fields)}"
        logger.success(result_msg)
        
        response = await success_response({
            "id": card_id, 
            "status": "updated", 
            "updated_fields": updated_fields
        })
        return response
    
    except Exception as e:
        await db.rollback()
        logger.error(f"Error updating card {card_id}: {e}")
        response = await error_response(f"Database error: {str(e)}", status_code=500)
        return response

async def delete_card(db: AsyncSession, card_id: int, current_user: dict):
    """Delete a card (or mark as inactive)"""
    try:
        # First check who owns this card
        owner_result = await db.execute(GET_CARD_OWNER, {"card_id": card_id})
        owner_row = owner_result.first()
        
        if not owner_row:
            response = await error_response(f"Card with ID {card_id} not found", status_code=404)
            return response
            
        card_owner_id = owner_row[0]
        auth_user_id = current_user.get("user_id")
        roles = current_user.get("roles", [])
        
        # Permission check - allow if it's your card or you have admin role
        if auth_user_id != card_owner_id and "admin" not in roles:
            has_permission = await check_permission("delete_any_user_cards", db, auth_user_id)
            if not has_permission:
                response = await error_response("Not authorized to delete this card", status_code=403)
                return response
        
        # Soft delete by setting is_active to false
        result = await db.execute(SOFT_DELETE_CARD, {"card_id": card_id})
        deleted_id = result.scalar_one_or_none()
        
        if not deleted_id:
            response = await error_response(f"Card with ID {card_id} not found", status_code=404)
            return response
        
        await db.commit()
        logger.success(f"Marked card {card_id} as inactive")
        response = await success_response({"id": card_id, "status": "deleted"})
        return response
    
    except Exception as e:
        await db.rollback()
        logger.error(f"Error deleting card {card_id}: {e}")
        response = await error_response(f"Database error: {str(e)}", status_code=500)
        return response
