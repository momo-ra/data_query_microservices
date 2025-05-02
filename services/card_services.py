from sqlalchemy.ext.asyncio import AsyncSession
from utils.log import setup_logger
from utils.time_utils import parse_relative_time
from fastapi import HTTPException, WebSocket, WebSocketDisconnect
from middleware.auth_middleware import authenticate_ws
from middleware.permission_middleware import check_permission, can_access_card
# from services.websocket_services import websocket_manager     
from services.query_services import get_historical_tag_data
from datetime import datetime
import json
from queries.card_queries import (
    GET_USER_CARDS, CREATE_CARD, ADD_TAG_TO_CARD, 
    UPDATE_CARD, DELETE_CARD_TAGS, SOFT_DELETE_CARD,
    GET_CARD_OWNER, GET_CARD_WITH_TAGS
)
from sqlalchemy import text
from utils.response_model import success_response, error_response

logger = setup_logger(__name__)

# Custom JSON encoder for datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

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
        
        return await success_response(cards_list)
        
    except Exception as e:
        logger.error(f"Error getting cards for user {user_id}: {e}")
        return await error_response(f"Database error: {str(e)}", status_code=500)

async def create_user_card(db: AsyncSession, user_id: int, card: dict, current_user: dict):
    """Create a new card for a user"""
    try:
        # Permission check - allow if it's your card or you have admin role
        auth_user_id = current_user.get("user_id")
        roles = current_user.get("roles", [])
        
        if auth_user_id != user_id and "admin" not in roles:
            has_permission = await check_permission("create_cards_for_any_user", db, auth_user_id)
            if not has_permission:
                return await error_response("Not authorized to create cards for this user", status_code=403)
                
        # Validate input
        if "tags" not in card or not isinstance(card["tags"], list) or not card["tags"]:
            return await error_response("Tags must be provided as a non-empty list", status_code=400)
            
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
        return await success_response({"id": card_id, "status": "created"})
    
    except Exception as e:
        await db.rollback()
        logger.error(f"Error creating card for user {user_id}: {e}")
        return await error_response(f"Database error: {str(e)}", status_code=500)

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
                return await error_response(f"Card with ID {card_id} not found", status_code=404)
        
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
        
        return await success_response({
            "id": card_id, 
            "status": "updated", 
            "updated_fields": updated_fields
        })
    
    except Exception as e:
        await db.rollback()
        logger.error(f"Error updating card {card_id}: {e}")
        return await error_response(f"Database error: {str(e)}", status_code=500)

async def delete_card(db: AsyncSession, card_id: int, current_user: dict):
    """Delete a card (or mark as inactive)"""
    try:
        # First check who owns this card
        owner_result = await db.execute(GET_CARD_OWNER, {"card_id": card_id})
        owner_row = owner_result.first()
        
        if not owner_row:
            return await error_response(f"Card with ID {card_id} not found", status_code=404)
            
        card_owner_id = owner_row[0]
        auth_user_id = current_user.get("user_id")
        roles = current_user.get("roles", [])
        
        # Permission check - allow if it's your card or you have admin role
        if auth_user_id != card_owner_id and "admin" not in roles:
            has_permission = await check_permission("delete_any_user_cards", db, auth_user_id)
            if not has_permission:
                return await error_response("Not authorized to delete this card", status_code=403)
        
        # Soft delete by setting is_active to false
        result = await db.execute(SOFT_DELETE_CARD, {"card_id": card_id})
        deleted_id = result.scalar_one_or_none()
        
        if not deleted_id:
            return await error_response(f"Card with ID {card_id} not found", status_code=404)
        
        await db.commit()
        logger.success(f"Marked card {card_id} as inactive")
        return await success_response({"id": card_id, "status": "deleted"})
    
    except Exception as e:
        await db.rollback()
        logger.error(f"Error deleting card {card_id}: {e}")
        return await error_response(f"Database error: {str(e)}", status_code=500)

async def handle_card_websocket(websocket: WebSocket, card_id: int, db: AsyncSession):
    """WebSocket handler for real-time updates of card data"""
    client_id = None
    try:
        # 1. Authenticate WebSocket connection
        user_data = await authenticate_ws(websocket)
        if not user_data:
            # Connection already closed by authenticate_ws if authentication failed
            return
            
        auth_user_id = user_data.get("user_id")
        
        # 2. Get card data
        result = await db.execute(GET_CARD_WITH_TAGS, {"card_id": card_id})
        card_rows = result.mappings().all()
        
        if not card_rows:
            logger.warning(f"Card {card_id} not found or inactive")
            await websocket.close(code=1008)
            return
        
        # 3. Check authorization to access this card
        card_owner_id = card_rows[0]['owner_id']
        can_access = await can_access_card(db, card_id, user_data)
        
        if not can_access:
            logger.warning(f"User {auth_user_id} not authorized to view card {card_id}")
            await websocket.close(code=1008)  # Policy violation
            return
            
        # 4. Extract all tag IDs and time range
        tag_ids = [row['tag_id'] for row in card_rows]
        start_time = card_rows[0]['start_time']
        end_time = card_rows[0]['end_time']
        
        logger.info(f"Card {card_id} tags: {tag_ids}, time range: {start_time} to {end_time}")
        
        # 5. Accept WebSocket connection - pass user data to the manager
        # client_id = await websocket_manager.connect(websocket, user_data)
        
        # 6. Fetch historical data and send it
        try:
            # Pass user_id to historical data function for audit logging
            initial_data = await get_historical_tag_data(
                tag_ids, 
                start_time, 
                end_time,
                user_id=auth_user_id
            )
            
            # Format response according to WebSocketCardSchema
            card_tags = []
            
            # Get tag names and create TagSchema objects for each tag
            tag_id_to_name = {row['tag_id']: row['tag_name'] for row in card_rows}
            
            # Process each tag with its latest data
            for tag_id in tag_ids:
                tag_id_str = str(tag_id)
                tag_name = tag_id_to_name.get(tag_id, f"Tag {tag_id}")
                tag_value = ""
                tag_timestamp = ""
                tag_description = ""
                tag_unit = ""
                
                # Find the latest data point for this tag
                for data_point in initial_data:
                    if isinstance(data_point, dict) and str(data_point.get("tag_id", "")) == tag_id_str:
                        tag_value = str(data_point.get("value", ""))
                        tag_timestamp = data_point.get("timestamp", "")
                        break
                
                # Create tag according to TagSchema
                tag_schema = {
                    "id": tag_id_str,
                    "name": tag_name,
                    "description": tag_description,
                    "timestamp": tag_timestamp,
                    "value": tag_value,
                    "unit_of_measure": tag_unit
                }
                card_tags.append(tag_schema)
            
            # Create card response according to WebSocketCardSchema
            card_response = {
                "type": "initial_data", 
                "card_id": str(card_id), 
                "tags": card_tags,
                "graph_type": "1"  # Default to "1" as string per schema
            }
            
            # Include original payload for backwards compatibility
            card_response["payload"] = initial_data
            
            # Convert to JSON and send
            initial_data_msg = json.dumps(card_response, cls=DateTimeEncoder)
            await websocket.send_text(initial_data_msg)
            logger.success(f"Sent initial data for card {card_id} to client {client_id}")
        except Exception as e:
            logger.error(f"Error fetching/sending historical data for card {card_id}: {e}")
            error_msg = json.dumps({"type": "error", "message": "Failed to fetch initial data"})
            await websocket.send_text(error_msg)
        
        # 7. Subscribe to tags for real-time updates
        # websocket_manager.subscribe(client_id, set(str(tag_id) for tag_id in tag_ids))
        logger.info(f"Client {client_id} subscribed to tags {tag_ids} for card {card_id}")
        
        # 8. Keep connection open to receive real-time updates
        while True:
            # Just wait to detect disconnects
            await websocket.receive_text()
            logger.debug(f"Received message from client {client_id} for card {card_id}")
            
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected by client: {client_id} for card {card_id}")
    except Exception as e:
        logger.error(f"Error in card WebSocket for card {card_id}: {e}")
    finally:
        if client_id:
            logger.info(f"Cleaning up connection for client {client_id}")
            # await websocket_manager.disconnect(client_id)

async def patch_user_card(db: AsyncSession, card_id: int, card_patch: dict, current_user: dict):
    """
    Patch a card with only the fields provided - simpler approach than full update
    """
    try:
        # First check who owns this card (for permission check)
        owner_result = await db.execute(GET_CARD_OWNER, {"card_id": card_id})
        owner_row = owner_result.first()
        
        if not owner_row:
            return await error_response(f"Card with ID {card_id} not found", status_code=404)
            
        card_owner_id = owner_row[0]
        auth_user_id = current_user.get("user_id")
        roles = current_user.get("roles", [])
        
        # Permission check - allow if it's your card or you have admin role
        if auth_user_id != card_owner_id and "admin" not in roles:
            has_permission = await check_permission("update_any_user_cards", db, auth_user_id)
            if not has_permission:
                return await error_response("Not authorized to update this card", status_code=403)
                
        # Special handling for tags - process separately
        has_tags = "tags" in card_patch
        tags = None
        if has_tags:
            tags = card_patch.pop("tags")  # Remove from patch to handle separately
        
        # Special handling for time fields
        if "startTime" in card_patch:
            card_patch["start_time"] = parse_relative_time(card_patch.pop("startTime"))
        elif "start_time" in card_patch:
            card_patch["start_time"] = parse_relative_time(card_patch["start_time"])
            
        if "endTime" in card_patch:
            card_patch["end_time"] = parse_relative_time(card_patch.pop("endTime"))
        elif "end_time" in card_patch:
            card_patch["end_time"] = parse_relative_time(card_patch["end_time"])
        
        # Build field update SET clause dynamically for the remaining fields
        if card_patch:
            set_clauses = []
            params = {"card_id": card_id}
            
            for field, value in card_patch.items():
                set_clauses.append(f"{field} = :{field}")
                params[field] = value
            
            # Always update the updated_at timestamp
            set_clauses.append("updated_at = CURRENT_TIMESTAMP")
            
            # Execute update if we have fields to update
            if set_clauses:
                query = f"""
                    UPDATE card_data 
                    SET {', '.join(set_clauses)}
                    WHERE id = :card_id
                    RETURNING id
                """
                result = await db.execute(text(query), params)
                updated_id = result.scalar_one_or_none()
                
                if not updated_id:
                    return await error_response(f"Card with ID {card_id} not found", status_code=404)
        
        # Handle tags update if provided
        if has_tags and isinstance(tags, list):
            # Delete existing tag associations
            await db.execute(DELETE_CARD_TAGS, {"card_id": card_id})
            
            # Add new tag associations
            for tag_id in tags:
                await db.execute(ADD_TAG_TO_CARD, {"card_id": card_id, "tag_id": tag_id})
        
        await db.commit()
        
        # Prepare result message
        updated_fields = list(card_patch.keys())
        if has_tags:
            updated_fields.append("tags")
            
        logger.success(f"Patched card {card_id}: {updated_fields}")
        
        # Return standardized success response
        return await success_response({
            "id": card_id, 
            "status": "updated", 
            "updated_fields": updated_fields
        })
        
    except Exception as e:
        await db.rollback()
        logger.error(f"Error patching card {card_id}: {e}")
        return await error_response(f"Database error: {str(e)}", status_code=500)
