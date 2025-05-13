from services.websocket_service import websocket_manager
from middleware.auth_middleware import authenticate_ws
from utils.log import setup_logger
from services.kafka_services import kafka_services
from queries.dashboard_queries import get_user_active_cards
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException, WebSocketDisconnect
from fastapi.websockets import WebSocketState
from schemas.schema import WebSocketCardSchema, TagSchema
import json 
import asyncio
import time


logger = setup_logger(__name__)

async def send_to_subscribe_user(kafka_message, user_id, card_tag_mapping, websocket):
    try:
        tag_id = kafka_message.get("tag_id")
        if not tag_id:
            logger.warning(f"Kafka message missing tag_id: {kafka_message}")
            return False
            
        # Find cards that contain this tag
        cards_with_tag = card_tag_mapping.get(tag_id, [])
        if not cards_with_tag:
            logger.debug(f"No active cards for user {user_id} with tag {tag_id}")
            return True
            
        # Create formatted message for each card containing this tag
        for card in cards_with_tag:
            # Format data according to WebSocketCardSchema
            formatted_message = {
                "card_id": str(card["card_id"]),
                "tag": {
                    "id": str(tag_id),
                    "name": card["tag_name"],
                    "description": kafka_message.get("description", ""),
                    "timestamp": kafka_message.get("timestamp", ""),
                    "value": kafka_message.get("value", ""),
                    "unit_of_measure": kafka_message.get("unit", "")
                },
                "graph_type": card.get("graph_type", "line")  # Default to line graph if not specified
            }
            
            # Send the formatted message to the user
            success = await websocket_manager.send_message(formatted_message, user_id, websocket)
            if not success:
                return False
                
        return True
    except KeyError as e:
        logger.error(f"Invalid kafka message format: {e}")
    except Exception as e:
        logger.error(f"Error in send_to_subscribe_user: {e}")
    return False

async def handle_dashboard(websocket, db:AsyncSession):
    user_id = None
    kafka_subscriber_id = None
    
    try:
        #Authenticate User if the token send in params or not
        try:
            user_data = await authenticate_ws(websocket)
            if user_data is None:
                logger.warning("Authentication failed: user_data is None")
                return
                
            user_id = user_data.get("user_id")
            logger.info(f"Dashboard websocket connection initiated for user {user_id}")
        except HTTPException as auth_error:
            logger.warning(f"Authentication failed: {auth_error.detail}")
            return
        
        #Connect user in websocket and return the UserId of the user
        await websocket_manager.connect(websocket, user_id)

        # Send initial connection success message
        await websocket.send_json({
            "type": "connection_status", 
            "status": "connected",
            "user_id": user_id
        })

        # Get active cards for this user using the query function
        active_cards = await get_user_active_cards(db, user_id)
        
        # Extract card data and organize by tag_id for efficient lookup
        # Store user's tag IDs
        user_tag_ids = set()
        
        # Create a mapping of tag_id -> list of cards containing that tag
        card_tag_mapping = {}
        
        for card in active_cards:
            for tag in card["tags"]:
                tag_id = tag["id"]
                user_tag_ids.add(tag_id)
                
                # Create or append to the list of cards for this tag
                if tag_id not in card_tag_mapping:
                    card_tag_mapping[tag_id] = []
                    
                # Add card info to the mapping
                card_tag_mapping[tag_id].append({
                    "card_id": card["id"],
                    "tag_name": tag["name"],
                    "graph_type": "line"  # Default graph type - can be enhanced to get from DB
                })
        
        # Subscribe to tags using the new system
        if user_tag_ids:
            kafka_subscriber_id = await kafka_services.subscribe_to_tags(list(user_tag_ids))
            logger.info(f"User {user_id} subscribed to {len(user_tag_ids)} tags with ID {kafka_subscriber_id}")
            
            # Send subscription confirmation
            await websocket.send_json({
                "type": "subscription_status",
                "status": "subscribed",
                "subscribed_tags": list(user_tag_ids)
            })
        else:
            logger.info(f"User {user_id} has no active cards with tags")
            await websocket.send_json({
                "type": "info",
                "message": "No active cards found"
            })
        
        # Process messages and connection
        close_connection = False
        while not close_connection and kafka_subscriber_id:
            # Check connection state
            if websocket.client_state == WebSocketState.DISCONNECTED:
                logger.info(f"Client disconnected for user {user_id}")
                break
                
            # Get available messages
            messages = await kafka_services.get_messages(kafka_subscriber_id)
            
            # Process received messages
            for message in messages:
                tag_id = message.get("tag_id")
                if not tag_id:
                    continue
                    
                # Find cards associated with this tag
                cards_with_tag = card_tag_mapping.get(tag_id, [])
                if not cards_with_tag:
                    continue
                
                # Create formatted message for each card
                for card in cards_with_tag:
                    card_id = str(card["card_id"])
                    
                    # Check connection state before sending
                    if websocket.client_state == WebSocketState.DISCONNECTED:
                        close_connection = True
                        break
                        
                    # Format the tag data
                    tag_data = {
                        "id": str(tag_id),
                        "name": card["tag_name"],
                        "description": message.get("description", ""),
                        "timestamp": message.get("timestamp", ""),
                        "value": message.get("value", ""),
                        "unit_of_measure": message.get("unit", "")
                    }
                    
                    # Send individual message with a single tag object (not in an array)
                    try:
                        await websocket.send_json({
                            "type": "data_batch",
                            "card_id": card_id,
                            "tag": tag_data,  # Single tag object, not an array
                            "graph_type": card.get("graph_type", "line")
                        })
                        logger.debug(f"Sent message for card {card_id} with tag {tag_id}")
                    except WebSocketDisconnect:
                        logger.info(f"WebSocket disconnected for user {user_id}")
                        close_connection = True
                        break
                    except Exception as e:
                        logger.error(f"Error sending update for card {card_id}: {e}")
                        if "close message has been sent" in str(e):
                            close_connection = True
                            break
            
            # Short wait if no messages
            if not messages:
                await asyncio.sleep(0.1)
                
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected during setup for user {user_id}")
    except Exception as e:
        logger.error(f"Error in handle_dashboard websocket: {str(e)}")
        # Try to send error to client if connection is still open
        try:
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json({
                    "type": "error",
                    "message": "Server error: " + str(e)
                })
        except:
            pass
    finally:
        # Unsubscribe from Kafka
        if kafka_subscriber_id:
            await kafka_services.unsubscribe(kafka_subscriber_id)
            
        # Always ensure we disconnect the user when the connection ends
        if user_id and websocket_manager.is_connected(user_id):
            await websocket_manager.disconnect(user_id)
            logger.info(f"Cleaned up websocket for user {user_id}")