from fastapi import APIRouter, Depends, Query, WebSocket, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from database import get_db
from services.query_services import get_table_data, get_tag_data_with_tag_id, get_all_tag_data, get_trends_data
from middleware.auth_middleware import authenticate_user
from services.card_services import get_user_cards, create_user_card, update_user_card, delete_card, handle_card_websocket, patch_user_card
# from services.dashboard_services import handle_realtime_tags_websocket, handle_dashboard_websocket
from utils.dashboard_formatter import format_dashboard_cards
from utils.response_model import success_response, error_response
import json
from services.dashboard_services import handle_dashboard
from utils.log import setup_logger

router = APIRouter(prefix="/api/v1")
logger = setup_logger(__name__)

#
# Data Query Endpoints
#
@router.get("/tables/{table_name}/data")
async def fetch_table_data(
    table_name: str,
    start_time: str = Query(None, description="Start timestamp"),
    end_time: str = Query(None, description="End timestamp"),
    limit: int = Query(100, description="Number of records", le=1000),
):
    """Fetch table data with optional time filtering and caching."""
    try:
        data = get_table_data(table_name, start_time, end_time, limit)
        return {"table": table_name, "records": data}
    except Exception as e:
        logger.error(f"Error fetching table data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")

@router.get("/tags/{tag_id}/data")
async def fetch_tag_data(tag_id: str):
    """Fetch tag data with tag ID with caching."""
    try:
        records = get_tag_data_with_tag_id(tag_id)
        return {"tag": tag_id, "records": records}
    except Exception as e:
        logger.error(f"Error fetching tag data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching tag data: {str(e)}")

@router.get("/tags")
async def fetch_all_tag_data():
    """Fetch all tag data."""
    try:
        records = get_all_tag_data()
        return {"records": records}
    except Exception as e:
        logger.error(f"Error fetching all tags: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching all tags: {str(e)}")

@router.get('/trends')
async def fetch_trends_data(
    db: AsyncSession = Depends(get_db),
    tag_id_1: int = Query(..., description="First tag ID"),
    tag_id_2: int = Query(..., description="Second tag ID"),
    start_time: str = Query(..., description="Start timestamp (YYYY-MM-DD HH:MM:SS)"),
    end_time: str = Query(..., description="End timestamp (YYYY-MM-DD HH:MM:SS)")
):
    """Get trends data for given tag IDs and time range."""
    try:
        tag_ids = {"tag_id_1": tag_id_1, "tag_id_2": tag_id_2}
        data = await get_trends_data(db, tag_ids, start_time, end_time)
        return {"data": data}
    except Exception as e:
        logger.error(f"Error fetching trends data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching trends data: {str(e)}")

#comments
{
# WebSocket Endpoints
#
# @router.websocket("/ws/realtime-tags")
# async def websocket_realtime_tags(websocket: WebSocket):
#     """WebSocket endpoint for real-time tag updates."""
#     await handle_realtime_tags_websocket(websocket)

# @router.websocket("/ws/card/{card_id}")
# async def websocket_card_data(websocket: WebSocket, card_id: int, db: AsyncSession = Depends(get_db)):
#     """WebSocket endpoint for real-time updates of card data."""
#     await handle_card_websocket(websocket, card_id, db)

# @router.websocket("/ws/dashboard")
# async def websocket_dashboard(websocket: WebSocket, db: AsyncSession = Depends(get_db)):
#     """WebSocket endpoint for dashboard with real-time updates for all user's cards."""
#     await handle_dashboard_websocket(websocket, db)

#
# Card Management Endpoints
}
#end comments

@router.websocket('/ws/dashboard')
async def websocket_dashboard(websocket:WebSocket, db: AsyncSession = Depends(get_db)):
    """WebSocket endpoint for dashboard with real-time updates for user's cards."""
    try:
        await handle_dashboard(websocket, db)
    except Exception as e:
        logger.error(f"WebSocket dashboard error: {str(e)}")
        # WebSocket errors are handled in the handler function

@router.get("/user/{user_id}/cards")
async def get_cards(
    user_id: int, 
    db: AsyncSession = Depends(get_db),
    current_user = Depends(authenticate_user)
):
    """Retrieve all cards for a specific user with their associated tags."""
    try:
        return await get_user_cards(db, user_id, current_user)
    except Exception as e:
        logger.error(f"Error getting user cards: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting user cards: {str(e)}")

@router.post("/user/{user_id}/cards")
async def create_card(
    user_id: int, 
    card: dict,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(authenticate_user)
):
    """Create a new card for a user."""
    try:
        return await create_user_card(db, user_id, card, current_user)
    except Exception as e:
        logger.error(f"Error creating user card: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating user card: {str(e)}")

@router.put("/user/cards/{card_id}")
async def update_card(
    card_id: int,
    card: dict,
    db: AsyncSession = Depends(get_db)
):
    """Update an existing card for a user."""
    try:
        return await update_user_card(db, card_id, card)
    except Exception as e:
        logger.error(f"Error updating card: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error updating card: {str(e)}")

@router.delete("/cards/{card_id}")
async def remove_card(
    card_id: int, 
    db: AsyncSession = Depends(get_db),
    current_user = Depends(authenticate_user)
):
    """Delete a card (or mark as inactive)."""
    try:
        return await delete_card(db, card_id, current_user)
    except Exception as e:
        logger.error(f"Error deleting card: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error deleting card: {str(e)}")

@router.patch("/user/cards/{card_id}")
async def patch_card(
    card_id: int,
    card_patch: dict,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(authenticate_user)
):
    """Patch an existing card - simpler alternative to PUT that only updates specified fields"""
    try:
        return await patch_user_card(db, card_id, card_patch, current_user)
    except Exception as e:
        logger.error(f"Error patching card: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error patching card: {str(e)}")

#
# Dashboard Endpoints
# Dashboard Endpoints
# Dashboard Endpoints
#
# @router.get("/user/{user_id}/dashboard")
# async def get_user_dashboard(
#     user_id: int, 
#     db: AsyncSession = Depends(get_db),
#     current_user = Depends(authenticate_user)
# ):
#     """Get formatted dashboard data for a user"""
#     try:
#         # Verify permissions
#         auth_user_id = current_user.get("user_id")
#         if auth_user_id != user_id and "admin" not in current_user.get("roles", []):
#             return await error_response("Not authorized to view this dashboard", status_code=403)
        
#         # Get cards using the existing function
#         cards_response = await get_user_cards(db, user_id, current_user)
        
#         # If there was an error getting cards, return it
#         if cards_response.status_code != 200:
#             return cards_response
        
#         # Get the raw cards data from the response
#         response_content = cards_response.body.decode('utf-8')
#         response_data = json.loads(response_content)
#         cards_data = response_data.get("data", [])
        
#         # Format for dashboard consumption
#         dashboard_data = format_dashboard_cards(cards_data)
        
#         # Add any additional dashboard data
#         dashboard_data["user_settings"] = {
#             "preferred_theme": "light",
#             "auto_refresh": True
#         }
        
#         # Return with standard response format
#         return await success_response(dashboard_data)
        
#     except Exception as e:
#         logger.error(f"Error loading dashboard: {str(e)}")
#         return await error_response(f"Error loading dashboard: {str(e)}", status_code=500)
