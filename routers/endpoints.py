from fastapi import APIRouter, Depends, Query, WebSocket, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from database import get_db
from queries import get_table_data, get_tag_data_with_tag_id, get_all_tag_data, get_trends_data, get_polling_tags
from middleware.auth_middleware import authenticate_user
from services.card_services import get_user_cards, create_user_card, update_user_card, delete_card, handle_card_websocket, patch_user_card
from services.card_services import handle_card_websocket
from services.dashboard_services import handle_dashboard
from utils.log import setup_logger
from schemas.schema import TagListResponse, CardSchema, GraphSchema, TagSchema, ResponseModel
from services.graph_services import create_graph, get_graphs
from typing import List
from utils.response import success_response, error_response, fail_response
from pydantic import BaseModel

router = APIRouter(prefix="/api/v1")
logger = setup_logger(__name__)

# Example User model for demonstration
class User(BaseModel):
    id: int
    name: str
    email: str

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
        return success_response({"table": table_name, "records": data})
    except Exception as e:
        logger.error(f"Error fetching table data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")

@router.get("/tags/{tag_id}/data", response_model=ResponseModel[TagSchema])
async def fetch_tag_data(tag_id: int):
    """Fetch tag data with tag ID with caching."""
    try:
        records = await get_tag_data_with_tag_id(tag_id)
        
        # Format the response to match TagSchema
        if records and not isinstance(records, dict) and len(records) > 0:
            record = records[0]  # Get the first record
            tag_data = TagSchema(
                id=str(record.get("id", tag_id)),
                name=record.get("name", ""),
                description=record.get("description", ""),
                timestamp=record.get("timestamp", ""),
                value=str(record.get("value", "")),
                unit_of_measure=record.get("unit_of_measure", "")
            )
            return ResponseModel(status="success", data=tag_data, message=None)
        elif isinstance(records, dict) and "error" in records:
            # Handle error case
            tag_data = TagSchema(
                id=str(tag_id),
                name=f"Tag {tag_id}",
                description="Error retrieving tag",
                timestamp="",
                value="",
                unit_of_measure=""
            )
            return ResponseModel(status="fail", data=tag_data, message=records.get("error", "Error retrieving tag"))
        else:
            # Handle case where no records found
            tag_data = TagSchema(
                id=str(tag_id),
                name=f"Tag {tag_id}",
                description="No data found",
                timestamp="",
                value="",
                unit_of_measure=""
            )
            return ResponseModel(status="success", data=tag_data, message="No data found")
    except Exception as e:
        logger.error(f"Error fetching tag data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching tag data: {str(e)}")

@router.get("/tags", response_model=ResponseModel[List[TagSchema]])
async def fetch_all_tag_data():
    """Fetch all tag data."""
    try:
        response = await get_all_tag_data()
            # Return properly constructed Pydantic model
        if response:
            return success_response(response)
        elif isinstance(response, dict) and "error" in response:
            # Handle error case using Pydantic model
            return error_response(status="fail",
                data=None,
                message=response["error"])
        else:
            # Handle unexpected response format using Pydantic model
            return error_response(
                message="Unexpected response format from database query"
            )
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
        return success_response({"data": data})
    except Exception as e:
        logger.error(f"Error fetching trends data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching trends data: {str(e)}")

@router.get('/polling/tags', response_model=ResponseModel[List[TagSchema]])
async def fetch_polling_tags(db: AsyncSession = Depends(get_db), current_user = Depends(authenticate_user)):
    """Fetch all active polling tags with validation"""
    try:
        result = await get_polling_tags(db, current_user)
        if result:
            # The data from get_polling_tags is already in the right format, just use it directly
            return ResponseModel(status="success", data=result, message=None)
        else:
            # Handle error case
            return ResponseModel(status="fail", data=None, message="No Data Found")
    except Exception as e:
        logger.error(f"Error fetching polling tags: {str(e)}")
        return ResponseModel(status="fail", data=None, message=str(e))

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

@router.websocket('/ws/card/{card_id}')
async def websocket_card(websocket: WebSocket, card_id: int, db: AsyncSession = Depends(get_db)):
    """WebSocket endpoint for real-time updates of card data."""
    try:
        await handle_card_websocket(websocket, card_id, db)
    except Exception as e:
        logger.error(f"WebSocket Card Error: {str(e)}")
        # WebSocket errors are handled in the handler function

@router.get("/user/{user_id}/cards")
async def get_cards(
    user_id: int, 
    db: AsyncSession = Depends(get_db),
    current_user = Depends(authenticate_user)
):
    """Retrieve all cards for a specific user with their associated tags."""
    try:
        result = await get_user_cards(db, user_id, current_user)
        return result
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
        result = await create_user_card(db, user_id, card, current_user)
        return result
    except Exception as e:
        logger.error(f"Error creating user card: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error creating user card: {str(e)}")

@router.put("/user/cards/{card_id}", response_model= CardSchema)
async def update_card(
    card_id: int,
    card: dict,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(authenticate_user)
):
    """Update an existing card for a user."""
    try:
        result = await update_user_card(db, card_id, card, current_user)
        return result
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
        result = await delete_card(db, card_id, current_user)
        return result
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
        result = await patch_user_card(db, card_id, card_patch, current_user)
        return result
    except Exception as e:
        logger.error(f"Error patching card: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error patching card: {str(e)}")

{#
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
}

#Graph Endpoints
@router.post('/graph', response_model=GraphSchema)
async def create_graph_(graph_name: str, description:str, db: AsyncSession = Depends(get_db), current_user= Depends(authenticate_user)):
    try:
        async with db as session:
            result = await create_graph(graph_name, description, session, current_user)
            
            # Check if we got a success response
            if result.get("status") == "success":
                # Return just the data part which should match GraphSchema
                return result["data"]
            else:
                # If it's an error response, raise an HTTPException
                raise HTTPException(status_code=400, detail=result.get("message", "Error creating graph"))
    except Exception as e:
        logger.error(f"Something Went Wrong in Create Graph: {str(e)}")
        return error_response(str(e))

#Get All Graphs
@router.get('/graphs', response_model=ResponseModel[List[GraphSchema]])
async def get_all_graphs(current_user= Depends(authenticate_user),db:AsyncSession= Depends(get_db)):
    try:
        async with db as session:
            result = await get_graphs(current_user, session)
            print(result)
            if result.get("status") == "success":
                # Return just the data part which should match GraphSchema
                return result
            else:
                # If it's an error response, raise an HTTPException
                raise HTTPException(status_code=400, detail=result.get("message", "Error creating graph"))
    except Exception as e:
        logger.error(f"Something Went Wrong in Getting Graph: {str(e)}")
        return error_response(str(e))


# Example endpoint using the new ResponseModel
@router.get("/users/{user_id}", response_model=ResponseModel[User])
async def get_user(user_id: int):
    """Example endpoint demonstrating ResponseModel usage"""
    if user_id == 1:
        user = User(id=1, name="Mohamed", email="mohamed@example.com")
        return ResponseModel(status="success", data=user, message=None)
    else:
        return ResponseModel(
            status="fail",
            data=None,
            message=f"User with ID {user_id} not found"
        )

# Example endpoint returning a list of users
@router.get("/users", response_model=ResponseModel[List[User]])
async def get_users():
    """Example endpoint demonstrating ResponseModel with a list"""
    users = [
        User(id=1, name="Mohamed", email="mohamed@example.com"),
        User(id=2, name="Ahmed", email="ahmed@example.com")
    ]
    return ResponseModel(status="success", data=users, message=None)

# Example demonstrating the helper functions
@router.get("/items/{item_id}")
async def get_item(item_id: int):
    """Example endpoint demonstrating the success_response and fail_response helper functions"""
    if item_id > 0:
        return success_response({"item_id": item_id, "name": f"Item {item_id}"})
    else:
        return fail_response("Invalid item ID")