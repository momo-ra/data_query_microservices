from sqlalchemy import text
from utils.response import error_response, success_response
from middleware.permission_middleware import check_permission
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from utils.log import setup_logger

logger = setup_logger(__name__)

# Create a new graph type
CREATE_GRAPH = text("""
    INSERT INTO graph_type (name, description, created_at, updated_at)
    VALUES (:graph_name, :description, :created_at, :updated_at)
    RETURNING id
""")

async def create_graph(graph_name, description, session, current_user):
    try:
        if not current_user:
            return error_response('User Not Authorized')
        has_permission = await check_permission('create_graph', session)
        if not has_permission:
            return error_response("User Not Authenticated To Create a New Graph")
        created_at = datetime.now()
        updated_at = datetime.now()
        result = await session.execute(CREATE_GRAPH, {"graph_name":graph_name, "description":description, "created_at":created_at, "updated_at":updated_at})
        await session.commit()
        
        # Get the inserted ID
        inserted_id = result.scalar_one()
        
        # Return response in the format required by GraphSchema
        return success_response({
            "id": inserted_id,
            "name": graph_name,
            "description": description
        })
    except Exception as e:
        logger.error(f"Error creating graph: {e}")
        return error_response(f"Error creating graph: {str(e)}")