from utils.response import error_response, success_response
from middleware.permission_middleware import check_permission
from queries.graph_queries import CREATE_GRAPH
from datetime import datetime

async def create_graph(graph_name, description, session):
    try:
        # if not current_user:
        #     return error_response('User Not Authorized')
        # has_permission = await check_permission('create_graph', db)
        # if not has_permission:
        #     return error_response("User Not Authenticated To Create a New Graph")
        created_at = datetime.now()
        updated_at = datetime.now()
        result = await session.execute(CREATE_GRAPH, {"graph_name":graph_name, "description":description, "created_at":created_at, "updated_at":updated_at})
        await session.commit()
        
        # Obtener el ID insertado
        inserted_id = result.scalar_one()
        
        # Devolver la respuesta con el formato requerido por GraphSchema
        return success_response({
            "id": inserted_id,
            "name": graph_name,
            "description": description
        })
    except Exception as e:
        return error_response(f"Error creating graph: {str(e)}")
    
