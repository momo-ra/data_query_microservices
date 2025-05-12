from queries.graph_queries import create_graph as create_graph_query
from utils.log import setup_logger

logger = setup_logger(__name__)

async def create_graph(graph_name, description, session, current_user):
    """
    Create a new graph type using the query from the queries directory
    """
    return await create_graph_query(graph_name, description, session, current_user)
    
