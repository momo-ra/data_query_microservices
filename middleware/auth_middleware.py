import jwt #type: ignore 
from fastapi import HTTPException, Security, Depends, Header, WebSocket
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import os
import dotenv
from utils.log import setup_logger
from typing import Optional, Dict, List, Any, Union

logger = setup_logger(__name__)

dotenv.load_dotenv('.env', override=True)

JWT_SECRET = os.getenv("JWT_SECRET")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM")

if not JWT_SECRET or not JWT_ALGORITHM:
    logger.error("JWT_SECRET or JWT_ALGORITHM environment variables not set")
    raise ValueError("JWT configuration is missing. Please set JWT_SECRET and JWT_ALGORITHM environment variables.")

security = HTTPBearer()

def verify_token(token: str) -> Dict[str, Any]:
    """
    Verify JWT token and return payload.
    Raises HTTPException if token is invalid.
    """
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        print(payload, "payloaddddd")
        
        # Validate payload structure
        if "user_id" not in payload:
            logger.warning(f"Token missing user_id in payload: {payload}")
            raise HTTPException(status_code=401, detail="Invalid token structure: missing user_id")
            
        return payload
    except jwt.ExpiredSignatureError:
        logger.warning("Token has expired")
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid token: {str(e)}")
        raise HTTPException(status_code=401, detail="Invalid token")
    except Exception as e:
        logger.error(f"Unexpected error verifying token: {str(e)}")
        raise HTTPException(status_code=401, detail="Authentication error")
    
async def authenticate_user(token: HTTPAuthorizationCredentials = Security(security)) -> Dict[str, Any]:
    """
    FastAPI dependency to authenticate a user from an HTTP request with a Bearer token.
    Returns the JWT payload containing user information.
    """
    payload = verify_token(token.credentials)
    logger.info(f"User authenticated: {payload.get('user_id')}")
    return payload

async def verify_ws_token(token: str) -> Dict[str, Any]:
    """
    Verify a JWT token from a WebSocket connection.
    Returns the JWT payload containing user information.
    Raises exceptions for invalid tokens - these should be caught by the caller.
    """
    return verify_token(token)

async def get_token_from_ws_query(websocket: WebSocket) -> Optional[str]:
    """
    Extract token from WebSocket query parameters.
    Returns None if no token is found.
    """
    return websocket.query_params.get("token")

async def authenticate_ws(websocket: WebSocket) -> Optional[Dict[str, Any]]:
    """
    Authenticate a WebSocket connection.
    Returns the JWT payload if authentication succeeds, None otherwise.
    Automatically closes the WebSocket connection if authentication fails.
    """
    token = await get_token_from_ws_query(websocket)
    bearer_token = str(f"Bearer {token}")
    
    if not token:
        logger.warning("WebSocket connection attempt without token")
        await websocket.close(code=1008)  # Policy violation
        return None
        
    try:
        payload = await verify_ws_token(bearer_token)
        logger.info(f"WebSocket authenticated for user: {payload.get('user_id')}")
        return payload
    except Exception as e:
        logger.warning(f"WebSocket authentication failed: {str(e)}")
        await websocket.close(code=1008)  # Policy violation
        return None

# Helper function to get user_id from authenticated user payload
def get_user_id(auth_data: Dict[str, Any]) -> int:
    """Extract user_id from authenticated user data"""
    return auth_data.get("user_id")

# Helper function to check if user has admin role
def is_admin(auth_data: Dict[str, Any]) -> bool:
    """Check if user has admin role"""
    roles = auth_data.get("roles", [])
    return "admin" in roles








