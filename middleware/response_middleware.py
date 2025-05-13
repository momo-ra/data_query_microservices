from fastapi import Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.middleware.base import BaseHTTPMiddleware
from typing import Callable
import json
from utils.response import fail_response


class StandardResponseMiddleware(BaseHTTPMiddleware):
    """
    Middleware to enforce standard response format across the application.
    """
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        try:
            # Process the request and get the response
            response = await call_next(request)
            
            # Skip standardization for non-JSON responses or streaming responses
            if (response.headers.get("content-type") != "application/json" or 
                isinstance(response, StreamingResponse) or
                not hasattr(response, "body")):
                return response
            
            # For JSON responses, check if they are already in standard format
            try:
                body = await response.body()
                response_data = json.loads(body)
                
                # If already standardized, return as is
                if isinstance(response_data, dict) and "status" in response_data:
                    return response
                
                # Wrap the response in success format
                standardized = {
                    "status": "success",
                    "data": response_data,
                    "message": None
                }
                
                return JSONResponse(
                    status_code=response.status_code,
                    content=standardized,
                    headers=dict(response.headers)
                )
                
            except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
                # If not JSON or can't decode, return as is
                return response
                
        except Exception as e:
            # Handle unhandled exceptions
            return JSONResponse(
                status_code=500,
                content=fail_response(f"Unhandled server error: {str(e)}")
            ) 