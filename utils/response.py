from typing import Any, Dict, List, Type, TypeVar, Optional
from pydantic import BaseModel
from schemas.schema import ResponseModel

T = TypeVar('T', bound=BaseModel)

def success_response(data: Any = None, message: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a standardized success response
    
    Args:
        data: The data to include in the response
        message: Optional message for the response
        
    Returns:
        A dictionary with the standardized response format
    """
    return ResponseModel(status="success", data=data, message=message).dict()

def fail_response(message: str, data: Any = None) -> Dict[str, Any]:
    """
    Create a standardized failure response
    
    Args:
        message: Error message describing the failure (required)
        data: Optional data to include with the error response
        
    Returns:
        A dictionary with the standardized response format
    """
    return ResponseModel(status="fail", data=data, message=message).dict()

# Keeping error_response for backward compatibility
def error_response(message: str, data: Any = None) -> Dict[str, Any]:
    """Alias for fail_response for backward compatibility"""
    return fail_response(message, data)

def handle_exception(exception: Exception) -> Dict[str, Any]:
    """
    Create a standardized failure response from an exception
    
    Args:
        exception: The exception to convert to a response
        
    Returns:
        A dictionary with the standardized failure response format
    """
    return fail_response(str(exception))

def create_model_response(model_class: Type[T], data: Any) -> T:
    """
    Create a Pydantic model instance from response data.
    This is useful when we need to return a response that matches a specific Pydantic model.
    
    Args:
        model_class: The Pydantic model class to instantiate
        data: The data to populate the model with
        
    Returns:
        An instance of the specified model class
    """
    # For standard responses with status, data, message format
    if hasattr(model_class, "status") and hasattr(model_class, "data"):
        return model_class(
            status="success",
            data=data,
            message=None
        )
    # For regular models without the standard response format
    else:
        return model_class(**data) 