from typing import Any, Dict, Optional
from schemas.schema import ResponseModel

def success_response(data: Any = None, message: Optional[str] = None) -> Dict[str, Any]:
    return ResponseModel(status="success", data=data, message=message).dict()

def fail_response(message: str, data: Any = None) -> Dict[str, Any]:
    return ResponseModel(status="fail", data=data, message=message).dict()

# Keeping error_response for backward compatibility
def error_response(message: str, data: Any = None) -> Dict[str, Any]:
    """Alias for fail_response for backward compatibility"""
    return fail_response(message, data)