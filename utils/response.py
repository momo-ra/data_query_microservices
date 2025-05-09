from typing import Any, Dict


def success_response(data: Any = None) -> Dict[str, Any]:
    return {
        "status": "success",
        "data": data
    }

def error_response(message: str, data: Any = None) -> Dict[str, Any]:
    return {
        "status": "fail",
        "data": data,
        "message": message
    }

def handle_exception(exception: Exception) -> Dict[str, Any]:
    return error_response(str(exception)) 