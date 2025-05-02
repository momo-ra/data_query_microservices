from fastapi.responses import JSONResponse
from fastapi import status

async def success_response(data=None, meta=None, status_code=status.HTTP_200_OK):
    return JSONResponse(
        status_code=status_code,
        content={
         "status": "success",
         "data": data,
         "meta": meta
        }
    )

async def error_response(message, status_code=status.HTTP_400_BAD_REQUEST):
    return JSONResponse(
        status_code=status_code,
        content={
            "status": "error",
            "message": message
        }
    )
