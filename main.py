from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from routers.endpoints import router
import asyncio
# from services.kafka_central_listener_services import kafka_central_data_listener
from services.kafka_services import kafka_services
from utils.log import setup_logger
from database import init_db
from middleware.response_middleware import StandardResponseMiddleware
from utils.response import fail_response

logger = setup_logger(__name__)
app = FastAPI(title="ChatAPC Data Query Microservice")

# Add CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this as needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add StandardResponseMiddleware to standardize all JSON responses
app.add_middleware(StandardResponseMiddleware)

# Add startup event to initialize Kafka consumer
@app.on_event("startup")
async def startup_event():
    await init_db()
    logger.info("Application startup: Initializing Kafka listener")
    # Use your actual Kafka topic name and broker address
    kafka_topic = "test"  # Updated to the likely topic name based on data format
    kafka_brokers = "localhost:9092"  # Change if your Kafka broker is elsewhere

    logger.success("Kafka listener started in background")

@app.on_event("shutdown")
async def shutdown_event():
    await kafka_services.stop()

# Exception handler for HTTPException
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content=fail_response(exc.detail)
    )

# Exception handler for RequestValidationError
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    return JSONResponse(
        status_code=422,
        content=fail_response(
            "Validation error",
            data={"errors": [{"loc": "/".join(map(str, err["loc"])), "msg": err["msg"]} for err in exc.errors()]}
        )
    )

app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)