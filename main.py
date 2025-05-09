from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers.endpoints import router
import asyncio
# from services.kafka_central_listener_services import kafka_central_data_listener
from services.kafka_services import kafka_services
from utils.log import setup_logger
from database import init_db
from middleware.response_middleware import StandardResponseMiddleware

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
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)