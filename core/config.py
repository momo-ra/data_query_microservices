import os
from dotenv import load_dotenv
from utils.log import setup_logger

logger = setup_logger(__name__)

# Load environment variables from .env file
load_dotenv('.env', override=True)

class Settings:
    """Configuration settings for database and caching."""
    DB_USER: str = os.getenv("DB_USER")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD")
    DB_HOST: str = os.getenv("DB_HOST")
    DB_PORT: str = os.getenv("DB_PORT", "5432")
    DB_NAME: str = os.getenv("DB_NAME")
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", 6379))
    JWT_SECRET: str = os.getenv("JWT_SECRET", "your_secret_key")
    JWT_ALGORITHM: str = os.getenv("JWT_ALGORITHM", "HS256")

    @property
    def DATABASE_URL(self):
        if not all([self.DB_USER, self.DB_PASSWORD, self.DB_HOST, self.DB_PORT, self.DB_NAME]):
            logger.error("Missing required environment variables")
            raise ValueError("Missing required environment variables")
        logger.success(f"Database successfully loaded")
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

settings = Settings()