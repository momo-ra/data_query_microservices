from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from core.config import settings
from utils.log import setup_logger
from models.models import Base
logger = setup_logger(__name__)

# Initialize database connection
engine = create_async_engine(settings.DATABASE_URL, echo=False, future=True)
logger.info(f"Database URL: {settings.DATABASE_URL}")
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_db():
    async with SessionLocal() as session:
        try:
            logger.info("Creating database session")
            yield session
        except Exception as e:
            logger.error(f"Error in database session: {e}")
            raise e
        finally:
            logger.info("Closing database session")
            await session.close()

async def init_db():
    async with engine.begin() as conn:
        try:
            await conn.run_sync(Base.metadata.create_all)
            logger.success("Database tables created")
        except Exception as e:
            logger.error(f"Error creating database tables: {e}")
            raise e

