# api/database/engine.py
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from api.settings import settings
from typing import AsyncGenerator
import logging

logger = logging.getLogger(__name__)

# Создаём асинхронный движок
engine: AsyncEngine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_size=20,
    max_overflow=30,
    pool_recycle=3600,
    connect_args={
        "command_timeout": 60,  # Таймаут подключения к БД
        "server_settings": {
            "application_name": "scheduler_service"
        }
    }
)

# Фабрика сессий
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)

async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Асинхронный генератор сессий для Dependency Injection в FastAPI.
    """
    session = AsyncSessionLocal()
    try:
        yield session
        await session.commit()
    except Exception as e:
        await session.rollback()
        logger.error(f"Database session error: {e}")
        raise
    finally:
        await session.close()