# scheduler/database/engine.py
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from scheduler.settings import settings
from contextlib import asynccontextmanager
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

# Контекстный менеджер для получения сессии
@asynccontextmanager
async def get_db() -> AsyncSession:
    """
    Асинхронный генератор сессий.
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