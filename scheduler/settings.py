from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    DB_HOST: str = "db"
    DB_PORT: int = 5432

    # Scheduler
    SCHEDULER_INITIAL_LOAD: bool = True
    SCHEDULER_HEALTH_CHECK_INTERVAL: int = 60

    @property
    def DATABASE_URL(self) -> str:
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.POSTGRES_DB}"
        )
settings = Settings()