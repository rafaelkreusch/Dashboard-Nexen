from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator
from typing import Optional


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    DATABASE_URL: str = "sqlite:///./local.db"
    JWT_SECRET: str = "change_me"
    JWT_ALG: str = "HS256"
    REDIS_URL: str = "redis://localhost:6379/0"
    GOOGLE_CLIENT_ID: Optional[str] = None
    GOOGLE_CLIENT_SECRET: Optional[str] = None
    GOOGLE_REDIRECT_URI: Optional[str] = None
    CRON_DEFAULT_MINUTES: int = 60

    @field_validator('CRON_DEFAULT_MINUTES', mode='before')
    @classmethod
    def _intify(cls, v):
        try:
            return int(v)
        except Exception:
            return 60


settings = Settings()

