# app/config/settings.py
import os
from functools import lru_cache
from typing import List, Optional
from pydantic_settings import BaseSettings
from pydantic import field_validator, Field
from dotenv import load_dotenv


load_dotenv()

class Settings(BaseSettings):
    # App Configuration
    APP_NAME: str = "P1 IV Automation Service"
    ENVIRONMENT: str = "development"
    HOST: str = "127.0.0.1"
    PORT: int = 8000
    DEBUG: bool = True
    
    # Security
    
    # CORS and Host Configuration
    ALLOWED_ORIGINS: List[str] = ["*"]
    ALLOWED_HOSTS: List[str] = ["*"]
    
    # External API Configuration
    # Base URLs for external APIs
    EXTERNAL_API_BASE_URL: Optional[str] = os.getenv("BASE_URL")

    # HTTP Client Configuration
    REQUEST_TIMEOUT: int = 200
    MAX_RETRIES: int = 3
    RETRY_BACKOFF_FACTOR: float = 0.3
    
    # Rate Limiting
    RATE_LIMIT_PER_MINUTE: int = 100
    
    # Logging Configuration
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Background Tasks
    CELERY_BROKER_URL: Optional[str] = None
    CELERY_RESULT_BACKEND: Optional[str] = None
    
    # Automation Configuration
    AUTOMATION_SCHEDULER_INTERVAL: int = 300  # 5 minutes
    AUTOMATION_MAX_CONCURRENT_TASKS: int = 50


def get_database_url():
    """Get database URL from AWS Secrets Manager or environment.

    Precedence:
    1. AWS Secrets Manager key DATABASE_URL (if available)
    2. Environment variable DATABASE_URL
    3. Environment variable DB_CONN (legacy)
    """
    # try:
    #     # from app.config.aws_secrets import DatabaseCredentialsManager
    #     db_secret = DatabaseCredentialsManager().get_db_credentials()
    #     if isinstance(db_secret, dict):
    #         secret_url = db_secret.get("DATABASE_URL")
    #         if secret_url:
    #             return secret_url
    # except Exception as e:
    #     from app.config.log_config import logger
    #     logger.exception("Could not fetch DB credentials from AWS: %s", e)

    # Fall back to environment variables (.env already loaded above)
    return os.environ.get("DATABASE_URL") or os.environ.get("DB_CONN")

@lru_cache()
def get_settings() -> Settings:
    return Settings()
