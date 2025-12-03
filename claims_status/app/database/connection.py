# app/database/connection.py
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from typing import Dict, Generator
import logging
from contextlib import contextmanager

from app.config.settings import get_settings, get_database_url
# from app.core.exceptions import DatabaseException

logger = logging.getLogger(__name__)
settings = get_settings()

# Get database URL from AWS Secrets Manager or environment
database_url = get_database_url()
if not database_url:
    raise ValueError("DATABASE_URL is not configured. Please set it in environment variables or AWS Secrets Manager.")

eng = create_engine(database_url)
engine = eng.execution_options(isolation_level="AUTOCOMMIT")
Session = sessionmaker(bind=engine)

class DatabaseManager:
    """Multi-tenant database connection manager"""

    @staticmethod
    def get_db():
        """Provides a database session that can be used across the application."""
        db = Session()
        try:
            yield db  # Provide the session
        finally:
            db.close()

db_session = next(DatabaseManager.get_db())