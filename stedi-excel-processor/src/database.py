"""Database connection and session management"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
import os
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()

def get_database_url():
    """Get database URL from environment variables"""
    return os.getenv("DATABASE_URL") or os.getenv("DB_CONN")

# Get database URL
database_url = get_database_url()
if not database_url:
    raise ValueError("DATABASE_URL or DB_CONN is not configured. Please set it in .env file.")

# Create engine and session
engine = create_engine(database_url)
SessionLocal = sessionmaker(bind=engine)

def get_db_session() -> Session:
    """Get a database session"""
    return SessionLocal()

# Create a global session for convenience
db_session = get_db_session()
