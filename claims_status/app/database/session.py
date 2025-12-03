# app/database/session.py
from sqlalchemy.orm import Session
from typing import Generator, Optional
import logging
from contextlib import contextmanager
from typing import Dict
from .connection import DatabaseManager
# from app.core import get_current_tenant_id

logger = logging.getLogger(__name__)


class SessionManager:
    """Session management for multi-tenant application"""
    
    @staticmethod
    def get_session(tenant_id: Optional[str] = None) -> Session:
        """
        Get database session for specific tenant
        """
        if not tenant_id:
            tenant_id = "1"
        
        session_maker = DatabaseManager.get_session_maker(tenant_id)
        return session_maker()
    
    @staticmethod
    @contextmanager
    def get_session_context(tenant_id: Optional[str] = None, auto_commit: bool = True):
        """
        Context manager for database session with automatic transaction management
        """
        if not tenant_id:
            tenant_id = get_current_tenant_id() or "1"
        
        session = SessionManager.get_session(tenant_id)
    
        try:
            yield session
            if auto_commit:
                session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database transaction error for tenant {tenant_id}: {str(e)}")
            raise
        finally:
            session.close()


    @staticmethod
    @contextmanager
    def get_read_only_session(tenant_id: Optional[str] = None):
        """
        Context manager for read-only database operations
        """
        if not tenant_id:
            tenant_id = get_current_tenant_id() or "1"
        
        session = SessionManager.get_session(tenant_id)
        try:
            # Configure session for read-only
            session.configure(autoflush=False)
            yield session
        except Exception as e:
            logger.error(f"Database read error for tenant {tenant_id}: {str(e)}")
            raise
        finally:
            session.close()


def get_db_session(tenant_id: Optional[str] = None) -> Generator[Session, None, None]:
    """
    FastAPI dependency to get database session for specific tenant
    """
    if not tenant_id:
        tenant_id = get_current_tenant_id() or "1"

    session_maker = DatabaseManager.get_session_maker(tenant_id)
    session = session_maker()
    try:
        yield session
    except Exception as e:
        session.rollback()
        logger.error(f"Database session error for tenant {tenant_id}: {str(e)}")
        raise
    finally:
        session.close()


def get_tenant_db_session(tenant_id: str) -> Generator[Session, None, None]:
    """
    FastAPI dependency to get database session for explicit tenant
    """
    session_maker = DatabaseManager.get_session_maker(tenant_id)
    session = session_maker()
    try:
        yield session
    except Exception as e:
        session.rollback()
        logger.error(f"Database session error for tenant {tenant_id}: {str(e)}")
        raise
    finally:
        session.close()
