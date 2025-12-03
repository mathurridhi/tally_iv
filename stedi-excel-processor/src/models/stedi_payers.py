"""StediPayers database model"""
from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy.sql import func
from database import Base


class StediPayers(Base):
    __tablename__ = 'StediPayers'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    PayerId = Column(String(100), nullable=False)
    DisplayName = Column(String(255), nullable=False)
    Aliases = Column(String, nullable=True)
    EligibilityInquiry = Column(Boolean, nullable=False, default=False)
    ClaimStatusInquiry = Column(Boolean, nullable=False, default=False)
    CreatedAt = Column(DateTime, nullable=False, server_default=func.getdate())
    ModifiedAt = Column(DateTime, nullable=False, server_default=func.getdate(), onupdate=func.getdate())

    def __repr__(self):
        return f"<StediPayers(Id={self.Id}, PayerId='{self.PayerId}', DisplayName='{self.DisplayName}')>"

    def to_dict(self):
        """Convert model instance to dictionary"""
        return {
            'Id': self.Id,
            'PayerId': self.PayerId,
            'DisplayName': self.DisplayName,
            'Aliases': self.Aliases,
            'EligibilityInquiry': self.EligibilityInquiry,
            'ClaimStatusInquiry': self.ClaimStatusInquiry,
            'CreatedAt': self.CreatedAt.isoformat() if self.CreatedAt else None,
            'ModifiedAt': self.ModifiedAt.isoformat() if self.ModifiedAt else None
        }
