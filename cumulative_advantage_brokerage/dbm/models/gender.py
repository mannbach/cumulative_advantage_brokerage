"""APS gender realization.
"""
from .base_mixin import Base
from sqlalchemy import Column, String

class Gender(Base):
    """Gender model."""
    __tablename__ = "gender"

    gender = Column(String(64))
