"""APS gender realization.
"""
from .base_mixin import Base
from sqlalchemy import Column, String

class Gender(Base):
    """Gender model."""
    __tablename__ = "gender"

    gender = Column(String(64))

GENDER_UNKNOWN = Gender(id=0, gender="unknown")
GENDER_FEMALE = Gender(id=1, gender="female")
GENDER_MALE = Gender(id=2, gender="male")
