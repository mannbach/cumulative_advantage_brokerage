"""Base class for all tables.
"""
from sqlalchemy import Column, Integer
from sqlalchemy.orm import declarative_base

class BaseMixin:
    """Base class for all tables.

    id (Integer): unique identifier for each row.
    """
    id = Column("id", Integer, primary_key=True)

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, __o: object) -> bool:
        return isinstance(__o, Base) and self.id == __o.id

Base = declarative_base(cls=BaseMixin)
