"""Defines APS author names.
"""
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from .base_mixin import Base

class CollaboratorName(Base):
    """Defines author names.
    As a result of name disambiguation, a single author can be associated with multiple names.
    """
    __tablename__ = "collaborator_name"

    id_collaborator = Column(Integer, ForeignKey("collaborator.id"))
    name = Column(String(255))

    collaborator = relationship("Collaborator", back_populates="names")
