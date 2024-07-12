"""Defines the APS journal class
"""
from sqlalchemy import Column, ForeignKey
from sqlalchemy import Column

from .base_mixin import Base

class Citation(Base):
    """APS journals that publish papers (projects).
    """
    __tablename__ = "citation"

    id_project_citing = Column("id_project_citing", ForeignKey("project.id"))
    id_project_cited = Column("id_project_cited", ForeignKey("project.id"))
