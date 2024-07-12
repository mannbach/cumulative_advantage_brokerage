"""Defines nxm-relationship table between collaborators and projects.
"""
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import declared_attr, relationship

from .base_mixin import Base

# pylint: disable=no-self-argument
class Collaboration(Base):
    """Abstract class that defines collaboration (connection between collaborators and projects).
    """
    __tablename__ = "collaboration"

    @declared_attr
    def id_collaborator(cls):
        """Connection to `collaborator.id`.
        """
        return Column("id_collaborator", ForeignKey("collaborator.id"), index=True)

    @declared_attr
    def id_project(cls):
        """Connection to `project.id`.
        """
        return Column("id_project", ForeignKey("project.id"), index=True)

    @declared_attr
    def collaborator(cls):
        """Define relationship to collaborator.
        """
        return relationship("Collaborator", back_populates="collaborations")

    @declared_attr
    def project(cls):
        """Define relationship to project.
        """
        return relationship("Project", back_populates="collaborations")

    id_collaborator_name = Column(Integer, ForeignKey("collaborator_name.id"))
