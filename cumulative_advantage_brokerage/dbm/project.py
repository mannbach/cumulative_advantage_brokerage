"""Holds project class implementation.
"""
from sqlalchemy import Column, String, DateTime
from sqlalchemy.orm import relationship, declared_attr

from .base_mixin import Base

# pylint: disable=no-self-argument
class Project(Base):
    """Project in APS dataset.
    A project is defined by a publication.
    """
    __tablename__ = "project"

    @declared_attr
    def collaborations(cls):
        """The collaborations defined by this project.
        """
        return relationship(
            "Collaboration",
            back_populates="project"
        )

    timestamp = Column(DateTime)

    # DOI of the paper
    doi = Column(String(64))
