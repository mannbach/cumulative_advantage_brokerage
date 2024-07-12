"""Defines APS collection.
"""
from dataclasses import dataclass
from typing import List, Union

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models.gender import Gender
from .models.collaboration import Collaboration
from .models.project import Project
from .models.collaborator import Collaborator
from .models.collaborator_name import CollaboratorName
from .models.citation import Citation
from .postgresql_engine import PostgreSQLEngine

@dataclass
class APSCollection:
    """Collection that holds references to the data table.
    """
    genders: Union[List[Gender], None] = None
    projects: Union[List[Project], None] = None
    collaborators: Union[List[Collaborator], None] = None
    collaborator_names: Union[List[CollaboratorName], None] = None
    collaborations: Union[List[Collaboration], None] = None
    citations: Union[List[Citation], None] = None

    @classmethod
    def from_database(cls, engine: PostgreSQLEngine):
        obj = cls()
        with Session(engine) as session:
            obj.genders = session.scalars(select(Gender)).all()
            obj.projects = session.scalars(select(Project)).all()
            obj.collaborators = session.scalars(select(Collaborator)).all()
            obj.collaborator_names = session.scalars(select(CollaboratorName)).all()
            obj.collaborations = session.scalars(select(Collaboration)).all()
            obj.citations = session.scalars(select(Citation)).all()
        return obj
