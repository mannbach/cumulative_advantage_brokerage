from sqlalchemy import select, Column, func, case, or_, and_, alias, distinct

from ..dbm import Collaboration, Project
from ..constants import\
    CAREER_LENGTH_MAX, DATE_OBSERVATION_END, DURATION_BUFFER_AUTHOR_ACTIVE

class CollaboratorFilter:
    def create_collaborator_source_subquery(self, return_sq: bool = True)\
            -> select:
        q = select(Collaboration.id_collaborator)\
        .select_from(Collaboration)\
        .join(Project, Collaboration.id_project == Project.id)\
        .group_by(Collaboration.id_collaborator)\
        .having(self._specify_conditions())
        return q.subquery() if return_sq else q

    @staticmethod
    def _specify_conditions():
        raise NotImplementedError

class StandardFilter(CollaboratorFilter):
    @staticmethod
    def _specify_conditions():
        return and_(
                func.max(Project.timestamp) != func.min(Project.timestamp),
                (func.max(Project.timestamp) - func.min(Project.timestamp)) <= CAREER_LENGTH_MAX,
                func.max(Project.timestamp) <= (DATE_OBSERVATION_END - DURATION_BUFFER_AUTHOR_ACTIVE)
            )
