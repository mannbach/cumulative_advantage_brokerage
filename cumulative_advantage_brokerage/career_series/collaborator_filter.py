from sqlalchemy import select, Column, func, case, or_, and_, alias, distinct

from ..dbm.models.collaboration import Collaboration
from ..dbm.models.project import Project
from ..constants import\
    CAREER_LENGTH_MAX, DATE_OBSERVATION_END, DURATION_BUFFER_AUTHOR_ACTIVE

class CollaboratorFilter:
    """Defines filters on the set of collaborators.
    """
    def create_collaborator_source_subquery(self, return_sq: bool = True)\
            -> select:
        """Queries the set of collaborators to be considered for the analysis, filtered by the conditions specified in the subclass.

        Returns
        -------
        select
            A subquery selecting filtered collaborator IDs.
        """
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
        """Filter as described in the publication.
        Removes authors with
        - career length more than 40 years
        - less than two publications (or two publications at the same date)
        - authors who are considered active because their last publication is within fours years of the end of the observation period

        Returns
        -------
        `and_`-clause
            A clause specifying the conditions to filter collaborators.
        """
        return and_(
                func.max(Project.timestamp) != func.min(Project.timestamp),
                (func.max(Project.timestamp) - func.min(Project.timestamp)) <= CAREER_LENGTH_MAX,
                func.max(Project.timestamp) <= (DATE_OBSERVATION_END - DURATION_BUFFER_AUTHOR_ACTIVE)
            )
