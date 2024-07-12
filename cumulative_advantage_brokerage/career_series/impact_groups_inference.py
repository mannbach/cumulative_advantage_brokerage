from typing import List
from sqlalchemy import select, func, and_, alias, distinct

from ..dbm import\
    ImpactGroup, CumAdvBrokSession, HasSession,\
    Collaboration, Project, Citation
from .collaborator_filter import CollaboratorFilter, StandardFilter

class ImpactGroupsInference(HasSession):
    collaborator_filter: CollaboratorFilter

    def __init__(
            self, *arg,
            session: CumAdvBrokSession,
            collaborator_filter: CollaboratorFilter=StandardFilter(), **kwargs) -> None:
        super().__init__(*arg, session=session, **kwargs)
        self.collaborator_filter = collaborator_filter

    def compute_impact_groups(self) -> List[ImpactGroup]:
        q = self._create_query_max_value()
        l_impact_groups = []
        for id_collaborator, metric in self.session.execute(q):
            l_impact_groups.append(
                ImpactGroup(metric=metric, id_collaborator=id_collaborator))
        return l_impact_groups

    def _create_query_max_value(self) -> select:
        raise NotImplementedError

class CitationsImpactGroupInference(ImpactGroupsInference):
    def _create_query_max_value(self) -> select:
        sq_collaborators = self.collaborator_filter\
            .create_collaborator_source_subquery()

        sq_collaborators_death = CitationsImpactGroupInference.\
            _create_collaborator_death_query(sq_collaborators=sq_collaborators)

        project_collab, project_citing = alias(Project), alias(Project)
        _q = select(
            sq_collaborators_death.c.id_collaborator,
            func.count(project_citing.c.id)\
                .label("metric"))\
        .select_from(sq_collaborators_death)\
        .join(Collaboration,
              Collaboration.id_collaborator == sq_collaborators_death.c.id_collaborator,
              isouter=True)\
        .join(project_collab,
              project_collab.c.id == Collaboration.id_project)\
        .join(Citation,
              Citation.id_project_cited == project_collab.c.id,
              isouter=True
        )\
        .join(project_citing,
              and_(project_citing.c.id == Citation.id_project_citing,
                   project_citing.c.timestamp <= sq_collaborators_death.c.death),
              isouter=True
        )\
        .group_by(sq_collaborators_death.c.id_collaborator)

        return _q

    @staticmethod
    def _create_citations_per_paper_subquery() -> select:
        return select(
            Citation.id_project_cited.label("id_project_cited"),
            func.count(Citation.id_project_citing).label("citations"))\
            .select_from(Citation)\
            .group_by(Citation.id_project_cited)\
            .subquery()


    @staticmethod
    def _create_collaborator_death_query(sq_collaborators: select) -> select:
        return select(
            sq_collaborators.c.id_collaborator,
            func.max(Project.timestamp).label("death"))\
            .select_from(sq_collaborators)\
            .join(Collaboration,
                  Collaboration.id_collaborator == sq_collaborators.c.id_collaborator)\
            .join(Project, Project.id == Collaboration.id_project)\
            .group_by(sq_collaborators.c.id_collaborator)\
            .subquery()

class ProductivityImpactGroupInference(ImpactGroupsInference):
    def _create_query_max_value(self) -> select:
        sq_collaborators = self.collaborator_filter\
            .create_collaborator_source_subquery()
        return select(
                sq_collaborators.c.id_collaborator,
                (func.count(distinct(Collaboration.id_project))).label("metric"))\
            .select_from(sq_collaborators)\
            .join(Collaboration,
                Collaboration.id_collaborator == sq_collaborators.c.id_collaborator,
                isouter=True)\
            .group_by(sq_collaborators.c.id_collaborator)
