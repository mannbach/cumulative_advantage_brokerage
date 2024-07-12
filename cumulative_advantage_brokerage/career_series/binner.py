from abc import abstractmethod
from typing import List, Optional

import numpy as np
from sqlalchemy.sql import select, func, distinct, alias, and_

from ..dbm import\
    HasSession, BinsRealization, CumAdvBrokSession,\
    Project, Collaboration, Citation
from ..constants import CS_BINS_PERCENTILES
from .collaborator_filter import CollaboratorFilter, StandardFilter

class PercentileBinner(HasSession):
    collaborator_filter: CollaboratorFilter
    id_metric_configuration: int
    percentiles: np.ndarray
    a_bin_values: Optional[np.ndarray] = None
    a_bin_realizations: Optional[List[BinsRealization]] = None

    def __init__(
            self, *arg,
            session: CumAdvBrokSession,
            id_metric_configuration: int,
            percentiles: np.ndarray = np.asarray(CS_BINS_PERCENTILES),
            collaborator_filter: CollaboratorFilter = StandardFilter(),
            **kwargs) -> None:
        super().__init__(*arg, session=session, **kwargs)
        self.collaborator_filter = collaborator_filter
        self.id_metric_configuration = id_metric_configuration
        self.percentiles = percentiles

    @abstractmethod
    def create_query_max_value(self) -> select:
        raise NotImplementedError

    def compute_binning_borders(self)->np.ndarray:
        q_val_max = self.create_query_max_value()

        a_values = np.asarray(
            [float(res[1])\
                for res in self.session.execute(q_val_max)])

        # @TODO: This should be replaced using PostgreSQL's own
        # `percentile_cont/_disc`-functions
        # (https://www.postgresql.org/docs/9.4/functions-aggregate.html).
        # Using numpy for this task
        # - is inefficient as the data needs to be retrieved completely
        # - is prone to errors, as the binning works differently
        bins = self._create_bins_from_values(
                a_values=a_values,
                percentiles=self.percentiles)
        self.a_bin_values = np.sort(np.asarray([border.value for border in bins]))
        self.a_bin_realizations = sorted(bins, key=lambda b: b.position)
        return bins

    def _create_bins_from_values(
            self,
            a_values=np.ndarray,
            percentiles=np.ndarray) -> List[BinsRealization]:
        _a_bins_np = np.quantile(a_values, q=percentiles)
        a_bins = _a_bins_np[:-1]
        _hist = np.histogram(a_values, bins=_a_bins_np)[0]

        print((f"Inferred bins {a_bins} for {len(a_values)} collaborators.\n"
               f"Histogram {_hist} (sum: {np.sum(_hist)}).\n"
               f"Sending to database under configuration ID: {self.id_metric_configuration}."))

        l_bins = [BinsRealization(
                id_metric_configuration=self.id_metric_configuration,
                position=pos,
                value=float(age_bin)
            ) for pos, age_bin in enumerate(a_bins)]
        self.session.commit_list(l=l_bins)

        return l_bins

class CareerLengthBinner(PercentileBinner):
    def create_query_max_value(self) -> select:
        sq_collaborators = self.collaborator_filter\
            .create_collaborator_source_subquery()
        return select(
            sq_collaborators.c.id_collaborator,
            (func.extract(
                "days",
                func.max(Project.timestamp) - func.min(Project.timestamp)) / 365)\
                    .label("metric"))\
        .select_from(sq_collaborators)\
        .join(Collaboration,
              Collaboration.id_collaborator == sq_collaborators.c.id_collaborator,
              isouter=True)\
        .join(Project,
              Project.id == Collaboration.id_project)\
        .group_by(sq_collaborators.c.id_collaborator)

class CitationsBinner(PercentileBinner):
    def create_query_max_value(self) -> select:
        sq_collaborators = self.collaborator_filter\
            .create_collaborator_source_subquery()

        sq_collaborators_death = CitationsBinner.\
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

class ProductivityBinner(PercentileBinner):
    def create_query_max_value(self) -> select:
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
