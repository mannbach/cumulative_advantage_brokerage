"""Binning of career series metrics base on percentiles.
"""
from abc import abstractmethod
from typing import List, Optional

import numpy as np
from sqlalchemy import\
    select, func, distinct,\
    alias, and_, case, Column

from ..dbm import\
    HasSession, BinsRealization, CumAdvBrokSession,\
    Project, Collaboration, Citation
from ..constants import CS_BINS_PERCENTILES
from .collaborator_filter import CollaboratorFilter, StandardFilter

class PercentileBinner(HasSession):
    """Percentile-based binning of career series metrics.
    """
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
        """Percentile-based binning of career series metrics.

        Parameters
        ----------
        session : CumAdvBrokSession
            Session object to communicate with the database.
        id_metric_configuration : int
            ID of `MetricConfiguration`-object to be used to reference results in database.
        percentiles : np.ndarray, optional
            Array of the percentile border-values, including minimum and maximum values, by default np.asarray(CS_BINS_PERCENTILES)
        collaborator_filter : CollaboratorFilter, optional
            Filter to apply to the set of all `Collaborator`s., by default StandardFilter()
        """
        super().__init__(*arg, session=session, **kwargs)
        self.collaborator_filter = collaborator_filter
        self.id_metric_configuration = id_metric_configuration
        self.percentiles = percentiles

    @abstractmethod
    def create_query_max_value(self) -> select:
        raise NotImplementedError

    def compute_binning_borders(self)->np.ndarray:
        """Compute the binning borders based on the percentiles values.
        """
        # Query the maximum value of all collaborators
        q_val_max = self.create_query_max_value()

        # Retrieve only the final values
        # (career length, citations, productivity)
        a_values = np.asarray(
            [float(res[1])\
                for res in self.session.execute(q_val_max)])

        # Infer border values using numpy's quantile function
        bins = self._create_bins_from_values(
                a_values=a_values,
                percentiles=self.percentiles)
        bins_sorted = sorted(bins, key=lambda b: b.position)
        self.a_bin_realizations = bins_sorted
        self.a_bin_values = np.asarray([border.value for border in bins_sorted])
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

        # Translate bins to database objects
        l_bins = [BinsRealization(
                id_metric_configuration=self.id_metric_configuration,
                position=pos,
                value=float(age_bin)
            ) for pos, age_bin in enumerate(a_bins)]
        self.session.commit_list(l=l_bins)

        return l_bins

    def bin_metric(self, metric: Column) -> Column:
        """Assigns a bin to a given metric value.

        Parameters
        ----------
        metric : Column
            The respective metric value column to be binned.

        Returns
        -------
        Column
            A switch-case to assign a bin to the metric value. The finale bin can be extracted from the `bin`-column.
        """
        assert self.a_bin_values is not None, "Binning borders not computed."
        return case(
            [(and_(
                metric >= float(self.a_bin_values[i]),
                metric < float(self.a_bin_values[i + 1])), i)
                    for i in range(len(self.a_bin_values) - 1)
            ],
            else_=len(self.a_bin_values) - 1).label("bin")

class CareerLengthBinner(PercentileBinner):
    """Binner for career length.
    """
    def create_query_max_value(self) -> select:
        """Query career length for all collaborators.
        The career length is defined by the duration between an author's first and last publication in years.

        Returns
        -------
        select
            A sub-query to retrieve the career length for all collaborators.
        """
        # Select from filtered set
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
        """Query final citations for all collaborators.
        The citation count is defined by the accumulated citations at the end of an author's career.

        Returns
        -------
        select
            A sub-query to retrieve the citation count for all collaborators.
        """
        # Select from filtered set
        sq_collaborators = self.collaborator_filter\
            .create_collaborator_source_subquery()

        # Create subquery to retrieve the last publication date of all collaborators
        sq_collaborators_death = CitationsBinner.\
            _create_collaborator_death_query(sq_collaborators=sq_collaborators)

        # Count incoming citations over all projects
        # A `distinct`-call is omitted to count multiple incoming citations of an another publication towards more than one projects
        # Filter citing papers by the last publication date of the collaborator to not accumulate forever
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
        """Query final productivity for all collaborators.
        The productivity is defined by the number of publications at the end of an author's career.

        Returns
        -------
        select
            A sub-query to retrieve the productivity for all collaborators.
        """
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
