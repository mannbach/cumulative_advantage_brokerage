from collections import defaultdict
from typing import List, Iterator, Dict, NamedTuple, Set, Union
from sqlalchemy import select, Column, func, or_, alias

import numpy as np

from .binner import PercentileBinner
from .collaborator_filter import CollaboratorFilter, StandardFilter
from ..constants import\
    CAREER_LENGTH_MAX, DURATION_BUFFER_AUTHOR_ACTIVE,\
    DATE_OBSERVATION_END
from ..dbm import\
    CollaboratorSeriesBrokerage, HasSession,\
    Collaboration, Project,\
    TriadicClosureMotif, SimplicialTriadicClosureMotif

class _YieldBinSeries(NamedTuple):
    id_collaborator: int
    l_series: List[CollaboratorSeriesBrokerage]

class CollaboratorSeriesBrokerageInference(HasSession):
    """Inference of brokerage series for collaborators.
    """
    collaborator_filter: CollaboratorFilter
    id_metric_configuration: int
    _map_bin_pos_id: List[int]
    _map_collaborator_max_group: Dict[int, int]

    def __init__(self, *arg,
                 id_metric_configuration: Union[int, None],
                 binner: PercentileBinner,
                 collaborator_filter: CollaboratorFilter=StandardFilter(),
                 **kwargs) -> None:
        """Inference of brokerage series for collaborators.

        Parameters
        ----------
        id_metric_configuration : Union[int, None]
            `MetricConfiguration`-ID to reference the results in the database.
        binner : PercentileBinner
            A binner object that was used to compute the binning borders.
        collaborator_filter : CollaboratorFilter, optional
            Filter for the set of collaborators, by default StandardFilter()
        """
        assert binner.a_bin_values is not None, "Binning borders not computed."

        self.id_metric_configuration = id_metric_configuration
        self.binner = binner
        self.collaborator_filter = collaborator_filter

        # Sort bins and create a numpy array for faster binning
        bins_sorted = sorted(self.binner.a_bin_realizations, key=lambda b: b.position)
        self.bins = bins_sorted
        self._map_bin_pos_id = np.asarray(
            [b.id for b in bins_sorted])

        super().__init__(*arg, **kwargs)
        # Create a cache of the final career length of all authors
        self._init_map_collaborator_max_group()

    def _create_query_by_role(self, col_role: Column) -> select:
        """Create queries to aggregate the counts of motifs for a given role per career stage.

        Parameters
        ----------
        col_role : The column representing the role of the collaborator in the motif. Either `TriadicClosureMotif.id_collaborator_a`, `TriadicClosureMotif.id_collaborator_b`, or `TriadicClosureMotif.id_collaborator_c`.

        Returns
        -------
        select
            Sub-query to aggregate the counts of motifs for a given role per career stage.
        """
        # Filter collaborator set
        sq_coll_birth = self.collaborator_filter\
            .create_collaborator_source_subquery()

        sq_coll_motifs = alias(sq_coll_birth)

        # Select starting point of career
        # Outer-join is used to not filter out brokerage events for which a subset of authors is not in the filtered set.
        sq_career_start = select(
                sq_coll_birth.c.id_collaborator.label("id_collaborator_birth"),
                func.min(Project.timestamp).label("birth"))\
            .select_from(sq_coll_birth)\
            .join(Collaboration,
                  Collaboration.id_collaborator == sq_coll_birth.c.id_collaborator,
                  isouter=True)\
            .join(Project, Collaboration.id_project == Project.id)\
            .group_by(sq_coll_birth.c.id_collaborator)\
            .subquery()

        # Assign career stage by binning the duration between the birth and the brokerage project timestamp `t_ac`.
        sq_motifs = select(
                TriadicClosureMotif.id.label("id_motif"),
                sq_coll_motifs.c.id_collaborator.label("id_collaborator"),
                TriadicClosureMotif.motif_type.label("motif_type"),
                self.binner.bin_metric((func.extract(
                    "days",
                    Project.timestamp - sq_career_start.c.birth) / 365)))\
            .select_from(sq_coll_motifs)\
            .join(TriadicClosureMotif,
                  col_role == sq_coll_motifs.c.id_collaborator,
                  isouter=True)\
            .join(Project,
                  Project.id == TriadicClosureMotif.id_project_ac)\
            .join(sq_career_start,
                  sq_career_start.c.id_collaborator_birth == col_role)\
            .where(or_(
                TriadicClosureMotif.motif_type == TriadicClosureMotif._motif_type,
                TriadicClosureMotif.motif_type == SimplicialTriadicClosureMotif._motif_type
            ))\
            .subquery()

        return sq_motifs

    def _init_map_collaborator_max_group(self):
        sq_vals = self.binner\
            .create_query_max_value()\
            .subquery()
        q_vals = select(
            sq_vals.c.id_collaborator,
            self.binner.bin_metric(sq_vals.c.metric)
        )

        self._map_collaborator_max_group = dict(
            (id_c, c_bin) for (id_c, c_bin) in self.session.execute(q_vals))

    def _init_yield(self,
                    id_collaborator:int,
                    role: str,
                    motif_type: str,
                    a_counts: np.ndarray) -> _YieldBinSeries:
        return _YieldBinSeries(
                id_collaborator,
                [CollaboratorSeriesBrokerage(
                    role=role,
                    id_collaborator=int(id_collaborator),
                    motif_type=motif_type,
                    id_bin=int(self._map_bin_pos_id[bin_pos]),
                    id_metric_configuration=int(self.id_metric_configuration) if self.id_metric_configuration is not None else None,
                    value=int(cnt))\
                        for bin_pos, cnt in enumerate(a_counts)
                ])

    def _aggregate_role_query(self, col_role: Column) -> select:
        query = self._create_query_by_role(col_role=col_role)
        return select(
                query.c.id_collaborator,
                query.c.motif_type,
                query.c.bin,
                func.count(query.c.id_motif))\
            .group_by(
                query.c.id_collaborator,
                query.c.motif_type,
                query.c.bin)\
            .order_by(
                query.c.id_collaborator,
                query.c.motif_type,
                query.c.bin)

    def generate_series(self) -> Iterator[_YieldBinSeries]:
        """Generates the brokerage frequency career stage series.
        This function iterates over all roles and collaborator IDs to aggregate the counts of motifs for a given role per career stage.
        It considers zero counts for a role-motif combination if no counts were found in the database.
        This way, each collaborator will always contribute `3x2xn_stages` values, where `n_stages` is the number of stages in which the collaborator published.

        Yields
        ------
        Iterator[_YieldBinSeries]
            Yields a tuple of the collaborator ID and a list of `CollaboratorSeriesBrokerage` objects.
        """
        id_collaborator, id_collaborator_curr = -1, -1
        motif_type, motif_type_curr = None, None
        _d_cache_collaborators: Dict[str, Set[int]] = defaultdict(set)

        for role, col_role in zip(
                "abc",
                (TriadicClosureMotif.id_collaborator_a,
                 TriadicClosureMotif.id_collaborator_b,
                 TriadicClosureMotif.id_collaborator_c)):
            print(f"Binning on role `{role}'.")
            for id_collaborator, motif_type, bin_pos, count in\
                    self.session.execute(self._aggregate_role_query(col_role)):
                if id_collaborator_curr == -1:
                    # Initial configuration
                    id_collaborator_curr = id_collaborator
                    motif_type_curr = motif_type

                    # Init counts array which counts the brokerage events per stage
                    a_counts = np.zeros(
                        self._map_collaborator_max_group[id_collaborator] + 1,
                        dtype=int)

                if (id_collaborator != id_collaborator_curr) or (motif_type != motif_type_curr):
                    # Change of motif type or role: yield result
                    yield self._init_yield(id_collaborator_curr, role, motif_type_curr, a_counts)

                    # Remember collaborator
                    _d_cache_collaborators[motif_type_curr].add(id_collaborator_curr)

                    # Reset counts array
                    a_counts = np.zeros(
                        self._map_collaborator_max_group[id_collaborator] + 1,
                        dtype=int)

                    # Reset current collaborator info
                    id_collaborator_curr = id_collaborator
                    motif_type_curr = motif_type

                if bin_pos < len(a_counts):
                    # Update count
                    a_counts[bin_pos] = count

            # Yield last collaborator
            yield self._init_yield(id_collaborator, role, motif_type, a_counts)
            _d_cache_collaborators[motif_type].add(id_collaborator)

            # Add zero counts for other motif_types
            id_collaborator_curr = -1
            motif_type_curr = None
            a_counts = np.zeros(
                self._map_collaborator_max_group[id_collaborator] + 1,
                dtype=int)

            print("Adding zero counts for other combinations of motif_type and role")
            for motif_type, _s_cache_collaborators in _d_cache_collaborators.items():
                _s_zero_count_collaborators = set(self._map_collaborator_max_group.keys())\
                    .difference(_s_cache_collaborators)
                print((f"\t Adding for `{motif_type}': {len(_s_zero_count_collaborators)}"
                       " authors with zero counts."))
                for id_collaborator in _s_zero_count_collaborators:
                    a_counts = np.zeros(
                        self._map_collaborator_max_group[id_collaborator] + 1,
                        dtype=int)
                    yield self._init_yield(id_collaborator, role, motif_type, a_counts)
                _d_cache_collaborators[motif_type] = set()
