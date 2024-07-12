from collections import defaultdict
from typing import List, Iterator, Dict, NamedTuple, Set, Union
from sqlalchemy import select, Column, func, case, or_, and_, alias, distinct

import numpy as np

from .collaborator_filter import CollaboratorFilter, StandardFilter
from ..constants import\
    CAREER_LENGTH_MAX, DURATION_BUFFER_AUTHOR_ACTIVE,\
    DATE_OBSERVATION_END
from ..dbm import\
    BinsRealization, CumAdvBrokSession,\
    CollaboratorSeriesBrokerage, HasSession,\
    Collaboration, Project,\
    TriadicClosureMotif, SimplicialTriadicClosureMotif

class _YieldBinSeries(NamedTuple):
    id_collaborator: int
    l_series: List[CollaboratorSeriesBrokerage]

class CollaboratorSeriesBrokerageBinner(HasSession):
    collaborator_filter: CollaboratorFilter
    id_metric_configuration: int
    _bins_np: np.ndarray
    _map_bin_pos_id: List[int]
    _map_collaborator_max_group: Dict[int, int]

    def __init__(self, *arg,
                 id_metric_configuration: Union[int, None],
                 bins: List[BinsRealization],
                 collaborator_filter: CollaboratorFilter=StandardFilter(),
                 **kwargs) -> None:
        self.id_metric_configuration = id_metric_configuration
        self.bins = bins
        self.collaborator_filter = collaborator_filter
        self._bins_np = np.asarray(
            [b.value for b in sorted(self.bins, key=lambda b: b.position)])
        self._map_bin_pos_id = np.asarray(
            [b.id for b in sorted(self.bins, key=lambda b: b.position)])
        super().__init__(*arg, **kwargs)
        self._init_map_collaborator_max_group()

    @classmethod
    def from_percentiles(
            cls,
            *args,
            id_metric_configuration: Union[int, None],
            percentiles: np.ndarray,
            session: CumAdvBrokSession,
            collaborator_filter: CollaboratorFilter=StandardFilter(),
            **kwargs):
        q_val_max = cls._create_query_max_value(collaborator_filter=collaborator_filter)

        a_values = np.asarray(
            [float(res[1])\
                for res in session.execute(q_val_max)])

        # @TODO: This should be replaced using PostgreSQL's own
        # `percentile_cont/_disc`-functions
        # (https://www.postgresql.org/docs/9.4/functions-aggregate.html).
        # Using numpy for this task
        # - is inefficient as the data needs to be retrieved completely
        # - is prone to errors, as the binning works differently
        l_bins = CollaboratorSeriesBrokerageBinner.\
            _create_bins_from_values(
                session=session,
                id_metric_configuration=id_metric_configuration,
                a_values=a_values,
                percentiles=percentiles)

        return cls(
            *args,
            id_metric_configuration=id_metric_configuration,
            bins=l_bins,
            session=session,
            **kwargs)

    @staticmethod
    def _create_query_max_value(collaborator_filter: CollaboratorFilter) -> select:
        sq_collaborators = collaborator_filter\
            ._create_collaborator_source_subquery()
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

    def _create_query_by_role(self, col_role: Column) -> select:
        sq_coll_birth = self.collaborator_filter._create_collaborator_source_subquery()
        sq_coll_motifs = alias(sq_coll_birth)

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

        sq_motifs = select(
                TriadicClosureMotif.id.label("id_motif"),
                sq_coll_motifs.c.id_collaborator.label("id_collaborator"),
                TriadicClosureMotif.motif_type.label("motif_type"),
                self._bin_metric((func.extract(
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
        sq_vals = CollaboratorSeriesBrokerageBinner\
            ._create_query_max_value(self.collaborator_filter)\
            .subquery()
        q_vals = select(
            sq_vals.c.id_collaborator,
            self._bin_metric(sq_vals.c.metric)
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

    def _bin_metric(self, metric: Column) -> Column:
        return case(
                    [(metric\
                          .between( # inclusive on both borders
                            float(self._bins_np[i]),
                            float(self._bins_np[i + 1])), i)
                        for i in range(len(self._bins_np) - 1)
                    ],
                    else_=len(self._bins_np) - 1).label("bin")

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

    @staticmethod
    def _create_bins_from_values(
            session: CumAdvBrokSession,
            id_metric_configuration: int,
            a_values=np.ndarray,
            percentiles=np.ndarray) -> List[BinsRealization]:
        _a_bins_np = np.quantile(a_values, q=percentiles)
        a_bins = _a_bins_np[:-1]
        _hist = np.histogram(a_values, bins=_a_bins_np)[0]

        print((f"Inferred bins {a_bins} for {len(a_values)} collaborators.\n"
               f"Histogram {_hist} (sum: {np.sum(_hist)}).\n"
               f"Sending to database under configuration ID: {id_metric_configuration}."))

        l_bins = [session.class_map.BinsRealization(
                id_metric_configuration=id_metric_configuration,
                position=pos,
                value=age_bin
            ) for pos, age_bin in enumerate(a_bins)]
        session.add_all(l_bins)
        session.commit()
        for _bin in l_bins:
            session.refresh(_bin)
        return l_bins

    def generate_binning(self) -> Iterator[_YieldBinSeries]:
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
                    id_collaborator_curr = id_collaborator
                    motif_type_curr = motif_type

                    a_counts = np.zeros(
                        self._map_collaborator_max_group[id_collaborator] + 1,
                        dtype=int)
                if (id_collaborator != id_collaborator_curr) or (motif_type != motif_type_curr):
                    yield self._init_yield(id_collaborator_curr, role, motif_type_curr, a_counts)
                    a_counts = np.zeros(
                        self._map_collaborator_max_group[id_collaborator] + 1,
                        dtype=int)
                    _d_cache_collaborators[motif_type_curr].add(id_collaborator_curr)
                    id_collaborator_curr = id_collaborator
                    motif_type_curr = motif_type

                if bin_pos < len(a_counts):
                    a_counts[bin_pos] = count
            yield self._init_yield(id_collaborator, role, motif_type, a_counts)
            _d_cache_collaborators[motif_type].add(id_collaborator)
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
