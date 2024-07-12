from typing import Iterator, Tuple, Dict, NamedTuple, List, Any
from datetime import datetime
from collections import defaultdict

from sqlalchemy.dialects.postgresql import insert

from .motif_factory import MotifFactory
from ..dbm import\
    HasSession, CumAdvBrokSession, BaseTriadicClosureMotif
from ..constants import\
    CN_EVENT_LINK_ADD_AFTER, CN_EVENT_LINK_ADD_BEFORE,\
    CN_EVENT_PROJECT_ADD_BEFORE, CN_EVENT_DATE_ADD_BEFORE,\
    CN_EVENT_DATE_ADD_AFTER
from .growing_temporal_linked_list_network import\
    GrowingTemporalLinkedListNetwork, LinkAttribute,\
    ProjectAttribute, DateYield

_OpenTriangle = NamedTuple(
    "_OpenTriangle",
    node_init=int,
    id_project_ab=int,
    id_project_bc=int,
    t_first=datetime,
    t_second=datetime)

class InitiationMotifCollector(HasSession):
    """Implements a motif counter with the following specifications:
    - Currency:
        Focus on first(!) links in case of multiple pre-closure collaborations.

    - Recurrence:
        Stops after closure (each triplet is counted maximally once).

    - Simplicial dominance:
        In case both a simplicial and regular closure
        appear on the same date, only the simplicial is counted.
    """
    network: GrowingTemporalLinkedListNetwork

    _triangles_open: Dict[Tuple[int, int], Dict[int, _OpenTriangle]]

    _id_current_project: int
    _d_current_date_projects: Dict[int, Dict[int,int]]

    _motif_factory: MotifFactory
    _l_motifs: List[Dict[str, Any]]

    def __init__(
            self, network: GrowingTemporalLinkedListNetwork,
            session: CumAdvBrokSession, **kwargs) -> None:
        self.network = network

        self._triangles_open = defaultdict(dict)
        self._d_current_date_projects: Dict[int, Dict[int,int]] = {}

        self._register_event_handlers()

        self._motif_factory = MotifFactory(session=session)
        self._l_motifs = []

        super().__init__(session=session, **kwargs)

    def _register_event_handlers(self):
        self.network.register_event_handler(
            event=CN_EVENT_LINK_ADD_AFTER,
            event_handler=lambda link, link_attr: self._identify_triangle_opening(
                link=link, link_attr=link_attr))
        self.network.register_event_handler(
            event=CN_EVENT_LINK_ADD_BEFORE,
            event_handler=lambda link, link_attr: self._identify_triangle_closure(
                link=link, link_attr=link_attr))
        self.network.register_event_handler(
            event=CN_EVENT_PROJECT_ADD_BEFORE,
            event_handler=lambda project, project_attr: self._register_current_project(
                project_attr=project_attr))
        self.network.register_event_handler(
            event=CN_EVENT_DATE_ADD_BEFORE,
            event_handler=lambda date: self._register_current_date(
                date=date))
        self.network.register_event_handler(
            event=CN_EVENT_DATE_ADD_AFTER,
            event_handler=lambda date: self._commit_motifs())

    def _identify_triangle_opening(self, link: Tuple[int, int], link_attr: LinkAttribute) -> None:
        u, v = link
        neigh_u = self.network.network[u].difference({u,v})
        neigh_v = self.network.network[v].difference({u,v})

        for w in neigh_u.difference(neigh_v):
            tpl_open = (v,w) if v < w else (w,v)
            tpl_init = (u,w) if u < w else (w,u)

            # Initiation focus
            if (tpl_open in self._triangles_open) and (u in self._triangles_open[tpl_open]):
                continue

            link_uw_attr = self.network.edges[tpl_init][0]
            self._triangles_open[tpl_open][u] = _OpenTriangle(
                node_init=w,
                id_project_ab=link_uw_attr.id_project,
                id_project_bc=link_attr.id_project,
                t_first=link_uw_attr.timestamp,
                t_second=link_attr.timestamp
            )
        for w in neigh_v.difference(neigh_u):
            tpl_open = (u,w) if u < w else (w,u)
            tpl_init = (v,w) if v < w else (w,v)

            # Initiation focus:
            # If the triangle is open already, don't(!) overwrite it
            if (tpl_open in self._triangles_open) and (v in self._triangles_open[tpl_open]):
                continue

            link_vw_attr = self.network.edges[tpl_init][0]
            self._triangles_open[tpl_open][v] = _OpenTriangle(
                node_init=w,
                id_project_ab=link_vw_attr.id_project,
                id_project_bc=link_attr.id_project,
                t_first=link_vw_attr.timestamp,
                t_second=link_attr.timestamp
            )

    def _identify_triangle_closure(self, link: Tuple[int, int], link_attr: LinkAttribute) -> None:
        u, v = link
        if u == v:
            return
        if (u,v) in self._triangles_open and self._triangles_open[(u,v)] is not None:
            for node_broker, triangle in self._triangles_open[(u,v)].items():
                tpl_collaborators = (
                    triangle.node_init,
                    node_broker,
                    u if u != triangle.node_init else v)
                tpl_projects = (
                    triangle.id_project_ab,
                    triangle.id_project_bc,
                    self._id_current_project)

                motif = self._motif_factory.identify_motif_type(
                    tpl_collaborators=tpl_collaborators,
                    tpl_projects=tpl_projects,
                    dt_open=triangle.t_second - triangle.t_first,
                    dt_close=link_attr.timestamp - triangle.t_second,
                    # Enforce simplicial dominance:
                    # Set flag in case the three nodes appear together
                    # in ANY other publication of the same date
                    is_simplicial=any(
                        {u, node_broker, v}.issubset(project)\
                            for project in self._d_current_date_projects.values())
                )
                self._l_motifs.append({
                    "id_collaborator_a": motif.id_collaborator_a,
                    "id_collaborator_b": motif.id_collaborator_b,
                    "id_collaborator_c": motif.id_collaborator_c,
                    "id_project_ab": motif.id_project_ab,
                    "id_project_bc": motif.id_project_bc,
                    "id_project_ac": motif.id_project_ac,
                    "motif_type": motif.__class__._motif_type,
                    "dt_open": motif.dt_open if hasattr(motif, "dt_open") else None,
                    "dt_close": motif.dt_close if hasattr(motif, "dt_close") else None,
                })
            self._triangles_open[(u,v)] = None

    def _register_current_project(self, project_attr: ProjectAttribute):
        self._id_current_project = project_attr.id_project

    def _register_current_date(self, date: DateYield):
        self._d_current_date_projects = date.collaborations

    def _commit_motifs(self):
        if len(self._l_motifs) == 0:
            return

        stmt_ins = insert(BaseTriadicClosureMotif)\
            .values(self._l_motifs)
        stmt_do_nothing = stmt_ins.on_conflict_do_nothing()
        self.session.execute(stmt_do_nothing)
        self.session.commit()

        self._l_motifs = []

    def generate_motifs(self) -> Iterator[Tuple[BaseTriadicClosureMotif]]:
        for _ in self.network.generate_network():
            yield self._l_motifs
            self._l_motifs = []

    def integrate_counts(self, stop_after: Tuple[int,None] = None) -> None:
        for i, _ in enumerate(self.generate_motifs()):
            if stop_after is not None and i == stop_after:
                return
