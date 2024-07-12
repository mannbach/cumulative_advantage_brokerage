from collections import defaultdict
from datetime import datetime
from dataclasses import dataclass
from typing import NamedTuple, Dict, Iterator, List, Tuple

from sqlalchemy import select
import numpy as np

from ..dbm import\
    HasSession, CumAdvBrokSession,\
    Project, Collaboration, Collaborator

class CollaboratorYield(NamedTuple):
    """Type of collaborator yielded.
    """
    id_gender: int # gender ID of that collaborator
    count_projects: int # count of projects in the current yield

@dataclass
class DateYield:
    """Tuple returned with each yield,
        containing all collaborators who published on that date and their respective links.
    """
    timestamp: datetime # Timestamp of the returned data.
    # Dict of collaborators who released at least one project
    # mapped to their gender and project count.
    # Does not necessarily match the union over all collaborations
    # as some collaborators might publish a project alone which
    # would not result in the formation of collaborations but might
    # introduce the collaborator to the system.
    collaborators: Dict[int, CollaboratorYield]

    # Dictionary that maps project IDs to another map that links collaborator IDs to the respective collaboration ID.
    collaborations: Dict[int, Dict[int, int]]

class SQLEdgeGenerator(HasSession):
    """Generator that yields edges iteratively.
    """
    _l_collaborations: np.ndarray

    def __init__(self, *arg,
                 session: CumAdvBrokSession,
                 skip_preprocess: bool = False, **kwargs) -> None:
        super().__init__(*arg, session=session, **kwargs)
        self.map_collaborator_gender = {}
        self._init_map_collaborator_gender()
        if not skip_preprocess:
            q_edges = self._create_sql_query()
            self._preprocess_collaboration(q_edges)
        else:
            self._l_collaborations = None

    def _create_sql_query(self) -> select:
        return select(
                Project.timestamp,
                Project.id,
                Collaboration.id_collaborator,
                Collaboration.id).\
            join(Project, Collaboration.id_project == Project.id).\
            join(Collaborator,
                Collaboration.id_collaborator == Collaborator.id).\
            order_by(Project.timestamp.asc(), # Fix temporal order
                     Project.id.asc(),
                     Collaborator.id.asc())

    def _preprocess_collaboration(self, q_edges: select):
        self._l_collaborations = np.asarray(self.session.execute(q_edges).all())

    # pylint: disable=comparison-with-callable
    def edges(self) -> Iterator[DateYield]:
        """Yields edges of collaboration network iteratively as they form in temporal order.

        Yields:
            DateYield: All edges formed on the current point in time.
                If there are multiple projects released at a specific point in time,
                all edges forming with regards to these projects are yielded at once.
        """
        date_curr = None # Current block defined by datetime
        proj_curr = None

        if self._l_collaborations is None:
            q_edges = self._create_sql_query()
            self._preprocess_collaboration(q_edges)

        d_collabs_curr = dict()

        edges_yield = defaultdict(int)
        collabs_yield = defaultdict(int)

        for date_row, proj_row, collab_row, collaboration_row in self._l_collaborations:
            if date_curr is None: # First iteration
                date_curr = date_row
                proj_curr = proj_row

            # Detect new project block
            if proj_row != proj_curr:
                edges_yield[proj_curr] = d_collabs_curr
                d_collabs_curr: Dict[int, int] = dict()
                proj_curr = proj_row

            # Detect new date block
            if date_curr != date_row:
                yield DateYield(
                    timestamp=date_curr,
                    collaborators={
                        idx_collab:
                            CollaboratorYield(
                                self.map_collaborator_gender[idx_collab],
                                cnt_proj)\
                                    for idx_collab, cnt_proj in collabs_yield.items()},
                    collaborations=edges_yield
                )

                # Reset date block structures
                date_curr = date_row
                edges_yield = defaultdict(int)
                collabs_yield = defaultdict(int)

            d_collabs_curr[collab_row] = collaboration_row
            collabs_yield[collab_row] += 1

        # Yield final date block
        edges_yield[proj_curr] = d_collabs_curr
        yield DateYield(
            timestamp=date_curr,
            collaborators={
                idx_collab:\
                    CollaboratorYield(
                        self.map_collaborator_gender[idx_collab],
                        cnt_proj)\
                            for idx_collab, cnt_proj in collabs_yield.items()},
            collaborations=edges_yield
        )

    def _init_map_collaborator_gender(self):
        stmt = (select(
            Collaborator.id,
            Collaborator.id_gender
        ))
        for id_collab, id_gender in self.session.execute(stmt):
            self.map_collaborator_gender[id_collab] = id_gender
