from typing import List
from sqlalchemy import select, case, Column

import numpy as np

from ..dbm import\
    ImpactGroup, CumAdvBrokSession, HasSession,\
    Collaboration, Project, Citation
from .collaborator_filter import CollaboratorFilter, StandardFilter
from .binner import PercentileBinner

class ImpactGroupsInference(HasSession):
    binner: PercentileBinner
    id_metric_configuration: int

    def __init__(
            self, *arg,
            session: CumAdvBrokSession,
            binner: PercentileBinner,
            id_metric_configuration: int, **kwargs) -> None:
        super().__init__(*arg, session=session, **kwargs)
        self.binner = binner
        self.id_metric_configuration = id_metric_configuration

    def compute_impact_groups(self) -> List[ImpactGroup]:
        assert self.binner.a_bin_values is not None, "Binning borders not computed."

        q_final_vals = self.binner.create_query_max_value().subquery()
        q_vals = select(
            q_final_vals.c.id_collaborator,
            self._bin_metric(q_final_vals.c.metric)
        )
        l_impact_groups = []
        for id_collaborator, q_m in self.session.execute(q_vals):
            l_impact_groups.append(
                ImpactGroup(value=q_m,
                            id_collaborator=id_collaborator,
                            id_metric_configuration=self.id_metric_configuration))
        self.session.commit_list(l=l_impact_groups)
        return l_impact_groups

    def _bin_metric(self, metric: Column) -> Column:
        assert self.binner.a_bin_values is not None, "Binning borders not computed."
        return case(
                    [(metric\
                          .between( # inclusive on both borders
                            float(self.binner.a_bin_values[i]),
                            float(self.binner.a_bin_values[i + 1])), i)
                        for i in range(len(self.binner.a_bin_values) - 1)
                    ],
                    else_=len(self.binner.a_bin_values) - 1).label("bin")
