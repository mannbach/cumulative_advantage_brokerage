from typing import List
from sqlalchemy import select

from ..dbm import\
    ImpactGroup, CumAdvBrokSession, HasSession
from .binner import PercentileBinner

class ImpactGroupsInference(HasSession):
    """Infers collaborators' impact groups based on the binned metric values.
    """
    binner: PercentileBinner
    id_metric_configuration: int

    def __init__(
            self, *arg,
            session: CumAdvBrokSession,
            binner: PercentileBinner,
            id_metric_configuration: int, **kwargs) -> None:
        """Infers collaborators' impact groups based on the binned metric values.

        Parameters
        ----------
        session : CumAdvBrokSession
            Connection to the database.
        binner : PercentileBinner
            Binner that specifies the binning of the metric values.
        id_metric_configuration : int
            ID of the `MetricConfiguration`-object to reference the results in the database.
        """
        super().__init__(*arg, session=session, **kwargs)
        self.binner = binner
        self.id_metric_configuration = id_metric_configuration

    def compute_impact_groups(self) -> List[ImpactGroup]:
        """Computes the impact groups for all collaborators.

        Returns
        -------
        List[ImpactGroup]
            List of all collaborators with their respective impact group.
        """
        # Make sure that binning borders are computed
        assert self.binner.a_bin_values is not None, "Binning borders not computed."

        # Get impact group for all filtered collaborators
        q_final_vals = self.binner.create_query_max_value().subquery()
        q_vals = select(
            q_final_vals.c.id_collaborator,
            self.binner.bin_metric(q_final_vals.c.metric)
        )

        # Store results
        l_impact_groups = []
        for id_collaborator, q_m in self.session.execute(q_vals):
            l_impact_groups.append(
                ImpactGroup(value=q_m,
                            id_collaborator=id_collaborator,
                            id_metric_configuration=self.id_metric_configuration))
        self.session.commit_list(l=l_impact_groups)
        return l_impact_groups
