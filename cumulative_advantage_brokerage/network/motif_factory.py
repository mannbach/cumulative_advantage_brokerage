from typing import Tuple
from datetime import timedelta

from ..dbm import\
    HasSession,\
    TriadicClosureMotif, SimplicialTriadicClosureMotif,\
    BrokerMotif, SimplicialBrokerMotif,\
    InitiationLinkMotif, SimplicialInitiationLinkMotif,\
    BaseTriadicClosureMotif, SimplicialBaseTriadicClosureMotif

class MotifFactory(HasSession):
    def identify_motif_type(
        self,
        tpl_collaborators: Tuple[int, int, int],
        tpl_projects: Tuple[int, int, int],
        dt_open: timedelta,
        dt_close: timedelta,
        is_simplicial: bool
    ) -> BaseTriadicClosureMotif:
        # Check for temporal distance
        if dt_open.days > 0 and dt_close.days > 0:
            return TriadicClosureMotif(
                id_collaborator_a=tpl_collaborators[0],
                id_collaborator_b=tpl_collaborators[1],
                id_collaborator_c=tpl_collaborators[2],
                id_project_ab=tpl_projects[0],
                id_project_bc=tpl_projects[1],
                id_project_ac=tpl_projects[2],
                dt_open=dt_open,
                dt_close=dt_close
            ) if not is_simplicial else SimplicialTriadicClosureMotif(
                id_collaborator_a=tpl_collaborators[0],
                id_collaborator_b=tpl_collaborators[1],
                id_collaborator_c=tpl_collaborators[2],
                id_project_ab=tpl_projects[0],
                id_project_bc=tpl_projects[1],
                id_project_ac=tpl_projects[2],
                dt_open=dt_open,
                dt_close=dt_close
            )
        if dt_close.days > 0:
            _pos_a, _pos_c = sorted([0,2], key=tpl_collaborators.__getitem__)

            return BrokerMotif(
                id_collaborator_a=tpl_collaborators[_pos_a],
                id_collaborator_b=tpl_collaborators[1],
                id_collaborator_c=tpl_collaborators[_pos_c],
                id_project_ab=tpl_projects[_pos_a],
                id_project_bc=tpl_projects[1],
                id_project_ac=tpl_projects[_pos_c],
                dt_close=dt_close
            ) if not is_simplicial else SimplicialBrokerMotif(
                id_collaborator_a=tpl_collaborators[_pos_a],
                id_collaborator_b=tpl_collaborators[1],
                id_collaborator_c=tpl_collaborators[_pos_c],
                id_project_ab=tpl_projects[_pos_a],
                id_project_bc=tpl_projects[1],
                id_project_ac=tpl_projects[_pos_c],
                dt_close=dt_close
            )
        if dt_open.days > 0:
            _pos_a, _pos_b = sorted([0,1], key=tpl_collaborators.__getitem__)
            return InitiationLinkMotif(
                id_collaborator_a=tpl_collaborators[_pos_a],
                id_collaborator_b=tpl_collaborators[_pos_b],
                id_collaborator_c=tpl_collaborators[2],
                id_project_ab=tpl_projects[_pos_a],
                id_project_bc=tpl_projects[_pos_b],
                id_project_ac=tpl_projects[2],
                dt_open=dt_open
            ) if not is_simplicial else SimplicialInitiationLinkMotif(
                id_collaborator_a=tpl_collaborators[_pos_a],
                id_collaborator_b=tpl_collaborators[_pos_b],
                id_collaborator_c=tpl_collaborators[2],
                id_project_ab=tpl_projects[_pos_a],
                id_project_bc=tpl_projects[_pos_b],
                id_project_ac=tpl_projects[2],
                dt_open=dt_open
            )
        # else:
        _pos_a, _pos_b, _pos_c = sorted([0,1,2], key=tpl_collaborators.__getitem__)
        return BaseTriadicClosureMotif(
                id_collaborator_a=tpl_collaborators[_pos_a],
                id_collaborator_b=tpl_collaborators[_pos_b],
                id_collaborator_c=tpl_collaborators[_pos_c],
                id_project_ab=tpl_projects[_pos_a],
                id_project_bc=tpl_projects[_pos_b],
                id_project_ac=tpl_projects[_pos_c],
        ) if not is_simplicial else SimplicialBaseTriadicClosureMotif(
                id_collaborator_a=tpl_collaborators[_pos_a],
                id_collaborator_b=tpl_collaborators[_pos_b],
                id_collaborator_c=tpl_collaborators[_pos_c],
                id_project_ab=tpl_projects[_pos_a],
                id_project_bc=tpl_projects[_pos_b],
                id_project_ac=tpl_projects[_pos_c],
        )
