import warnings
from collections import defaultdict
from typing import Dict

from sqlalchemy import select, and_, func, alias, or_
import numpy as np
import pandas as pd

from .dbm.session import CumAdvBrokSession
from .dbm.models.metric_mixin import MetricConfiguration
from .dbm.models.bins_realization import BinsRealization
from .dbm.models.metric_cs_bf_comparison import\
    MetricCollaboratorSeriesBrokerageFrequencyComparison
from .dbm.models.metric_cs_br_comparison import\
    MetricCollaboratorSeriesBrokerageRateComparison
from .dbm.models.collaborator_series import CollaboratorSeriesBrokerage
from .dbm.models.gender import Gender
from .dbm.models.collaborator import Collaborator
from .dbm.models.project import Project
from .dbm.models.collaboration import Collaboration
from .dbm.models.triadic_closure_motifs import\
    TriadicClosureMotif, SimplicialTriadicClosureMotif
from .career_series.collaborator_filter import StandardFilter

def select_latest_metric_config_id_by_args(metric_args: Dict[str, str]) -> int:
    return select(MetricConfiguration.id)\
        .where(
            and_(
                *(MetricConfiguration.args[k].as_string() == v\
                    for k,v in metric_args.items())))\
        .order_by(MetricConfiguration.computed_at.desc())\
        .limit(1)

def get_single_result(session, query):
    return session.execute(query).scalar()

def get_bin_values_by_id(session, id_config: int) -> np.ndarray:
    q_bins = select(BinsRealization.value)\
        .where(BinsRealization.id_metric_configuration == id_config)\
        .order_by(BinsRealization.position)

    _l_bins = [r[0] for r in session.execute(q_bins).fetchall()]
    return np.asarray(_l_bins)

def get_bf_comparison_results_by_id(
          id_metric_config: int, metric: str, session: CumAdvBrokSession) -> pd.DataFrame:
    q = select(
        MetricCollaboratorSeriesBrokerageFrequencyComparison.id,
        MetricCollaboratorSeriesBrokerageFrequencyComparison.stage,
        MetricCollaboratorSeriesBrokerageFrequencyComparison.max_stage_curr,
        MetricCollaboratorSeriesBrokerageFrequencyComparison.max_stage_next,
        MetricCollaboratorSeriesBrokerageFrequencyComparison.grouping_key,
        MetricCollaboratorSeriesBrokerageFrequencyComparison.test_statistic,
        MetricCollaboratorSeriesBrokerageFrequencyComparison.ci_low,
        MetricCollaboratorSeriesBrokerageFrequencyComparison.ci_high,
        MetricCollaboratorSeriesBrokerageFrequencyComparison.p_value,
        MetricCollaboratorSeriesBrokerageFrequencyComparison.n_x,
        MetricCollaboratorSeriesBrokerageFrequencyComparison.n_y,
        )\
    .select_from(MetricCollaboratorSeriesBrokerageFrequencyComparison)\
    .order_by(
        MetricCollaboratorSeriesBrokerageFrequencyComparison.stage,
        MetricCollaboratorSeriesBrokerageFrequencyComparison.max_stage_curr,
        MetricCollaboratorSeriesBrokerageFrequencyComparison.max_stage_next,
        MetricCollaboratorSeriesBrokerageFrequencyComparison.grouping_key)\
    .where(
        MetricCollaboratorSeriesBrokerageFrequencyComparison.id_metric_configuration == id_metric_config)

    df_tests = defaultdict(list)
    for (idx, stage, max_stage_curr, max_stage_next,
            grouping_key, test_statistic, ci_low, ci_high,
            p_value, nx, ny) in session.execute(q):
        df_tests["id"].append(idx)
        df_tests["stage"].append(stage)
        df_tests["max_stage_curr"].append(max_stage_curr)
        df_tests["max_stage_next"].append(max_stage_next)
        df_tests["grouping_key"].append(grouping_key)
        df_tests["test_statistic"].append(test_statistic)
        df_tests["ci_low"].append(ci_low)
        df_tests["ci_high"].append(ci_high)
        df_tests["p_value"].append(p_value)
        df_tests["n_x"].append(nx)
        df_tests["n_y"].append(ny)
    df_tests = pd.DataFrame(df_tests)
    df_tests = df_tests.set_index("id")
    df_tests.name = metric
    return df_tests

def get_br_comparison_results_by_id(
            id_metric_config: int, metric: str, session) -> pd.DataFrame:
    q = select(
            MetricCollaboratorSeriesBrokerageRateComparison.id,
            MetricCollaboratorSeriesBrokerageRateComparison.stage_curr,
            MetricCollaboratorSeriesBrokerageRateComparison.stage_next,
            MetricCollaboratorSeriesBrokerageRateComparison.stage_max,
            MetricCollaboratorSeriesBrokerageRateComparison.grouping_key,
            MetricCollaboratorSeriesBrokerageRateComparison.test_statistic,
            MetricCollaboratorSeriesBrokerageRateComparison.ci_low,
            MetricCollaboratorSeriesBrokerageRateComparison.ci_high,
            MetricCollaboratorSeriesBrokerageRateComparison.p_value,
            MetricCollaboratorSeriesBrokerageRateComparison.n_x,
            MetricCollaboratorSeriesBrokerageRateComparison.n_y,
            )\
        .select_from(MetricCollaboratorSeriesBrokerageRateComparison)\
        .order_by(
            MetricCollaboratorSeriesBrokerageRateComparison.stage_curr,
            MetricCollaboratorSeriesBrokerageRateComparison.stage_next,
            MetricCollaboratorSeriesBrokerageRateComparison.stage_max,
            MetricCollaboratorSeriesBrokerageRateComparison.grouping_key)\
        .where(
            MetricCollaboratorSeriesBrokerageRateComparison.id_metric_configuration == id_metric_config)

    df_tests = defaultdict(list)
    for (idx, stage_curr, stage_next, stage_max,
            grouping_key, test_statistic, ci_low,
            ci_high, p_value, nx, ny) in session.execute(q):
        df_tests["id"].append(idx)
        df_tests["stage_curr"].append(stage_curr)
        df_tests["stage_next"].append(stage_next)
        df_tests["stage_max"].append(stage_max)
        df_tests["grouping_key"].append(grouping_key)
        df_tests["test_statistic"].append(test_statistic)
        df_tests["ci_low"].append(ci_low)
        df_tests["ci_high"].append(ci_high)
        df_tests["p_value"].append(p_value)
        df_tests["n_x"].append(nx)
        df_tests["n_y"].append(ny)
    df_tests = pd.DataFrame(df_tests)
    df_tests = df_tests.set_index("id")
    df_tests.name = metric
    return df_tests

def init_metric_id(metric_args: Dict[str, str], session)\
    -> int:
    id_metric = get_single_result(
        session=session,
        query=select_latest_metric_config_id_by_args(metric_args))
    assert id_metric is not None, f"No metric ID found with args\n{metric_args}."
    warnings.warn(
        (f"No metric ID provided for {metric_args}. "
        "Using latest metric. "
        "This only works if the last computation of "
        "the respective binning was successful!\n"
        f"Found ID '{id_metric}'."))
    return id_metric

def get_brokerage_freq_by_id(
        id_metric_config: int,
        session: CumAdvBrokSession,
        filter_max_stage: bool = True) -> pd.DataFrame:
    q_cs_broker_freq = select(
            CollaboratorSeriesBrokerage.id,
            CollaboratorSeriesBrokerage.id_collaborator,
            Gender.gender,
            BinsRealization.position,
            CollaboratorSeriesBrokerage.motif_type,
            CollaboratorSeriesBrokerage.role,
            CollaboratorSeriesBrokerage.value)\
        .select_from(CollaboratorSeriesBrokerage)\
        .join(
            BinsRealization,
            BinsRealization.id == CollaboratorSeriesBrokerage.id_bin)\
        .join(
            Collaborator,
            Collaborator.id == CollaboratorSeriesBrokerage.id_collaborator
        )\
        .join(
            Gender,
            Gender.id == Collaborator.id_gender
        )\
        .where(
            and_(
                CollaboratorSeriesBrokerage.id_metric_configuration == id_metric_config
            ))
    d_brokerage_freq = defaultdict(list)
    for id_cs, id_c, gender, pos, mt, ro, val in session.execute(q_cs_broker_freq):
        d_brokerage_freq["id"].append(id_cs)
        d_brokerage_freq["id_collaborator"].append(id_c)
        d_brokerage_freq["gender"].append(gender)
        d_brokerage_freq["stage"].append(pos)
        d_brokerage_freq["motif_type"].append(mt)
        d_brokerage_freq["role"].append(ro)
        d_brokerage_freq["value"].append(val)
    d_brokerage_freq = pd.DataFrame(d_brokerage_freq).set_index("id")

    # Add a dummy group to everyone
    d_brokerage_freq["g_dummy"] = "0"

    # Remove last (incomplete) stages
    if filter_max_stage:
        # Assign max stage
        # TODO: Add this to query directly
        gs_idc_stage_max = d_brokerage_freq\
            .groupby("id_collaborator")\
            ["stage"]\
            .max()\
            .rename("stage_max")
        _sm = d_brokerage_freq\
            .join(gs_idc_stage_max, on="id_collaborator")\
            ["stage_max"]
        d_brokerage_freq = d_brokerage_freq[d_brokerage_freq["stage"] < _sm]

    return d_brokerage_freq

def get_auth_info(session, filtered: bool = True) -> pd.DataFrame:
    sq_collaborator_id = select(Collaborator.id.label("id_collaborator"))\
        if not filtered else\
            StandardFilter()\
                .create_collaborator_source_subquery()
    sq_birth_death = select(Collaboration.id_collaborator,
                func.min(Project.timestamp).label("birth"),\
                func.max(Project.timestamp).label("death"))\
        .select_from(Collaboration)\
        .join(Project, Project.id == Collaboration.id_project)\
        .join(Collaborator, Collaborator.id == Collaboration.id_collaborator)\
        .group_by(Collaboration.id_collaborator)\
        .subquery()
    q = select(
        sq_collaborator_id.c.id_collaborator,
        sq_birth_death.c.birth,
        sq_birth_death.c.death,
        Gender.gender)\
        .select_from(sq_collaborator_id)\
        .join(sq_birth_death, sq_collaborator_id.c.id_collaborator == sq_birth_death.c.id_collaborator)\
        .join(Collaborator, Collaborator.id == sq_collaborator_id.c.id_collaborator)\
        .join(Gender, Gender.id == Collaborator.id_gender)
    d_coll = defaultdict(list)
    for idx, birth, death, gender in session.execute(q):
        d_coll["id"].append(idx)
        d_coll["birth"].append(birth)
        d_coll["death"].append(death)
        d_coll["gender"].append(gender)
    d_coll = pd.DataFrame(d_coll)
    d_coll = d_coll.set_index("id")
    return d_coll

def get_brokerage_events(
        session,
        join_projects: bool = False) -> pd.DataFrame:
    a,b,c = tuple(alias(Collaborator) for _ in range(3))
    g_a, g_b, g_c = tuple(alias(Gender) for _ in range(3))
    q = select(
            TriadicClosureMotif.id,
            TriadicClosureMotif.id_collaborator_a,
            g_a.c.gender,
            TriadicClosureMotif.id_collaborator_b,
            g_b.c.gender,
            TriadicClosureMotif.id_collaborator_c,
            g_c.c.gender,
            TriadicClosureMotif.id_project_ab,
            TriadicClosureMotif.id_project_bc,
            TriadicClosureMotif.id_project_ac,
            TriadicClosureMotif.motif_type)\
        .select_from(TriadicClosureMotif)\
        .join(a, a.c.id == TriadicClosureMotif.id_collaborator_a)\
        .join(g_a, g_a.c.id == a.c.id_gender)\
        .join(b, b.c.id == TriadicClosureMotif.id_collaborator_b)\
        .join(g_b, g_b.c.id == b.c.id_gender)\
        .join(c, c.c.id == TriadicClosureMotif.id_collaborator_c)\
        .join(g_c, g_c.c.id == c.c.id_gender)\
        .where(or_(
            TriadicClosureMotif.motif_type == TriadicClosureMotif._motif_type,
            TriadicClosureMotif.motif_type == SimplicialTriadicClosureMotif._motif_type))
    if join_projects:
        p_ab, p_bc, p_ac = tuple(alias(Project) for _ in range(3))
        sq = q.subquery()
        q = select(
                sq,
                p_ab.c.timestamp,
                p_bc.c.timestamp,
                p_ac.c.timestamp)\
            .select_from(sq)\
            .join(p_ab, p_ab.c.id == sq.c.id_project_ab)\
            .join(p_bc, p_bc.c.id == sq.c.id_project_bc)\
            .join(p_ac, p_ac.c.id == sq.c.id_project_ac)

    df_brok = defaultdict(list)
    for t_res in session.execute(q):
        if join_projects:
            idx, id_collaborator_a, g_a, id_collaborator_b, g_b, id_collaborator_c, g_c, id_project_ab, id_project_bc, id_project_ac, motif_type, t_ab, t_bc, t_ac = t_res
        else:
            idx, id_collaborator_a, g_a, id_collaborator_b, g_b, id_collaborator_c, g_c, id_project_ab, id_project_bc, id_project_ac, motif_type = t_res

        df_brok["id"].append(idx)
        df_brok["id_collaborator_a"].append(id_collaborator_a)
        df_brok["gender_a"].append(g_a)
        df_brok["id_collaborator_b"].append(id_collaborator_b)
        df_brok["gender_b"].append(g_b)
        df_brok["id_collaborator_c"].append(id_collaborator_c)
        df_brok["gender_c"].append(g_c)
        df_brok["id_project_ab"].append(id_project_ab)
        df_brok["id_project_bc"].append(id_project_bc)
        df_brok["id_project_ac"].append(id_project_ac)
        df_brok["motif_type"].append(motif_type)
        if join_projects:
            df_brok["t_ab"].append(t_ab)
            df_brok["t_bc"].append(t_bc)
            df_brok["t_ac"].append(t_ac)
    df_brok = pd.DataFrame(df_brok)
    df_brok = df_brok.set_index("id")
    return df_brok
