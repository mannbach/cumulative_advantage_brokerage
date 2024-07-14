import warnings
from collections import defaultdict
from typing import Dict, Optional

from sqlalchemy import select, and_
import numpy as np
import pandas as pd

from .session import CumAdvBrokSession
from .models.metric_mixin import MetricConfiguration
from .models.bins_realization import BinsRealization
from .models.metric_cs_bf_comparison import\
    MetricCollaboratorSeriesBrokerageFrequencyComparison
from .models.metric_cs_br_comparison import\
    MetricCollaboratorSeriesBrokerageRateComparison
from .models.collaborator_series import CollaboratorSeriesBrokerage
from .models.gender import Gender
from .models.collaborator import Collaborator

def select_latest_metric_config_id_by_args(metric_args: Dict[str, str]) -> int:
    return select(MetricConfiguration.id)\
        .where(
            and_(
                *(MetricConfiguration.args[k].as_string() == v\
                    for k,v in metric_args.items)))\
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
    assert id_metric is not None, "No career series ID found."
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
        stage: Optional[int] = None,

        filter_max_stage: bool = True) -> pd.DataFrame:
    q_cs_broker_freq = select(
            CollaboratorSeriesBrokerage.id,
            CollaboratorSeriesBrokerage.id_collaborator,
            Gender.gender,
            BinsRealization.position,
            CollaboratorSeriesBrokerage.motif_type,
            CollaboratorSeriesBrokerage.role,
            CollaboratorSeriesBrokerage.value)\
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
    d_brokerage_freq.name = metric

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
        d_brokerage_freq.name = metric

    return d_brokerage_freq
