from sqlalchemy import select
import numpy as np

from .models.metric_mixin import MetricConfiguration
from .models.bins_realization import BinsRealization

def select_latest_metric_config_id_by_metric(metric: str) -> int:
    return select(MetricConfiguration.id)\
        .where(MetricConfiguration.args["metric"].as_string() == metric)\
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
