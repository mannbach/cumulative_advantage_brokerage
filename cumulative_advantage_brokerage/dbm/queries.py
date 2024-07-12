from sqlalchemy import select

from .models.metric_mixin import MetricConfiguration

def select_latest_metric_config_id_by_metric(metric: str) -> int:
    return select(MetricConfiguration.id)\
        .where(MetricConfiguration.args["metric"] == metric)\
        .order_by(MetricConfiguration.computed_at.desc())\
        .limit(1)

def get_single_result(session, query):
    return session.execute(query).scalar()
