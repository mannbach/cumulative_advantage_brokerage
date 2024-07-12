from sqlalchemy import Column, Index, Integer, Float
from sqlalchemy.orm import declared_attr

from .metric_mixin import MetricMixin

class BinsRealization(MetricMixin):
    __tablename__ = "bins_realization"

    @declared_attr
    def position(cls):
        return Column(Integer, nullable=False)

    @declared_attr
    def value(cls):
        return Column(Float, nullable=False)

    __table_args__ = (
        Index(
            "idx_bins_realization",
            "id_metric_configuration",
            "position",
            unique=True
        ),
    )
