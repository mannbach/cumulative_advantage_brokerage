from sqlalchemy import Column, Index, ForeignKey, Integer
from sqlalchemy.orm import declared_attr

from .metric_mixin import MetricMixin

class ImpactGroup(MetricMixin):
    __tablename__ = "impact_group"

    value = Column(Integer, nullable=False)

    @declared_attr
    def id_collaborator(cls):
        """Connection to `collaborator.id`.
        """
        return Column("id_collaborator",
                      ForeignKey("collaborator.id"),
                      index=True, nullable=False)

    __table_args__ = (
        Index(
            "idx_impact_group",
            "id_collaborator",
            "id_metric_configuration",
            unique=True
        ),
    )
