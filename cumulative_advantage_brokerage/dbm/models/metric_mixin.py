"""Definition of Project model.
"""
from sqlalchemy import Column, DateTime, func, Integer, ForeignKey, JSON
from sqlalchemy.orm import declarative_mixin, declared_attr

from .base_mixin import Base

class MetricConfiguration(Base):
    __tablename__ = "metric_configuration"

    computed_at = Column(DateTime, server_default=func.now())
    args = Column(JSON)

# pylint: disable=no-self-argument
@declarative_mixin
class MetricMixin(Base):
    __abstract__ = True
    @declared_attr
    def id_metric_configuration(cls):
        return Column(
            Integer,
            ForeignKey("metric_configuration.id"),
            nullable=True)
