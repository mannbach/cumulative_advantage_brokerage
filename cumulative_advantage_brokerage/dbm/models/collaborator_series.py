"""Definition of Collaborator series.
"""
from sqlalchemy import Column, ForeignKey, Index, String, Integer
from sqlalchemy.orm import declared_attr

from .metric_mixin import MetricMixin

# pylint: disable=no-self-argument
class CollaboratorSeriesBrokerage(MetricMixin):
    __tablename__ = "collaborator_series_brokerage"

    value = Column(Integer, nullable=False)

    @declared_attr
    def id_collaborator(cls):
        """Connection to `collaborator.id`.
        """
        return Column("id_collaborator", ForeignKey("collaborator.id"), index=True, nullable=False)

    @declared_attr
    def id_bin(cls):
        return Column("id_bin", ForeignKey("bins_realization.id"), index=True)

    @declared_attr
    def role(cls):
        return Column(String, nullable=False)

    @declared_attr
    def motif_type(cls):
        return Column(String, nullable=False)

    __table_args__ = (
        Index(
            "idx_cs_brokerage",
            "id_collaborator",
            "id_bin",
            "role",
            "motif_type",
            unique=True
        ),
    )
