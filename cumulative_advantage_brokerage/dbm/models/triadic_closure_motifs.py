from sqlalchemy import Column, ForeignKey, Integer, Interval, String, Index
from sqlalchemy.orm import declarative_mixin, declared_attr

from .base_mixin import Base

# pylint: disable=no-self-argument
@declarative_mixin
class BaseTriadicClosureMotif(Base):
    _motif_type = "instant"
    __tablename__ = "triadic_closure_motif"

    @declared_attr
    def id_collaborator_a(cls):
        return Column(Integer, ForeignKey("collaborator.id"), nullable=False)

    @declared_attr
    def id_collaborator_b(cls):
        return Column(Integer, ForeignKey("collaborator.id"), nullable=False)

    @declared_attr
    def id_collaborator_c(cls):
        return Column(Integer, ForeignKey("collaborator.id"), nullable=False)

    @declared_attr
    def id_project_ab(cls):
        return Column(Integer, ForeignKey("project.id"), nullable=False)

    @declared_attr
    def id_project_bc(cls):
        return Column(Integer, ForeignKey("project.id"), nullable=False)

    @declared_attr
    def id_project_ac(cls):
        return Column(Integer, ForeignKey("project.id"), nullable=False)

    @declared_attr
    def motif_type(cls):
        return Column(String, nullable=False, default=cls._motif_type)

    __mapper_args__ = {
        "polymorphic_on": "motif_type",
        "polymorphic_identity": "instant",
    }
    __table_args__ = (
        Index(
            "idx_collaborator_motif_triplet",
            "id_collaborator_a",
            "id_collaborator_b",
            "id_collaborator_c",
            unique=True
        ),
    )

class SimplicialBaseTriadicClosureMotif(BaseTriadicClosureMotif):
    _motif_type = "simplicial_instant"
    __tablename__ = "triadic_closure_motif"
    __mapper_args__ = {
        "polymorphic_identity": _motif_type,
    }
    __table_args__ = (
        {"extend_existing": True},
    )

class BrokerMotif(BaseTriadicClosureMotif):
    _motif_type = "broker"
    __tablename__ = "triadic_closure_motif"
    __mapper_args__ = {
        "polymorphic_identity": _motif_type,
    }
    __table_args__ = (
        {"extend_existing": True},
    )
    @declared_attr
    def dt_close(cls):
        return Column(Interval)

class SimplicialBrokerMotif(BrokerMotif):
    _motif_type = "simplicial_broker"
    __tablename__ = "triadic_closure_motif"
    __mapper_args__ = {
        "polymorphic_identity": _motif_type,
    }
    __table_args__ = (
        {"extend_existing": True},
    )


class InitiationLinkMotif(BaseTriadicClosureMotif):
    _motif_type = "initiation_link"
    __tablename__ = "triadic_closure_motif"
    __mapper_args__ = {
        "polymorphic_identity": _motif_type,
    }
    __table_args__ = (
        {"extend_existing": True},
    )
    @declared_attr
    def dt_open(cls):
        return Column(Interval)

class SimplicialInitiationLinkMotif(InitiationLinkMotif):
    _motif_type = "simplicial_initiation_link"
    __tablename__ = "triadic_closure_motif"
    __mapper_args__ = {
        "polymorphic_identity": _motif_type,
    }
    __table_args__ = (
        {"extend_existing": True},
    )
class TriadicClosureMotif(BaseTriadicClosureMotif):
    _motif_type = "triadic_closure"
    __tablename__ = "triadic_closure_motif"
    __mapper_args__ = {
        "polymorphic_identity": _motif_type,
    }
    __table_args__ = (
        {"extend_existing": True},
    )
    @declared_attr
    def dt_open(cls):
        return Column(Interval)

    @declared_attr
    def dt_close(cls):
        return Column(Interval)

class SimplicialTriadicClosureMotif(TriadicClosureMotif):
    _motif_type = "simplicial_triadic_closure"
    __tablename__ = "triadic_closure_motif"
    __mapper_args__ = {
        "polymorphic_identity": _motif_type,
    }
    __table_args__ = (
        {"extend_existing": True},
    )
