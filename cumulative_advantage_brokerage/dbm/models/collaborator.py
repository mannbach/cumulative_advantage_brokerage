"""APS Collaborator realization
"""
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship, declared_attr

from .base_mixin import Base
from .collaborator_name import CollaboratorName

# pylint: disable=no-self-argument
class Collaborator(Base):
    """Base class that defines APS-specific characteristics and relationships.

    This class incorporates all present authors, including those
    that were disambiguated and those who were not.
    The following sub-classes are defined to automatically select from the desired set of authors.
    This additional step avoids errors due to forgetting to exclude
    non-disambiguated authors and aligns the `Collaborator`-class
    to the abstract scenario.
    That is, queries including `Collaborator` will automatically
    be limited to disambiguated authors.
    `NonDisambiguatedCollaborator` enables explicit access to authors who were not disambiguated.
    """
    __tablename__ = "collaborator"

    @declared_attr
    def id_gender(cls):
        """Connect to gender.
        Declared attribute to evaluate at time of initialization.
        """
        return Column(Integer, ForeignKey("gender.id"))

    @declared_attr
    def gender(cls):
        """Define gender relationship.
        """
        # TODO: add primaryjoin=lambda: Target.id==cls.target_id?
        return relationship("Gender")

    # Relationship to names (due to disambiguation)
    names = relationship(
        CollaboratorName, back_populates="collaborator")
