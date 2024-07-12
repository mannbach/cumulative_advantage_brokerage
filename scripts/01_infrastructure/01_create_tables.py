"""Create database tables as defined by the model.
"""
from sqlalchemy import inspect

from cumulative_advantage_brokerage.config import parse_config
from cumulative_advantage_brokerage.constants import\
    ARG_POSTGRES_DB_APS
from cumulative_advantage_brokerage.dbm import\
    PostgreSQLEngine,\
    Base, Collaboration, Gender,\
    Collaborator, Project,\
    CollaboratorName, Citation,\
    MetricConfiguration, BinsRealization,\
    MetricCollaboratorSeriesBrokerageFrequencyComparison,\
    MetricCollaboratorSeriesBrokerageRateComparison,\
    BaseTriadicClosureMotif, SimplicialBaseTriadicClosureMotif,\
    BrokerMotif, SimplicialBrokerMotif,\
    InitiationLinkMotif, SimplicialInitiationLinkMotif,\
    TriadicClosureMotif, SimplicialTriadicClosureMotif

def main():
    """Create tables."""
    config = parse_config([ARG_POSTGRES_DB_APS])

    tables_base = [
        Gender, CollaboratorName, Collaboration, Project,
        Collaborator, Citation,
        MetricConfiguration, BinsRealization,
        MetricCollaboratorSeriesBrokerageFrequencyComparison,
        MetricCollaboratorSeriesBrokerageRateComparison,
        BaseTriadicClosureMotif, SimplicialBaseTriadicClosureMotif,
        BrokerMotif, SimplicialBrokerMotif,
        InitiationLinkMotif, SimplicialInitiationLinkMotif,
        TriadicClosureMotif, SimplicialTriadicClosureMotif]
    print(f"Creating tables for {config[ARG_POSTGRES_DB_APS]}.")
    engine_test = PostgreSQLEngine.from_config(
        config, key_dbname=ARG_POSTGRES_DB_APS)
    for table in tables_base:
        if inspect(engine_test).has_table(table):
            table.__table__.drop(engine_test)
    Base.metadata.create_all(engine_test)

if __name__ == "__main__":
    main()
