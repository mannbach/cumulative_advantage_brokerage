from cumulative_advantage_brokerage.config import parse_config
from cumulative_advantage_brokerage.constants import ARG_POSTGRES_DB_APS
from cumulative_advantage_brokerage.dbm import\
    PostgreSQLEngine, CumAdvBrokSession
from cumulative_advantage_brokerage.network import\
    SQLEdgeGenerator, GrowingTemporalLinkedListNetwork,\
    InitiationMotifCollector

def main():
    config = parse_config([ARG_POSTGRES_DB_APS])
    engine = PostgreSQLEngine.from_config(config, key_dbname=ARG_POSTGRES_DB_APS)

    with CumAdvBrokSession(engine) as session:
        print("Establishing session.")
        generator = SQLEdgeGenerator(session=session)
        print("Initiating network.")
        network = GrowingTemporalLinkedListNetwork(generator=generator)
        print("Initiating motif collector.")
        counter = InitiationMotifCollector(network=network, session=session)

        print("Starting motif count.")
        counter.integrate_counts()

if __name__ == "__main__":
    main()
