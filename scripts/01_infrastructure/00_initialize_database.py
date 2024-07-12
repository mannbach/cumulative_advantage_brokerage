"""Create database tables as defined by the model.
"""
from sqlalchemy import text

from cumulative_advantage_brokerage.config import parse_config
from cumulative_advantage_brokerage.constants import\
    ARG_POSTGRES_DB_APS, ARG_POSTGRES_HOST, ARG_POSTGRES_USER, ARG_POSTGRES_DB
from cumulative_advantage_brokerage.dbm import PostgreSQLEngine

def main():
    """Create tables."""
    config = parse_config([ARG_POSTGRES_DB_APS])
    engine_test = PostgreSQLEngine.from_config(config, key_dbname=ARG_POSTGRES_DB)

    print(f"Connecting to {config[ARG_POSTGRES_HOST]}.")
    with engine_test.connect() as conn:
        print(f"Creating database {config[ARG_POSTGRES_DB_APS]}.")
        conn.execute(text("COMMIT"))
        conn.execute(text(
            f"CREATE DATABASE {config[ARG_POSTGRES_DB_APS]} "
            f"OWNER {config[ARG_POSTGRES_USER]};"))

if __name__ == "__main__":
    main()
