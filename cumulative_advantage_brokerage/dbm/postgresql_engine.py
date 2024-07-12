"""Defines engine to connect to PostgreSQL DB."""
import configparser
from typing import Any, Dict

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from ..constants import\
    ARG_POSTGRES_DB_APS, ARG_POSTGRES_HOST,\
    ARG_POSTGRES_USER, ARG_POSTGRES_PASSWORD,\
    ARG_POSTGRES_PORT

# pylint: disable=abstract-method
class PostgreSQLEngine(Engine):
    """Engine to connect to PostgreSQL database."""
    @ classmethod
    def from_config_parser(cls, config: configparser.ConfigParser):
        """Instantiate from config parser.
        The expected format can be seen in `secrets/db_connection_test.ini`.

        Args:
            config (configparser.ConfigParser):
                Instantiation of config parsers from an existing `.ini`-file.
        """
        return create_engine(PostgreSQLEngine._create_connection_string(config), future=True)

    @classmethod
    def from_config(cls,
            config: Dict[str, Any],
            key_dbname: str = ARG_POSTGRES_DB_APS):
        """Instantiate from environment variables.
        Required keys are (default values):
            POSTGRES_USER ("postgres")
            POSTGRES_PASSWORD ("postgres")
            POSTGRES_HOST ("localhost")
            POSTGRES_PORT (5432)
            POSTGRES_DBNAME ("collaboration_test")
        """
        config_local = config.copy()
        config_local["dbname"] = config[key_dbname]
        return create_engine(PostgreSQLEngine._create_connection_string(config_local), future=True)

    @staticmethod
    def _create_connection_string(config: Dict[str, str]) -> str:
        """Create connection string from configuration dict.

        Args:
            config (Dict[str, str]): Dictionary with expected keys
                user, password, host, port, dbname

        Returns:
            str: The connect string of the format
                "postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}
        """
        return (f"postgresql+psycopg2://{config[ARG_POSTGRES_USER]}:"
                f"{config[ARG_POSTGRES_PASSWORD]}@{config[ARG_POSTGRES_HOST]}:"
                f"{config[ARG_POSTGRES_PORT]}/{config['dbname']}")
