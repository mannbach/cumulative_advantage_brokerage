"""Script to execute APS data integration.
"""
from cumulative_advantage_brokerage.dbm import\
    PostgreSQLEngine, APSIntegrator
from cumulative_advantage_brokerage.config import parse_config
from cumulative_advantage_brokerage.constants import\
    ARG_TRANSF_APS_CSV_FOLDER,\
    FILE_NAME_CSV_GENDER,\
    FILE_NAME_CSV_AUTHORS,\
    FILE_NAME_CSV_AUTHOR_NAMES,\
    FILE_NAME_CSV_AUTHORSHIPS,\
    FILE_NAME_CSV_PUBLICATIONS,\
    FILE_NAME_CSV_CITATIONS,\
    ARG_TRANSF_APS_FILE_LOG, ARG_POSTGRES_DB_APS

def main():
    """Performs integration.
    1. Load command line arguments
    2. Load test or production configuration
    3. Setup integrator with config
    4. Execute integration
    5. Add data to database
    """
    config = parse_config([
        ARG_POSTGRES_DB_APS,
        ARG_TRANSF_APS_CSV_FOLDER,
        ARG_TRANSF_APS_FILE_LOG])

    engine = PostgreSQLEngine.from_config(
        config, key_dbname=ARG_POSTGRES_DB_APS)

    integrator = APSIntegrator(
        engine=engine,
        folder_csv=config[ARG_TRANSF_APS_CSV_FOLDER],
        file_gender=FILE_NAME_CSV_GENDER,
        file_authors=FILE_NAME_CSV_AUTHORS,
        file_author_names=FILE_NAME_CSV_AUTHOR_NAMES,
        file_authorships=FILE_NAME_CSV_AUTHORSHIPS,
        file_publications=FILE_NAME_CSV_PUBLICATIONS,
        file_citations=FILE_NAME_CSV_CITATIONS,
        path_log=config[ARG_TRANSF_APS_FILE_LOG]
    )
    integrator.populate_database()

if __name__ == "__main__":
    main()
