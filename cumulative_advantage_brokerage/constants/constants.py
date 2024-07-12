from datetime import timedelta, datetime

ID_GENDER_UNKNOWN = 0
ID_GENDER_FEMALE = 1
ID_GENDER_MALE = 2
MAP_GENDER_ID = {
    "unknown": ID_GENDER_UNKNOWN,
    "female": ID_GENDER_FEMALE,
    "male": ID_GENDER_MALE,
}
MAP_GENDERNQ_ID = {
    "-": ID_GENDER_UNKNOWN,
    "gf": ID_GENDER_FEMALE,
    "gm": ID_GENDER_MALE
}

# Preprocessing
FILE_NAME_CSV_GENDER = "genders.csv"
FILE_NAME_CSV_AUTHORS = "authors.csv"
FILE_NAME_CSV_AUTHOR_NAMES = "author_names.csv"
FILE_NAME_CSV_AUTHORSHIPS = "authorships.csv"
FILE_NAME_CSV_PUBLICATIONS = "publications.csv"
FILE_NAME_CSV_CITATIONS = "citations.csv"

# Network generation
CN_EVENT_NODE_ADD_BEFORE = "CN_EVENT_NODE_ADD_BEFORE"
CN_EVENT_NODE_ADD_AFTER = "CN_EVENT_NODE_ADD_AFTER"
CN_EVENT_LINK_ADD_BEFORE = "CN_EVENT_LINK_ADD_BEFORE"
CN_EVENT_LINK_ADD_AFTER = "CN_EVENT_LINK_ADD_AFTER"
CN_EVENT_PROJECT_ADD_BEFORE = "CN_EVENT_PROJECT_ADD_BEFORE"
CN_EVENT_PROJECT_ADD_AFTER = "CN_EVENT_PROJECT_ADD_AFTER"
CN_EVENT_DATE_ADD_BEFORE = "CN_EVENT_DATE_ADD_BEFORE"
CN_EVENT_DATE_ADD_AFTER = "CN_EVENT_DATE_ADD_AFTER"

# Career series
STR_CAREER_LENGTH = "career_length"
STR_CITATIONS = "citations"
STR_PRODUCTIVITY = "productivity"
TPL_STR_IMPACT = (STR_CITATIONS, STR_PRODUCTIVITY)
CAREER_LENGTH_MAX = timedelta(days=365*40)
DURATION_BUFFER_AUTHOR_ACTIVE = timedelta(days=365*4)
DATE_OBSERVATION_END = datetime(year=2020, month=12, day=31)
CS_BINS_PERCENTILES = [0.0, 0.5, 0.7, 0.85, 0.95, 1.0]
N_STAGES = len(CS_BINS_PERCENTILES) - 1

# Comparisons
N_RESAMPLES_DEFAULT = 5000

# ARG NAMES
ARG_PATH_CONTAINER_DATA = "PATH_CONTAINER_DATA"
ARG_PATH_CONTAINER_OUTPUT = "PATH_CONTAINER_OUTPUT"
ARG_SEED = "SEED"
ARG_NOMQUAM_THRESHOLD = "GI_NOMQUAM_THRESHOLD"

# Postgres
ARG_POSTGRES_HOST = "POSTGRES_HOST"
ARG_POSTGRES_USER = "POSTGRES_USER"
ARG_POSTGRES_PASSWORD = "POSTGRES_PASSWORD"
ARG_POSTGRES_PORT = "POSTGRES_PORT"
ARG_POSTGRES_DB = "POSTGRES_DB"
ARG_POSTGRES_DB_APS = "POSTGRES_DB_APS"

# Data transformation
ARG_TRANSF_APS_FOLDER_METADATA = "TRANSF_APS_FOLDER_METADATA"
ARG_TRANSF_APS_CSV_FILE_CITATIONS = "TRANSF_APS_CSV_FILE_CITATIONS"
ARG_TRANSF_APS_CSV_FILE_DISAMB = "TRANSF_APS_CSV_FILE_DISAMB"
ARG_TRANSF_APS_FILE_LOG = "TRANSF_APS_FILE_LOG"
ARG_TRANSF_APS_CSV_FOLDER = "TRANSF_APS_CSV_FOLDER"
