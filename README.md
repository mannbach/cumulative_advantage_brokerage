# Cumulative advantage of brokerage in academia
This repository will contain the code to reproduce the results of the "Cumulative advantage of brokerage in academia" paper. A summary of the project structure is provided below (see [Project structure](#project-structure)).

## Requirements
### Software
To make this code independent of the underlying OS and software dependencies, we make us of docker compose (`>= v.2.12.2`, see [official docs](https://docs.docker.com/compose/) for installation steps).
Docker takes care of installing required software packages, and the setup of the local code and the [PostgreSQL database](https://www.postgresql.org/).

### Data
The successful reproduction of the results currently requires two datasets:
1. At its current stage, the original dataset up until January, 31st 2020 as provided by the American Physical Society (APS) is required (both citations and metadata).
As sharing these files is not permitted, a request must be filed [here](https://journals.aps.org/datasets).
Pending on an agreement with APS, we plan to upload processed and anonymized files prior to publication.
2. The results of the first step of the name disambiguation are available upon reasonable request from the authors. The code to reproduce the first step manually will be added shortly.


## Configuration
Configuration of the docker builds, script execution and notebooks is mainly handled by `.env`-files.
An example of such a file can be found in `/secrets/sample.env`.
The file defines environment variables, such as folders for in- and output folders or model parameters.
Docker sets these variables to the environment when building and running the containers (and scripts therein).
The high-level execution scripts in `/scripts/` then read these variables.
In many cases, the configuration can be overwritten by applying command line arguments when calling these scripts.
By default, when following to the provided folder structure, the results of the paper are reproducible using only this `sample.env` and direct calls to the provided scripts.
In case you do want to make a switch to the configuration and a respective argument option is unavailable, simply prepend a `export <ARG>=<VAL>;` to the `docker exec`-calls presented below.
Some important variables include
- `PATH_HOST_DATA/_OUTPUT` points to the directories that contain the raw APS dataset files and results like plots or intermediate results.
- `POSTGRES_*` sets the database configuration. The port will be linked outside the docker container to enable access the raw data using SQL clients, such as [DBeaver](https://dbeaver.io/).

## Execution steps
### Building and running the docker image
### Running the code

## Project structure
To quickly navigate the code base, we provide a brief description of the project tree
```
├── Dockerfile # Configures the local docker container
├── LICENCE
├── README.md
├── cumulative_advantage_brokerage # The main source code
│   ├── career_series # Compute career stages & impact groups
│   ├── config
│   ├── constants
│   ├── data # Handling of raw data
│   ├── dbm # Database models
│   └── network # Network & tracking of brokerage events
├── data # Folder in container that contains the input data
│   └── aps # Contains the APS data
├── docker-compose.yml # Manages images (local & database)
├── output # Contains output data, results and plots
│   ├── data
│   └── logs
├── requirements.txt
├── scripts # Entry-point of callable scripts (in order!)
│   ├── 00_data_preprocessing # CSV transform, name disamb. & gender inference
│   ├── 01_database_setup # Setup of database infrastructure
│   └── 02_brokerage_frequencies # & career stages & impact groups
├── secrets
│   └── sample.env # Configuration file
└── setup.py
```


## Code & data dependency backlinks
- APS data integrator: `f8ea8f854720a5df8ee7d7c8119cbb773be94cbe`