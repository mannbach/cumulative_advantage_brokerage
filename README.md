# Cumulative advantage of brokerage in academia
This repository contains the code to reproduce the results of the "Cumulative advantage of brokerage in academia" paper.
A summary of the [project structure](#project-structure) and [frequent problems](#faq) can be found below.

## Requirements
### Software
To make this code independent of the underlying OS and software dependencies, we make us of docker compose (`>= v.2.12.2`, see [official docs](https://docs.docker.com/compose/) for installation steps).
Docker takes care of installing required software packages, and the setup of the local code and the [PostgreSQL database](https://www.postgresql.org/).

### Data
The successful reproduction of the results currently requires two datasets:
1. At its current stage, the original dataset up until January, 31st 2020 as provided by the American Physical Society (APS) is required (both citations and metadata).
As sharing these files is not permitted, a request must be filed [here](https://journals.aps.org/datasets).
2. The code to reproduce the first step of the name disambiguation will be added shortly.
Until then, the results are available directly from the [authors](#contact-information).

## Configuration
Configuration of the docker builds, script execution and notebooks is mainly handled by `.env`-files.
An example of such a file can be found in `/secrets/sample.env`.
The file defines environment variables, such as folders for in- and output folders or model parameters.
Docker sets these variables to the environment when building and running the containers (and scripts therein).
The high-level execution scripts in `/scripts/` then read these variables.
In many cases, the configuration can be overwritten by applying command line arguments when calling these scripts.
By default, when following to the provided folder structure, the results of the paper are reproducible using only this `sample.env` and direct calls to the provided scripts.

Some important variables include
- `PATH_HOST_DATA/_OUTPUT` points to the directories that contain the raw APS dataset files and results like plots or intermediate results.
- `POSTGRES_*` sets the database configuration. The port will be linked outside the docker container to enable access the raw data using SQL clients, such as [DBeaver](https://dbeaver.io/).

## Execution steps
### Building and running the docker image
After adjusting the [configuration](#configuration), build the docker image by running
```bash
docker compose --env-file secrets/sample.env build
```
replacing `secrets/sample.env` to the path to your custom `.env`-file in case you created a new on (from here on, we will continue to use `secrets/sample.env`).
Start the container by executing
```bash
docker compose --env-file secrets/sample.env up -d
```
with the `-d` flag signaling docker to run the containers in a background process, allowing you to continue using the same terminal.

### Running the code
All executable, high-level scripts are located in the `scripts/` folder.
To execute a script at location `<script.py>` and its arguments `<arguments>`, simply run (see below for examples):
```bash
docker exec -t cumulative_advantage_brokerage python <script.py> <arguments>
```
The `-t` forwards the output of the container immediately to your local terminal.

Running the executable scripts in the presented order will reproduce the figures of the paper.
If two scripts share the same prefix number (e.g., `01a_compute_brokerage_frequencies.py` and `01b_compute_impact_groups` in `02_brokerage_frequencies/`) or if no prefix is specified (as in `03_plotting/`), they can be executed in parallel.
Otherwise, they have to be executed sequentially.
Below are some examples that also highlight custom arguments.

#### Gender inference
To compute the gender inference with an uncertainty threshold set to 30%, run
```bash
docker exec -t cumulative_advantage_brokerage\
    python 00_data_processing/02_infer_gender_data.py --threshold 0.3
```

#### Inferring impact groups
To compute scientists' impact groups, run
```bash
docker exec -t cumulative_advantage_brokerage\
    python 02_brokerage_frequencies/01b_compute_impact_groups.py
```
Note that the result will be stored in the database under a metric configuration ID that is communicated through the script's output.
These IDs are inferred automatically by subsequent scripts.
To prepare for incomplete runs or deviations from the order, it is best to remember the respective ID.
Follow-up scripts always take these IDs as input.

#### Comparisons
To compute the statistical tests, comparing brokerage participation across impact groups and stages, run
```bash
docker exec -t cumulative_advantage_brokerage\
    python 02_brokerage_frequencies/02_compute_frequency_comparisons.py\
        -c <comparisons...> -g <groupers...> -t <tests...>
```
with
- `<comparisons>`: Choice of comparisons to compute, among `bf-comparison`, `br-comparison`, `br-correlation`. Defaults to all comparisons.
- `<groupers>`: List of how to group the data. These are required for comparisons grouped by gender or cohort decade.
- `<tests>`: The statistical tests to use. Some tests, like `br-correlation`, require special correlational tests, such as `permut-pearson` or `permut-spearman`. Invalid combinations are skipped.

Note that the scripts will execute the combinatorial product of all configs.
If these choices are not limited, the execute might take a long time.
Other arguments include the IDs of previous results (e.g., `--id-impact-group-citations` and `--id-impact-group-productivity` for [impact groups inference](#inferring-impact-groups) results).
Multiple executions, for instance, by fixing a single value of `comparisons`, can run in parallel.

#### Figure - Gender disparities
To reproduce the Gender disparities figure, run
```bash
docker exec -t cumulative_advantage_brokerage\
    python 03_plotting/figure_05_gender_disparities.py\
        --normalize
```
This will produce the plot and saves it in `output/`.
The `--normalize`-flag will also produce the normalized brokerage event counts in a separate plot.

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
│   ├── dbm # Database models and communication
│   ├── network # Network & tracking of brokerage events
│   ├── queries.py # Common queries
│   ├── stats # Statistical tests, groupers and comparisons
│   └── visuals # Plotting code
├── data # Folder in container that contains the input data
│   └── aps # Contains the APS data
├── docker-compose.yml
├── output # Contains output data, results and plots
│   ├── data
│   ├── logs
├── requirements.txt
├── scripts # Entry-point of callable scripts
│   ├── 00_data_preprocessing # CSV transform, name disamb. & gender inference
│   ├── 01_database_setup # Setup of database infrastructure
│   ├── 02_brokerage_frequencies # Career stages, impact groups & comparisons
│   └── 03_plotting # Plotting
├── secrets
│   └── sample.env # Configuration file
└── setup.py
```

## FAQ
### Changing the config
In case you do want to make a change to the configuration and a respective argument option is unavailable, you typicall do not need to re-build the image.
In most cases, it is enough to alter the `docker exec`-calls presented below as follows:
```bash
docker exec -t cumulative_advantage_brokerage /bin/bash -c\
    "export <ARG>=<VAL> && python <script.py> <arguments>"
```
with `<ARG>=<VAL>` being the updated configuration.

### Identifying metric IDs
To retrieve a lost metric ID which could not be loaded automatically (indicated by an assertion error stating `"No career series ID found with args: ..."`), you have two options.
1. Re-run the computation of the respective metric. This might take a while, but is much simpler than option two.
2. Query the metric ID directly from the PostgreSQL database.
This can be done by setting up a local database client, such as [DBeaver](https://dbeaver.io/), using the configuration specified in the `.env`-file used during the [building step](#building-and-running-the-docker-image)
 (the provided port number is forwarded to your local machine).
The respective metric configuration can then be found in the `metric_configuration` database table.

## Contact information
In case you face any problems feel free to file an issue directly in GitHub or send a mail to the corresponding authors
- Jan Bachmann: jan@mannbach.de

## Code & data dependency backlinks
- APS data integrator: `f8ea8f854720a5df8ee7d7c8119cbb773be94cbe`
- Zenodo DOI: [10.5281/zenodo.12724812](https://zenodo.org/doi/)