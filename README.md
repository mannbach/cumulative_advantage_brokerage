# Cumulative advantage of brokerage in academia
This repository will contain the code to reproduce the results of the "Cumulative advantage of brokerage in academia" paper.

## Requirements
To make this code independant of the underlying OS, we make us of docker compose (`>= v.2.12.2`, see [official docs](https://docs.docker.com/compose/) for installation steps).

## Setup
Configuration of the docker builds, script execution and notebooks is done by `.env`-files.
An example of such a file can be found in `/secrets/sample.env`.
The file defines configurational environment variables, such as folders for in- and output or model parameters.
Docker sets these variables to the environment when building and running.
The high-level execution scripts in `/scripts/` then read these variables.
In many cases, the configuration can be overwritten by applying command line arguments when calling these scripts.
By default, when following to the provided folder structure, the results of the paper are reproducible using only this `sample.env` and direct calls to the provided scripts.

## Code dependency backlinks
- APS data integrator: f8ea8f854720a5df8ee7d7c8119cbb773be94cbe