# Serving Layer Data
Serving layer for processing and preparing data intended for consumption in BI systems, 
developed with Spark and integrating automated extraction, transformation, and load (ETL).

[![docker-image.yml](https://github.com/leotrevizo37/readings-etl/actions/workflows/validate-docker-image.yml/badge.svg)](https://github.com/leotrevizo37/readings-etl/actions/workflows/validate-docker-image.yml)
[![validate-compose.yml](https://github.com/leotrevizo37/readings-etl/actions/workflows/validate-compose.yml/badge.svg)](https://github.com/leotrevizo37/readings-etl/actions/workflows/validate-compose.yml)
[![lint-sql.yml](https://github.com/leotrevizo37/readings-etl/actions/workflows/lint-sql.yml/badge.svg)](https://github.com/leotrevizo37/readings-etl/actions/workflows/lint-sql.yml)

## Scope
This repository provides:
- A Dagster webserver + daemon running in containers.
- A Spark master running in containers.
- Automation/orchestration of a multi-node Spark cluster.
- Integration with the ADLS data lake.
Out of scope:
- Complex ETLs that are not required by the agentic tools.

## Repository Structure
- `compose/`
  - `docker-compose.yml`: local orchestration
  - `.env` / `.env.example`: environment variables
- `dagster/`
  - `Dockerfile`: Dagster image
  - `requirements.txt`: Python dependencies
  - `project_A/`: Dagster project (assets/jobs/resources)
    - `defs/`: Dagster project definition
    - `tests`: unit tests
  - `project_B/`: Dagster project (assets/jobs/resources)
    - `defs/`: Dagster project definition
    - `tests`: unit tests
  - `project_C/`: Dagster project (assets/jobs/resources)
    - `defs/`: Dagster project definition
    - `tests`: unit tests
  - `dagster_home/`: Dagster configuration/state
- `sql/`
  - `init/`: T-SQL scripts executed once directly in the database before starting the services 
    (table definitions, etc.)
  - `procedures/`: T-SQL scripts executed once directly in the database before starting the services
- `workspace.yaml`: Dagster workspace configuration

## Prerequisites
- Docker Desktop with Docker Compose v2 (minimum)
- Configure the `.env` file
- Make utilities installed (optional)

## Configuration
Create `compose/.env` from `compose/.env.example`.

### Where It Runs (Expected Environment)
This service is intended to run in a Docker environment. with only spark master running.
Spark workers are expected to be added on a compute cluster separately. 
(Used [spark-worker](https://github.com/leotrevizo37/data-etls-spark-worker))
- **Runtime**: Docker Engine (server/daemon on Linux): `29.1.2` / Python 3.11+ <3.14+
- **Docker-Compose**: v2+
- **OS**: Linux (Ubuntu 22.04)
- **Architecture**: x86_64
- **RAM**: 8GB+
- **CPU**: 4+
- **Disk**: 100GB+
- **Ports**: 8080 (webserver-internal), 8081 (webserver-external), 7077 (docker-services), 7078 (external-spark-worker).

## Running the Project 
Run the project from the repository root. 
### Using Make 
If `make` is available on your system, start the services with: `make up` 
### Using Docker Compose Directly 
If `make` is not available, run Docker Compose manually: `docker compose --env-file compose/.env -f compose/docker-compose.yml up -d --build` 
### Scaling Spark Workers 
For better performance, depending on if the project can benefit from it, you can use
[spark-worker](https://github.com/leotrevizo37/data-etls-spark-worker) to add more workers and executors.
## Local Development and Testing 
### Go to the Dagster Project Directory `cd dagster/projectA` 
### Set Up the Virtual Environment 
#### Windows Create the virtual environment: 
`py -m venv .venv` Install dependencies: 
`uv sync` Activate the virtual environment: 
`.venv\Scripts\activate` 
#### Linux Activate the virtual environment: 
`source .venv/bin/activate` 
### Run Dagster Locally 
Start the local Dagster development server: `uv run dagster dev`
