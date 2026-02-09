# Serving Layer Data
Serving layer for processing and preparing data intended for consumption in BI systems, developed with Spark, integrating automated extraction, transformation, and load (ETL).

[![docker-image.yml](https://github.com/leotrevizo37/readings-etl/actions/workflows/validate-docker-image.yml/badge.svg)](https://github.com/leotrevizo37/readings-etl/actions/workflows/validate-docker-image.yml)
[![ci-python.yml](https://github.com/leotrevizo37/readings-etl/actions/workflows/lint-python.yml/badge.svg)](https://github.com/leotrevizo37/readings-etl/actions/workflows/lint-python.yml)
[![validate-compose.yml](https://github.com/leotrevizo37/readings-etl/actions/workflows/validate-compose.yml/badge.svg)](https://github.com/leotrevizo37/readings-etl/actions/workflows/validate-compose.yml)
[![lint-sql.yml](https://github.com/leotrevizo37/readings-etl/actions/workflows/lint-sql.yml/badge.svg)](https://github.com/leotrevizo37/readings-etl/actions/workflows/lint-sql.yml)

# BI serving layer (Dagster + Spark -> SQL Server)
The small file size problem is solved via a relational layer on the server (10.80), in the database corresponding to the client.
- SQL Server (T-SQL schema + stored procedures)
- Dagster (assets/jobs, scheduling/execution)

The project starts with a Docker Compose stack (more details below), and is consumed in Power BI like any SQL endpoint, in this case MySQL, since the database corresponding to the project is in MySQL.

## Scope
This repository provides:
- A Dagster webserver + daemon running in containers.
- A Spark master running in containers.
- Automation/orchestration of a multi-node Spark cluster.
- Integration with the ADLS data lake.
- Transformation of data lake Parquet files into a relational table (serving layer) for BI consumption.

Non-goals:
- Complex ETLs beyond the transformations required to load into tables.

## Repository structure
- `compose/`
  - `docker-compose.yml`: local orchestration
  - `.env` / `.env.example`: environment variables
- `dagster/`
  - `Dockerfile`: Dagster image
  - `requirements.txt`: Python dependencies
  - `project/`: Dagster project (assets/jobs/resources)
  - `dagster_home/`: Dagster configuration/state
- `sql/`
  - `init/`: T-SQL scripts executed once directly on the database before starting services (table definitions, etc.)
  - `procedures/`: T-SQL scripts executed once directly on the database before starting services
- `workspace.yaml`: Dagster workspace configuration

## Prerequisites
- Docker Desktop with Docker Compose v2 (minimum)
- Configure the `.env` file

## Configuration
Create `compose/.env` from `compose-project/.env.example`.

### Where it runs (expected environment)
This service is intended to run on:
- **Server/host**: `VM`
- **Environment**: `Linux`
- **Runtime**: Docker Engine (server/daemon on Linux):** `29.1.2` / Python 3.11+ <3.14+ / etc.

## Run the project (Docker Compose)
From the repo root:

### If you have make:
make up

### Otherwise:
docker compose --env-file compose-project/.env -f compose-project/docker-compose.yml up -d --build

### For better performance (if the host allows it), add workers
docker compose --env-file compose-project/.env -f compose-project/docker-compose.yml up -d --scale spark-worker=4

## For local development and testing

### Go to the root of the Dagster project
cd dagster/project

### Activate Dagster's default venv
* Windows:
py -m venv .venv
uv sync
.venv\Scripts\activate
* Linux:
source venv/bin/activate

### Run the project
uv run dagster dev
