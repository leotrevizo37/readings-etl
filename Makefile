SHELL := /bin/bash

PYTHON ?= python
COMPOSE_FILE := compose/docker-compose.yml
ENV_FILE := compose/.env

COMPOSE := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE)

.PHONY: help up down restart build ps lint-python test-python lint-projectA lint-projectB lint-projectD test-projectA test-projectB test-projectD

help:
	@echo "Targets:"
	@echo "  make up          Start every service"
	@echo "  make down        Stops every service and delete images and volumes"
	@echo "  make restart     Re-start services"
	@echo "  make build       Builds docker images"
	@echo "  make ps          Show running project containers"
	@echo "  make lint-python Executes Ruff over every Python package"
	@echo "  make test-python Tests every project"

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v

restart:
	$(COMPOSE) restart

build:
	$(COMPOSE) build

ps:
	$(COMPOSE) ps

lint-python: lint-projectA lint-projectB lint-projectD

lint-projectA:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-projectA -lc "python -m pip install --quiet ruff && cd /opt/dagster/app/projectA && ruff check src tests"

lint-projectB:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-projectB -lc "python -m pip install --quiet ruff && cd /opt/dagster/app/projectB && ruff check src tests"

lint-projectD:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-projectD -lc "python -m pip install --quiet ruff && cd /opt/dagster/app/projectD && ruff check src tests"

test-python: test-projectA test-projectB test-projectD

test-projectA:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-projectA -lc "cd /opt/dagster/app/projectA && python -m pytest --import-mode=importlib tests"

test-projectB:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-projectB -lc "cd /opt/dagster/app/projectB && python -m pytest --import-mode=importlib tests"

test-projectD:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-projectD -lc "cd /opt/dagster/app/projectD && python -m pytest --import-mode=importlib tests"