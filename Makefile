SHELL := /bin/bash

PYTHON ?= python
COMPOSE_FILE := compose/docker-compose.yml
ENV_FILE := compose/.env

COMPOSE := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE)

.PHONY: help up down restart build ps lint-python test-python lint-projectA lint-projectB lint-projectC test-projectA test-projectB test-projectC

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

lint-python: lint-projectA lint-projectB lint-projectC

lint-projectA:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-projectA -lc "python -m pip install --quiet ruff && cd /opt/dagster/app/projectA && ruff check src tests"

lint-projectB:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-projectB -lc "python -m pip install --quiet ruff && cd /opt/dagster/app/projectB && ruff check src tests"

lint-projectC:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-projectC -lc "python -m pip install --quiet ruff && cd /opt/dagster/app/projectC && ruff check src tests"

test-python: test-projectA test-projectB test-projectC

test-projectA:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-projectA -lc "cd /opt/dagster/app/projectA && python -m unittest discover tests -p 'test_*.py'"

test-projectB:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-projectB -lc "cd /opt/dagster/app/projectB && python -m unittest discover tests -p 'test_*.py'"

test-projectC:
	$(COMPOSE) run --rm --no-deps --entrypoint sh dagster-user-code-projectC -lc "cd /opt/dagster/app/projectC && python -m unittest discover tests -p 'test_*.py'"