SHELL := /bin/bash

COMPOSE_FILE := compose/docker-compose.yml
ENV_FILE := compose/.env

COMPOSE := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE)

.PHONY: help up down restart build ps

help:
	@echo "Targets:"
	@echo "  make up          Starts all services"
	@echo "  make down        Stops and removes all services"
	@echo "  make restart     Restarts all services"
	@echo "  make build       Builds images only"
	@echo "  make ps          Shows containers"

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
