-include .env
export KAFKA_BOOTSTRAP_SERVERS KAFKA_RAW_EARTHQUAKES_TOPIC
export FAKE_PRODUCER_EVENT_COUNT FAKE_PRODUCER_INTERVAL_SECONDS

PYTHON ?= $(firstword $(wildcard .venv/Scripts/python.exe .venv/bin/python) python)

.PHONY: help install format lint test up down ps logs restart clean clean-python docs create-topics produce-fake

help:
	@echo "Available commands:"
	@echo "  make install      Install project dependencies"
	@echo "  make format       Format Python code"
	@echo "  make lint         Run linting checks"
	@echo "  make test         Run tests"
	@echo "  make up           Start the local Docker Compose stack"
	@echo "  make down         Stop the local Docker Compose stack"
	@echo "  make ps           Show local Docker Compose services"
	@echo "  make logs         Follow local Docker Compose logs"
	@echo "  make restart      Restart the local Docker Compose stack"
	@echo "  make clean        Stop the stack and remove local volumes"
	@echo "  make clean-python Remove local Python cache files"
	@echo "  make create-topics Create raw_earthquakes if needed"
	@echo "  make produce-fake  Publish simulated earthquakes to Kafka"

install:
	$(PYTHON) -m pip install -e ".[dev]"

format:
	$(PYTHON) -m ruff format src tests scripts

lint:
	$(PYTHON) -m ruff check src tests scripts

test:
	$(PYTHON) -m pytest

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

restart: down up

clean:
	docker compose down --volumes --remove-orphans

clean-python:
	@echo "Removing local Python cache files"
	@if exist .pytest_cache rmdir /s /q .pytest_cache
	@if exist .ruff_cache rmdir /s /q .ruff_cache

docs:
	@echo "Documentation lives in the docs/ directory."

create-topics:
	$(PYTHON) scripts/create_topics.py

produce-fake:
	$(PYTHON) -m seismic_pipeline.producers.fake_earthquake_producer
