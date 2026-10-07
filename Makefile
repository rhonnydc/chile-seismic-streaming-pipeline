-include .env
export KAFKA_BOOTSTRAP_SERVERS KAFKA_RAW_EARTHQUAKES_TOPIC KAFKA_ENRICHED_EARTHQUAKES_TOPIC
export CONSUMER_GROUP_ID
export FAKE_PRODUCER_EVENT_COUNT FAKE_PRODUCER_INTERVAL_SECONDS
SCHEMA_REGISTRY_URL ?= http://localhost:8081
export SCHEMA_REGISTRY_URL
POSTGRES_HOST ?= localhost
POSTGRES_PORT ?= 5432
POSTGRES_DB ?= seismic
POSTGRES_USER ?= seismic_user
POSTGRES_PASSWORD ?= seismic_password
POSTGRES_SINK_GROUP_ID ?= seismic-postgres-sink
export POSTGRES_HOST POSTGRES_PORT POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD POSTGRES_SINK_GROUP_ID

PYTHON ?= $(firstword $(wildcard .venv/Scripts/python.exe .venv/bin/python) python)

.PHONY: help install format lint test up down ps logs restart clean clean-python docs create-topics produce-fake register-schemas list-schemas test-contracts reset-raw-topic consume-enrich test-enrichment init-db consume-sink query-db test-sink

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
	@echo "  make create-topics Create raw and enriched topics if needed"
	@echo "  make produce-fake  Publish simulated earthquakes to Kafka"
	@echo "  make register-schemas Register raw and enriched Avro schemas"
	@echo "  make list-schemas     List Schema Registry subjects"
	@echo "  make test-contracts   Run Avro contract and producer tests"
	@echo "  make reset-raw-topic  Delete and recreate local raw_earthquakes"
	@echo "  make consume-enrich   Consume raw events and publish enriched events"
	@echo "  make test-enrichment Run Phase 4 enrichment, contract, and consumer tests"
	@echo "  make init-db       Create the enriched earthquake table if needed"
	@echo "  make consume-sink  Consume enriched events and persist them in Postgres"
	@echo "  make query-db      Run the analytical Postgres queries"
	@echo "  make test-sink     Run Postgres sink and consumer tests"

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

register-schemas:
	$(PYTHON) scripts/register_schemas.py

list-schemas:
	curl --fail --silent --show-error "$(SCHEMA_REGISTRY_URL)/subjects"

test-contracts:
	$(PYTHON) -m pytest tests/contracts tests/unit/test_fake_earthquake_producer.py

reset-raw-topic:
	$(PYTHON) scripts/reset_raw_topic.py

consume-enrich:
	$(PYTHON) -m seismic_pipeline.consumers.enriching_consumer

test-enrichment:
	$(PYTHON) -m pytest tests/unit/test_enrichment.py tests/contracts/test_enriched_earthquake_contract.py tests/unit/test_enriching_consumer.py

init-db:
	docker compose cp sql/init.sql postgres:/tmp/init.sql
	docker compose exec -T postgres psql -U "$(POSTGRES_USER)" -d "$(POSTGRES_DB)" -v ON_ERROR_STOP=1 -f /tmp/init.sql

consume-sink:
	$(PYTHON) -m seismic_pipeline.consumers.postgres_sink_consumer

query-db:
	docker compose cp sql/analytics_queries.sql postgres:/tmp/analytics_queries.sql
	docker compose exec -T postgres psql -U "$(POSTGRES_USER)" -d "$(POSTGRES_DB)" -v ON_ERROR_STOP=1 -f /tmp/analytics_queries.sql

test-sink:
	$(PYTHON) -m pytest tests/unit/test_postgres_sink.py tests/unit/test_postgres_sink_consumer.py
