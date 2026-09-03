.PHONY: help install format lint test up down ps logs restart clean clean-python docs

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

install:
	pip install -e ".[dev]"

format:
	python -m ruff format src tests

lint:
	python -m ruff check src tests

test:
	pytest

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
