.PHONY: help install format lint test clean docker-up docker-down docs

help:
	@echo "Available commands:"
	@echo "  make install      Install project dependencies"
	@echo "  make format       Format Python code"
	@echo "  make lint         Run linting checks"
	@echo "  make test         Run tests"
	@echo "  make docker-up    Start local Docker services in later phases"
	@echo "  make docker-down  Stop local Docker services in later phases"
	@echo "  make clean        Remove local cache files"

install:
	pip install -e ".[dev]"

format:
	python -m ruff format src tests

lint:
	python -m ruff check src tests

test:
	pytest

docker-up:
	@echo "Docker Compose services will be implemented in a later phase."

docker-down:
	@echo "Docker Compose services will be implemented in a later phase."

docs:
	@echo "Documentation lives in the docs/ directory."

clean:
	@echo "Removing local cache files"
	@if exist .pytest_cache rmdir /s /q .pytest_cache
	@if exist .ruff_cache rmdir /s /q .ruff_cache
