.PHONY: help format lint typecheck test up down migrate revision db-up db-down clean

help:
	@echo "Available commands:"
	@echo "  make format     - Format code with ruff"
	@echo "  make lint       - Lint code with ruff"
	@echo "  make typecheck  - Type check with mypy"
	@echo "  make test       - Run tests with pytest"
	@echo "  make up         - Start docker compose services"
	@echo "  make down       - Stop docker compose services"
	@echo "  make db-up      - Start database container"
	@echo "  make db-down    - Stop database container"
	@echo "  make migrate    - Run alembic upgrade head"
	@echo "  make revision   - Create new alembic revision (use MSG=message)"
	@echo "  make clean      - Clean up generated files"

format:
	uv run ruff format .

lint:
	uv run ruff check .

typecheck:
	uv run mypy src/metismedia

test:
	uv run pytest

up:
	docker compose up -d

down:
	docker compose down

db-up:
	docker compose up -d postgres

db-down:
	docker compose stop postgres

migrate:
	uv run alembic upgrade head

revision:
	uv run alembic revision -m "$(MSG)"

clean:
	find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -r {} + 2>/dev/null || true
	rm -rf .mypy_cache .pytest_cache .ruff_cache
