.PHONY: install lint format typecheck test test-unit test-integration test-e2e migrate web worker

install:
	pip install -e ".[dev,postgres]"

lint:
	ruff check core apps tests

format:
	ruff format core apps tests
	ruff check --fix core apps tests

typecheck:
	mypy core apps

test:
	pytest

test-unit:
	pytest tests/unit

test-integration:
	pytest tests/integration -m integration

test-e2e:
	pytest tests/e2e -m e2e

migrate:
	alembic upgrade head

web:
	uvicorn apps.web.main:app --reload --port 8000

worker:
	python -m apps.worker.main
