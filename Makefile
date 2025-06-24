.PHONY: install install-dev format lint type-check test test-cov clean run-etl run-streaming run-ml run-all docker-up docker-down docker-full-up docker-full-down help

# Default target
.DEFAULT_GOAL := help

# Variables
PYTHON := python
UV := uv
RUFF := $(UV) run ruff
MYPY := $(UV) run mypy
PYTEST := $(UV) run pytest
SRC_DIR := src
TEST_DIR := tests
VENV_DIR := .venv

# Help target
help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Virtual environment: $(VENV_DIR)"
	@echo "Python version: $$($(UV) run python --version 2>/dev/null || echo 'Not installed')"

# Installation targets
install: ## Install project dependencies with uv (creates .venv)
	$(UV) sync
	@echo "Virtual environment created at $(VENV_DIR)"
	@echo "Python version: $$($(UV) run python --version)"

install-dev: ## Install project with development dependencies (creates .venv)
	$(UV) sync --dev
	@echo "Virtual environment created at $(VENV_DIR)"
	@echo "Python version: $$($(UV) run python --version)"

# Code quality targets
format: ## Format code with ruff
	$(RUFF) format $(SRC_DIR) $(TEST_DIR) scripts/
	$(RUFF) check --fix $(SRC_DIR) $(TEST_DIR) scripts/

lint: ## Lint code with ruff
	$(RUFF) check $(SRC_DIR) $(TEST_DIR) scripts/

type-check: ## Type check with mypy
	$(MYPY) $(SRC_DIR)

check: lint type-check ## Run all checks (lint + type-check)

# Testing targets
test: ## Run tests
	$(PYTEST) $(TEST_DIR)

test-cov: ## Run tests with coverage
	$(PYTEST) $(TEST_DIR) --cov=$(SRC_DIR) --cov-report=html --cov-report=term

# Running applications
run-etl: ## Run ETL pipeline
	$(UV) run spark-app --mode etl --date $${DATE:-2024-01-01}

run-streaming: ## Run streaming pipeline
	$(UV) run spark-app --mode streaming

run-ml: ## Run ML pipeline
	$(UV) run spark-app --mode ml

run-all: ## Run all pipelines
	$(UV) run spark-app --mode all

# Docker targets
up: ## Start Spark cluster with Docker Compose
	docker-compose up -d

down: ## Stop Spark cluster
	docker-compose down

logs: ## Show Docker logs
	docker-compose logs -f

# Utility targets
clean: ## Clean cache and build files
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf build/ dist/ htmlcov/ .coverage

clean-venv: ## Remove virtual environment
	rm -rf $(VENV_DIR)
	@echo "Virtual environment removed"

clean-all: clean clean-venv ## Clean everything including virtual environment

gen-data: ## Generate sample data
	$(UV) run python scripts/generate_sample_data.py
gen-data-s3:
	$(UV) run python scripts/generate_sample_data.py --s3

create-topics: ## Create Kafka topics
	docker exec kafka kafka-topics --create --topic events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 || true
	docker exec kafka kafka-topics --create --topic processed_events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 || true
	docker exec kafka kafka-topics --create --topic predictions --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 || true

# Development workflow
dev-setup: install-dev generate-data ## Full development setup
	$(UV) run pre-commit install
	@echo "Development environment ready!"

dev-check: format lint type-check test ## Run all development checks

venv-info: ## Show virtual environment information
	@echo "Virtual environment: $(VENV_DIR)"
	@echo "Python executable: $$($(UV) run which python 2>/dev/null || echo 'Not found')"
	@echo "Python version: $$($(UV) run python --version 2>/dev/null || echo 'Not installed')"
	@echo "uv version: $$($(UV) --version 2>/dev/null || echo 'uv not installed')"

# Quick commands
fmt: format ## Alias for format
qa: dev-check ## Alias for dev-check
info: venv-info ## Alias for venv-info