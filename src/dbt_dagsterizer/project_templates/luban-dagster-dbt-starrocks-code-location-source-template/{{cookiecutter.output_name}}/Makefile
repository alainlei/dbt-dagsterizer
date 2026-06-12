
.PHONY: help setup install refresh-dagsterizer macros-sync dev test check-db ods-test-bootstrap ods-test-append dbt-deps dbt-parse dbt-build dbt-run clean{% if cookiecutter.include_docker %} docker-up docker-down docker-ps docker-logs{% endif %}


# Load local environment variables (if present) for dbt/Dagster commands.
# The leading '-' keeps make from erroring before `.env` exists.
-include .env
export

DBT_PROJECT_DIR ?= ./dbt_project
DBT_PROFILES_DIR ?= ./dbt_project
DAGSTER_HOME ?= $(CURDIR)/dagster_home
DBT_VARS ?=
ODS_TEST_BOOTSTRAP_VARS ?= {"enable_ods_test": true}
ODS_TEST_APPEND_VARS ?= {"enable_ods_test": true, "ods_test_orders_append_count": 1000}

help: ## Show available targets
	@echo "Available targets:"
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Setup local development environment
	@echo "Setting up local development environment..."
	@if [ ! -f .env ]; then \
		echo "Copying .env.example to .env..."; \
		cp .env.example .env; \
		echo "Please update .env with your local database credentials."; \
	else \
		echo ".env file already exists. Skipping."; \
	fi
	@$(MAKE) install
	@echo "Setup complete!"


install: ## Install or update python dependencies
	@echo "Installing dependencies..."
	uv sync
	$(MAKE) macros-sync


refresh-dagsterizer: ## Reinstall local dbt-dagsterizer package and sync template macros
	@echo "Reinstalling local dbt-dagsterizer..."
	uv sync --reinstall-package dbt-dagsterizer
	$(MAKE) macros-sync


macros-sync: ## Sync dbt macros required by this code location
	uv run dbt-dagsterizer macros sync --dbt-project-dir $(DBT_PROJECT_DIR)


dev: ## Run Dagster webserver for local development
	uv run dagster dev


test: ## Run unit tests and dbt tests
	@echo "Running python unit tests..."
	uv run pytest tests/
	@echo "Running dbt tests..."
	@if [ -z "$(DBT_VARS)" ]; then \
		echo "DBT_VARS is required for dbt execution (partition window vars). Example:"; \
		echo "  export DBT_VARS='{\"min_date\":\"2026-01-02\",\"max_date\":\"2026-01-03\",\"min_datetime\":\"2026-01-02 00:00:00\",\"max_datetime\":\"2026-01-03 00:00:00\"}'"; \
		exit 1; \
	fi
	uv run dbt test --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR) --vars '$(DBT_VARS)'


check-db: ## Test connectivity to the StarRocks database
	@echo "Testing StarRocks connectivity using dbt debug..."
	uv run dbt debug --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)

{% if cookiecutter.include_docker %}

docker-up: ## Start local StarRocks via docker compose (local dev only)
	docker compose -f docker/docker-compose.yml up -d


docker-down: ## Stop local StarRocks docker compose stack (local dev only)
	docker compose -f docker/docker-compose.yml down


docker-ps: ## Show docker compose stack status (local dev only)
	docker compose -f docker/docker-compose.yml ps


docker-logs: ## Tail StarRocks logs (local dev only)
	docker compose -f docker/docker-compose.yml logs -f --tail=200
{% endif %}


ods-test-bootstrap: dbt-deps ## Bootstrap ODS test tables (customers/orders)
	uv run dbt run --full-refresh --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR) --select tag:ods_test --vars '$(ODS_TEST_BOOTSTRAP_VARS)'


ods-test-append: dbt-deps ## Append into ODS test tables (simulate new arrivals)
	uv run dbt run --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR) --select tag:ods_test --vars '$(ODS_TEST_APPEND_VARS)'


dbt-deps: ## Install dbt dependencies
	uv run dbt deps --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)


dbt-parse: dbt-deps ## Parse the dbt project (generates manifest.json)
	uv run dbt parse --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)


dbt-build: dbt-deps ## Build the dbt project (run + test)
	@if [ -z "$(DBT_VARS)" ]; then \
		echo "DBT_VARS is required for dbt execution (partition window vars). Example:"; \
		echo "  export DBT_VARS='{\"min_date\":\"2026-01-02\",\"max_date\":\"2026-01-03\",\"min_datetime\":\"2026-01-02 00:00:00\",\"max_datetime\":\"2026-01-03 00:00:00\"}'"; \
		exit 1; \
	fi
	uv run dbt build --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR) --vars '$(DBT_VARS)'


dbt-run: dbt-deps ## Run the dbt project (without testing)
	@if [ -z "$(DBT_VARS)" ]; then \
		echo "DBT_VARS is required for dbt execution (partition window vars). Example:"; \
		echo "  export DBT_VARS='{\"min_date\":\"2026-01-02\",\"max_date\":\"2026-01-03\",\"min_datetime\":\"2026-01-02 00:00:00\",\"max_datetime\":\"2026-01-03 00:00:00\"}'"; \
		exit 1; \
	fi
	uv run dbt run --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR) --vars '$(DBT_VARS)'


clean: ## Clean up temporary generated files and virtual environment
	@echo "Cleaning up generated files..."
	rm -rf .venv
	rm -rf $(DBT_PROJECT_DIR)/target
	rm -rf $(DBT_PROJECT_DIR)/dbt_packages
	rm -rf $(DBT_PROJECT_DIR)/logs
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	@echo "Clean complete!"
