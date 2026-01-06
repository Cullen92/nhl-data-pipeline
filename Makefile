setup:
	./setup_wsl.sh
	./setup_venv.sh

# Use bash for shell commands
SHELL := /bin/bash

.PHONY: setup build-plugins test lint export-sheets dbt-run sync-mwaa

# Use .venv if it exists, otherwise use system python
PYTHON := $(shell if [ -f .venv/bin/python ]; then echo .venv/bin/python; else echo python; fi)
DBT := $(shell if [ -f .venv/bin/dbt ]; then echo .venv/bin/dbt; else echo dbt; fi)

# MWAA S3 bucket - update this to match your environment
MWAA_BUCKET := mwaa-bucket-nhl-cullenm-dev

# Helper to source .env before running commands
ENV_SOURCE := $(if $(wildcard ./.env), source ./.env &&,)

build-plugins:
	@echo "Building plugins.zip for MWAA..."
	@python3 -c "import shutil; shutil.make_archive('plugins', 'zip', root_dir='src', base_dir='nhl_pipeline')"
	@echo "Done! Upload plugins.zip to your MWAA S3 bucket."

sync-mwaa:
	@echo "Syncing DAGs and dbt project to MWAA S3 bucket..."
	@aws s3 sync dags/ s3://$(MWAA_BUCKET)/dags/ --exclude "__pycache__/*" --exclude "*.pyc"
	@aws s3 sync dbt_nhl/ s3://$(MWAA_BUCKET)/dags/dbt_nhl/ --exclude "target/*" --exclude "logs/*" --exclude "dbt_packages/*" --exclude "__pycache__/*"
	@aws s3 cp plugins.zip s3://$(MWAA_BUCKET)/plugins.zip
	@aws s3 cp requirements-mwaa.txt s3://$(MWAA_BUCKET)/requirements.txt
	@echo "Done! MWAA will pick up changes within a few minutes."

deploy-mwaa: build-plugins sync-mwaa
	@echo "Full MWAA deployment complete!"

test:
	@$(PYTHON) -m pytest -q

lint:
	@$(PYTHON) -m ruff check .

lint-fix:
	@$(PYTHON) -m ruff check --fix .

validate-data:
	@echo "Running Time Travel data quality validation..."
	@$(PYTHON) -m nhl_pipeline.utils.time_travel_validator

export-sheets:
	@echo "Exporting data to Google Sheets..."
	@source ./.env && $(PYTHON) -m nhl_pipeline.export.sheets_export
	@echo "Done! View your data in Google Sheets."

dbt-run:
	@echo "Running dbt models..."
	@source ./.env && cd dbt_nhl && ../$(DBT) run
	@echo "Done!"

dbt-run-export: dbt-run export-sheets
	@echo "dbt run and Google Sheets export complete!"

export-tableau:
	@echo "Exporting shot location data for Tableau heatmaps..."
	@source ./.env && $(PYTHON) -m nhl_pipeline.export.tableau_export

export-tableau-full:
	@echo "Exporting all shot data for Tableau (including raw events)..."
	@source ./.env && $(PYTHON) -m nhl_pipeline.export.tableau_export --include-raw