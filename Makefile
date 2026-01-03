setup:
	./setup_wsl.sh
	./setup_venv.sh

# Use bash for shell commands
SHELL := /bin/bash

.PHONY: setup build-plugins test lint export-sheets dbt-run

# Use .venv if it exists, otherwise use system python
PYTHON := $(shell if [ -f .venv/bin/python ]; then echo .venv/bin/python; else echo python; fi)
DBT := $(shell if [ -f .venv/bin/dbt ]; then echo .venv/bin/dbt; else echo dbt; fi)

# Helper to source .env before running commands
ENV_SOURCE := $(if $(wildcard ./.env), source ./.env &&,)

build-plugins:
	@echo "Building plugins.zip for MWAA..."
	@python3 -c "import shutil; shutil.make_archive('plugins', 'zip', root_dir='src', base_dir='nhl_pipeline')"
	@echo "Done! Upload plugins.zip to your MWAA S3 bucket."

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

generate-rink:
	@echo "Generating NHL rink background image..."
	@$(PYTHON) -m nhl_pipeline.export.generate_rink_image