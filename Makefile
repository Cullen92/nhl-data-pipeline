setup:
	./setup_wsl.sh
	./setup_venv.sh

.PHONY: setup build-plugins test lint

# Use .venv if it exists, otherwise use system python
PYTHON := $(shell if [ -f .venv/bin/python ]; then echo .venv/bin/python; else echo python; fi)

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