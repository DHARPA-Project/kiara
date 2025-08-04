.PHONY: clean clean-test clean-pyc clean-build docs help
.DEFAULT_GOAL := help

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)


clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg' -exec rm -rf {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache
	rm -fr .mypy_cache

mypy: ## run mypy
	uv run mypy --namespace-packages --explicit-package-base src/

lint:
	uv run ruff check --fix src/

format:
	uv run ruff format src/

test: ## run tests quickly with the default Python
	uv run pytest tests

docs:
	uv run mkdocs build

docs-serve:
	uv run mkdocs serve

check: lint mypy test ## run dev-related checks

pre-commit: ## run pre-commit on all files
	uv run pre-commit run --all-files

coverage: ## check code coverage quickly with the default Python
	coverage run -m pytest tests
	coverage report -m
	coverage html
	xdg-open htmlcov/index.html

render-api:
	uv run kiara render --source-type base_api --target-type kiara_api item kiara_api template_file=src/kiara/interfaces/python_api/kiara_api.py target_file=src/kiara/interfaces/python_api/kiara_api.py

pre-commit: ## run pre-commit on all files
	uv run pre-commit run --all-files

doc: ## build documentation
	uv run mkdocs build
