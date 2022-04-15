.PHONY: clean clean-test clean-pyc clean-build docs help
.DEFAULT_GOAL := help

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

docs: ## build documentation
	mkdocs build

serve-docs: ## serve and watch documentation
	mkdocs serve --dirtyreload -a 0.0.0.0:8000

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-doc: ## remove doc artifacts
	rm -fr build/site

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg' -exec rm -fr {} +

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

init: clean ## initialize a development environment (to be run in virtualenv)
	git init
	git checkout -b develop || true
	pip install -U pip
	pip install --extra-index-url https://pypi.fury.io/dharpa/ --extra-index-url https://gitlab.com/api/v4/projects/25344049/packages/pypi/simple -U -e '.[all_dev]'
	pre-commit install
	pre-commit install --hook-type commit-msg
	setup-cfg-fmt setup.cfg || true
	git add "*" ".*"
	pre-commit run --all-files || true
	git add "*" ".*"

update-modules:  ## update default modules dependency
	pip install --extra-index-url https://pypi.fury.io/dharpa/ --extra-index-url https://gitlab.com/api/v4/projects/25344049/packages/pypi/simple -U -e '.[modules_all]'

setup-cfg-fmt: # format setup.cfg
	setup-cfg-fmt setup.cfg || true

black: ## run black
	black --config pyproject.toml setup.py src/kiara tests

flake: ## check style with flake8
	flake8 src/kiara tests

mypy: ## run mypy
	mypy src/kiara

test: ## run tests quickly with the default Python
	py.test

test-all: ## run tests on every Python version with tox
	tox

coverage: ## check code coverage quickly with the default Python
	coverage run -m pytest tests
	coverage report -m
	coverage html
	xdg-open htmlcov/index.html

check: black flake mypy test ## run dev-related checks

pre-commit: ## run pre-commit on all files
	pre-commit run --all-files

dist: clean ## build source and wheel packages
	python setup.py sdist
	python setup.py bdist_wheel
	ls -l dist
