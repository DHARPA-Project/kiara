[![PyPI status](https://img.shields.io/pypi/status/kiara.svg)](https://pypi.python.org/pypi/kiara/)
[![PyPI version](https://img.shields.io/pypi/v/kiara.svg)](https://pypi.python.org/pypi/kiara/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/kiara.svg)](https://pypi.python.org/pypi/kiara/)
[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2FDHARPA-Project%2Fkiara%2Fbadge%3Fref%3Ddevelop&style=flat)](https://actions-badge.atrox.dev/DHARPA-Project/kiara/goto?ref=develop)
[![Code style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

# kiara

*A workflow management and execution engine for the DHARPA project.*

 - Documentation: [https://dharpa.org/kiara](https://dharpa.org/kiara)
 - Code: [https://github.com/DHARPA-Project/kiara](https://github.com/DHARPA-Project/kiara)

## Description

Documentation still to be done.

## Downloads

### Binaries

Only snapshot binaries (for now):

  - [Linux](https://github.com/DHARPA-Project/kiara/actions/workflows/build-linux.yaml)
  - [Windows](https://github.com/DHARPA-Project/kiara/actions/workflows/build-windows.yaml)
  - [Mac OS X](https://github.com/DHARPA-Project/kiara/actions/workflows/build-darwin.yaml)

# Development

## Requirements

- Python (version >=3.6 -- some make targets only work for Python >=3.7, but *kiara* itself should work on 3.6)
- pip, virtualenv
- git
- make
- [direnv](https://direnv.net/) (optional)


## Prepare development environment

```console
git clone https://github.com/DHARPA-Project/kiara.git
cd kiara
python3 -m venv .venv
source .venv/bin/activate
make init
```

If you use [direnv](https://direnv.net/), you can alternatively do:

``` console
git clone https://github.com/DHARPA-Project/kiara.git
cd kiara
cp .envrc.disabled .envrc
direnv allow   # if using direnv, otherwise activate virtualenv
make init
```

*Note*: you might want to adjust the Python version in ``.envrc`` (should not be necessary in most cases though)

## ``make`` targets

- ``init``: init development project (install project & dev dependencies into virtualenv, as well as pre-commit git hook)
- ``flake``: run *flake8* tests
- ``mypy``: run mypy tests
- ``test``: run unit tests
- ``docs``: create static documentation pages
- ``serve-docs``: serve documentation pages (incl. auto-reload)
- ``clean``: clean build directories

For details (and other, minor targets), check the ``Makefile``.


## Running tests

``` console
> make test
# or
> make coverage
```


## Copyright & license

This project is MPL v2.0 licensed, for the license text please check the [LICENSE](/LICENSE) file in this repository.

[Copyright (c) 2021 DHARPA project](https://dharpa.org)
