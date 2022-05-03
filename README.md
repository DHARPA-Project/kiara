[![PyPI status](https://img.shields.io/pypi/status/kiara.svg)](https://pypi.python.org/pypi/kiara/)
[![PyPI version](https://img.shields.io/pypi/v/kiara.svg)](https://pypi.python.org/pypi/kiara/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/kiara.svg)](https://pypi.python.org/pypi/kiara/)
[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2FDHARPA-Project%2Fkiara%2Fbadge%3Fref%3Ddevelop&style=flat)](https://actions-badge.atrox.dev/DHARPA-Project/kiara/goto?ref=develop)
[![Coverage Status](https://coveralls.io/repos/github/DHARPA-Project/kiara/badge.svg?branch=develop)](https://coveralls.io/github/DHARPA-Project/kiara?branch=develop)
[![Code style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

# kiara

*A data-centric workflow orchestration framework.*

 - *kiara* user documentation: [https://dharpa.org/kiara.documentation](https://dharpa.org/kiara.documentation/)
 - Code: [https://github.com/DHARPA-Project/kiara](https://github.com/DHARPA-Project/kiara)
 - Development documentation for this repo: [https://dharpa.org/kiara](https://dharpa.org/kiara)

## Description

*Kiara* is the data orchestration engine developed by the DHARPA project. It uses a modular approach
to let users re-use tried and tested data orchestration pipelines, as well as create new ones from existing building
blocks. It also helps you manage your research data, and augment it with automatically-, semi-automatically-, and manually-
created metadata. Most of this is not yet implemented.

## Development

### Requirements

- Python (version >=3.6 -- some make targets only work for Python >=3.7, but *kiara* itself should work on 3.6)
- pip, virtualenv
- git
- make
- [direnv](https://direnv.net/) (optional)


### Prepare development environment

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
direnv allow
make init
```

*Note*: you might want to adjust the Python version in ``.envrc`` (should not be necessary in most cases though)

### ``make`` targets

- ``init``: init development project (install project & dev dependencies into virtualenv, as well as pre-commit git hook)
- ``update-modules``: update default kiara modules package from git
- ``flake``: run *flake8* tests
- ``mypy``: run *mypy* tests
- ``test``: run unit tests
- ``docs``: create static documentation pages (under ``build/site``)
- ``serve-docs``: serve documentation pages (incl. auto-reload) for getting direct feedback when working on documentation
- ``clean``: clean build directories

For details (and other, minor targets), check the ``Makefile``.


### Running tests

``` console
> make test
# or
> make coverage
```


## Copyright & license

This project is MPL v2.0 licensed, for the license text please check the [LICENSE](/LICENSE) file in this repository.

- Copyright (c) 2021, 2022 [DHARPA project](https://dharpa.org)
- Copyright (c) 2021, 2022 Markus Binsteiner
