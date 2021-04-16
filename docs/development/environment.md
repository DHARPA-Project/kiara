# Development environment

This page describes how to setup a development environment for *Kiara*.

## Check out *Kiara* source code

```console
git clone https://github.com/DHARPA-Project/kiara.git
```

## Python virtualenv/conda-env

Now create and activate a Python virtualenv in whatever way you usually do, for example:

```console
cd kiara
python3 -m venv .venv
source .venv/bin/activate
```

## (Re-)Initialize the project environment

The *Kiara* project contains a [``Makefile``](https://github.com/DHARPA-Project/kiara/blob/develop/Makefile) that helps with some often used
development tasks. One such is the initialization (or re-initialization) of
the *Kiara* development virtualenv, which in this case means installing the project, its runtime- and development dependencies into your virtualenv:

```console
make init
```

## Add the ``kiara_modules.default`` project dependency

If you plan to also work on the [*Kiara* default modules](https://github.com/DHARPA-Project/kiara_modules.default), check out that git repository and add it as dependency to your virtualenv:

```console
git clone https://github.com/DHARPA-Project/kiara_modules.default.git ../kiara_modules.default
pip install -e '../kiara_modules.default[all]'
```
