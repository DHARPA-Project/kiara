# Installation

## Python package

The python package is currently not available on [pypi](https://pypi.org), so for now you have to install the package directly from the git repo. If you chooose this install method, I assume you know how to install Python packages manually, which is why I only show you an example way of getting *kiara* onto your machine:

``` console
> python3 -m venv ~/.venvs/kiara
> source ~/.venvs/kiara/bin/activate
> pip install git+https://github.com/DHARPA-Project/kiara.git
...
...
...
Successfully installed ... ... ...
> kiara --help
Usage: kiara [OPTIONS] COMMAND [ARGS]...
   ...
   ...
```

In addition to the ``kiara`` package, you'll need a package containing modules, most likely ``kiara_modules.default``:

``` console
> pip install git+https://github.com/DHARPA-Project/kiara_modules.default.git
```
