# Install development environment

## Python package

``` console
> python3 -m venv ~/.venvs/kiara
> source ~/.venvs/kiara/bin/activate
> pip install 'kiara[dev_all]'
...
...
...
Successfully installed ... ... ...
> kiara --help
Usage: kiara [OPTIONS] COMMAND [ARGS]...
   ...
   ...
```

In addition to the ``kiara`` package, you'll need to install plugin packages, for example:

``` console
> pip install kiara_plugin.core_types kiara_plugin.tabular
```


## Conda

```console
> conda create -n kiara python=3.10
> conda activate kiara
> conda install -c conda-forge -c dharpa kiara
```

And also plugin packages, like:

```console
> conda install -c conda-forge -c dharpa kiara_plugin.core_types kiara_plugin.tabular
```

Note, the conda install does not include development dependencies, so you'll need to either install those manually, or use pip:

```console
> pip install 'kiara[dev_all]'
```
