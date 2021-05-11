# Installation

There are currently two ways to install *kiara* on your machine. Via a manual binary download, or installation of the python package.

## Binaries

To install the `kiara` binary, download the appropriate binary from one of the links below, and set the downloaded file to be executable (``chmod +x kiara``):

Only snapshot binaries for now, not for production use:

  - [Linux](https://github.com/DHARPA-Project/kiara/actions/workflows/build-linux.yaml)
  - [Windows](https://github.com/DHARPA-Project/kiara/actions/workflows/build-windows.yaml)
  - [Mac OS X](https://github.com/DHARPA-Project/kiara/actions/workflows/build-darwin.yaml)


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
