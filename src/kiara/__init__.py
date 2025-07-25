# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

# isort: skip_file

__all__ = [
    "get_version",
]
import logging
import os
import sys

import structlog
import typing

from .utils import is_develop, is_debug
from .utils.class_loading import (
    KiaraEntryPointItem,
    find_kiara_model_classes_under,
    find_kiara_renderers_under,
)

try:
    builtins = __import__("__builtin__")
except ImportError:
    builtins = __import__("builtins")


# =================================================================
# global init stuff

# default logging, unless set somewhere else
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING),
)
# check if run in Jupyter
if "google.colab" in sys.modules or "jupyter_client" in sys.modules:
    from kiara.interfaces import set_console_width

    set_console_width()

try:
    from rich import inspect
    from rich import print as rich_print

    setattr(builtins, "insp", inspect)

    def dbg(
        *objects: typing.Any,
        sep: str = " ",
        end: str = "\n",
        file: typing.Union[typing.IO[str], None] = None,
        flush: bool = False,
    ):
        for obj in objects:
            if hasattr(obj, "create_renderable"):
                obj = obj.create_renderable()
            try:
                rich_print(obj, sep=sep, end=end, file=file, flush=flush)
            except Exception:
                rich_print(
                    f"[green]{obj}[/green]", sep=sep, end=end, file=file, flush=flush
                )

    setattr(builtins, "dbg", dbg)

    def DBG(
        *objects: typing.Any,
        sep: str = " ",
        end: str = "\n",
        file: typing.Union[typing.IO[str], None] = None,
        flush: bool = False,
    ):
        objs = (
            ["[green]----------------------------------------------[/green]"]
            + list(objects)
            + ["[green]----------------------------------------------[/green]"]
        )
        dbg(*objs, sep=sep, end=end, file=file, flush=flush)

    setattr(builtins, "DBG", DBG)

except ImportError:  # Graceful fallback if IceCream isn't installed.
    pass

if is_develop() or is_debug():
    try:
        from icecream import install

        install()
    except ImportError:  # Graceful fallback if IceCream isn't installed.
        pass

"""Top-level package for kiara."""


__author__ = """Markus Binsteiner"""
"""The author of this package."""
__email__ = "markus@frkl.dev"
"""Email address of the author."""


KIARA_METADATA = {
    "authors": [{"name": __author__, "email": __email__}],
    "description": "Kiara Python package",
    "references": {
        "source_repo": {
            "desc": "The kiara project git repository.",
            "url": "https://github.com/DHARPA-Project/kiara",
        },
        "documentation": {
            "desc": "The url for kiara documentation.",
            "url": "https://dharpa.org/kiara_documentation/",
        },
    },
    "tags": [],
    "labels": {"package": "kiara"},
}

find_model_classes: KiaraEntryPointItem = (
    find_kiara_model_classes_under,
    "kiara.models",
)
find_model_classes_api: KiaraEntryPointItem = (
    find_kiara_model_classes_under,
    "kiara.interfaces.python_api.models",
)
find_renderer_classes: KiaraEntryPointItem = (
    find_kiara_renderers_under,
    "kiara.renderers.included_renderers",
)


def get_version() -> str:
    """Return the current version of *Kiara*."""
    from pkg_resources import DistributionNotFound, get_distribution

    try:
        # Change here if project is renamed and does not equal the package name
        dist_name = __name__
        __version__ = get_distribution(dist_name).version
    except DistributionNotFound:
        try:
            version_file = os.path.join(os.path.dirname(__file__), "version.txt")

            if os.path.exists(version_file):
                with open(version_file, encoding="utf-8") as vf:
                    __version__ = vf.read()
            else:
                __version__ = "unknown"

        except Exception:
            __version__ = "unknown"

        if __version__ is None:
            __version__ = "unknown"

    return __version__  # type: ignore
