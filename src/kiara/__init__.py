# -*- coding: utf-8 -*-
# isort: skip_file

__all__ = [
    "Kiara",
    "explain",
    "KiaraModule",
    "Pipeline",
    "PipelineStructure",
    "PipelineController",
    "PipelineModule",
    "DataRegistry",
    "find_kiara_modules_under",
    "find_pipeline_base_path_for_module",
    "KiaraEntryPointItem",
    "get_version",
]
import os
import typing

from .kiara import Kiara, explain  # noqa
from .module import KiaraModule  # noqa
from .pipeline.pipeline import Pipeline  # noqa
from .pipeline.structure import PipelineStructure  # noqa
from .pipeline.controller import PipelineController  # noqa
from .pipeline.module import PipelineModule  # noqa
from .data.registry import DataRegistry  # noqa
from .utils.class_loading import (
    find_kiara_modules_under,
    find_pipeline_base_path_for_module,
    KiaraEntryPointItem,
)

try:
    builtins = __import__("__builtin__")
except ImportError:
    builtins = __import__("builtins")


try:
    from rich import inspect
    from rich import print as rich_print

    setattr(builtins, "insp", inspect)

    def dbg(
        *objects: typing.Any,
        sep: str = " ",
        end: str = "\n",
        file: typing.Optional[typing.IO[str]] = None,
        flush: bool = False,
    ):

        for obj in objects:
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
        file: typing.Optional[typing.IO[str]] = None,
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


"""Top-level package for kiara."""


__author__ = """Markus Binsteiner"""
"""The author of this package."""
__email__ = "markus@frkl.io"
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

        except (Exception):
            pass

        if __version__ is None:
            __version__ = "unknown"

    return __version__
