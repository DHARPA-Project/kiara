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

from .kiara import Kiara, explain, pretty_print  # noqa
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

"""Top-level package for kiara."""


__author__ = """Markus Binsteiner"""
"""The author of this package."""
__email__ = "markus.binsteiner@uni.lu"
"""Email address of the author."""


KIARA_METADATA: typing.Mapping[str, typing.Any] = {"tags": [], "labels": {}}


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
