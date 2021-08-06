#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Dummy conftest.py for kiara.

    If you don't know what this is for, just leave it empty.
    Read more about conftest.py under:
    https://pytest.org/latest/plugins.html
"""
import pytest

import os
import typing

from kiara.kiara import Kiara
from kiara.pipeline.config import PipelineModuleConfig

from .utils import PIPELINES_FOLDER


@pytest.fixture
def pipeline_paths():

    result = {}
    for root, dirnames, filenames in os.walk(PIPELINES_FOLDER, topdown=True):

        for f in filenames:
            full = os.path.join(root, f)
            if os.path.isfile(full) and f.endswith(".json"):
                result[os.path.splitext(f)[0]] = full

    return result


@pytest.fixture
def pipeline_configs(pipeline_paths) -> typing.Mapping[str, PipelineModuleConfig]:

    return {
        name: PipelineModuleConfig.parse_file(path)
        for name, path in pipeline_paths.items()
    }


@pytest.fixture
def kiara() -> Kiara:

    kiara = Kiara()
    return kiara
