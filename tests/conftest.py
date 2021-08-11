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
import tempfile
import typing
import uuid

from kiara.config import KiaraConfig
from kiara.kiara import Kiara
from kiara.pipeline.config import PipelineModuleConfig

from .utils import PIPELINES_FOLDER

TEMP_DIR = os.path.join(tempfile.gettempdir(), "kiara_tests")
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


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


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="module")
def presseeded_data_store():

    session_id = str(uuid.uuid4())

    instance_data_store = os.path.join(TEMP_DIR, f"instance_{session_id}")
    conf = KiaraConfig(data_store=instance_data_store)

    kiara = Kiara(config=conf)
    kiara.run(
        "table.import_from.file_path.string",
        inputs={
            "source": os.path.join(
                ROOT_DIR, "examples", "data", "journals", "JournalNodes1902.csv"
            ),
            "aliases": ["journal_nodes"],
        },
    )

    return kiara
