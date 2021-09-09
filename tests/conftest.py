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
from kiara.pipeline.config import PipelineConfig

from .utils import INVALID_PIPELINES_FOLDER, MODULE_CONFIGS_FOLDER, PIPELINES_FOLDER

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
def invalid_pipeline_paths():

    result = {}
    for root, dirnames, filenames in os.walk(INVALID_PIPELINES_FOLDER, topdown=True):

        for f in filenames:
            full = os.path.join(root, f)
            if os.path.isfile(full) and f.endswith(".json"):
                result[os.path.splitext(f)[0]] = full

    return result


@pytest.fixture
def module_config_paths():

    result = {}
    for root, dirnames, filenames in os.walk(MODULE_CONFIGS_FOLDER, topdown=True):

        for f in filenames:
            full = os.path.join(root, f)
            if os.path.isfile(full) and f.endswith(".json"):
                result[os.path.splitext(f)[0]] = full

    return result


@pytest.fixture
def pipeline_configs(pipeline_paths) -> typing.Mapping[str, PipelineConfig]:

    return {
        name: PipelineConfig.parse_file(path) for name, path in pipeline_paths.items()
    }


@pytest.fixture
def kiara() -> Kiara:

    kiara = Kiara()
    return kiara


@pytest.fixture(scope="module")
def presseeded_data_store():

    session_id = str(uuid.uuid4())

    instance_data_store = os.path.join(TEMP_DIR, f"instance_{session_id}")
    conf = KiaraConfig(data_store=instance_data_store)

    pipeline_file = os.path.join(PIPELINES_FOLDER, "table_import.json")

    kiara = Kiara(config=conf)
    kiara.register_pipeline_description(pipeline_file, module_type_name="preseed")
    results = kiara.run(
        "preseed",
        inputs={
            "import_file__source": os.path.join(
                ROOT_DIR, "examples", "data", "journals", "JournalNodes1902.csv"
            )
        },
    )

    table_value = results.get_value_obj("create_table_from_files__value_item")
    table_value.save(aliases=["journal_nodes"])

    return kiara
