#!/usr/bin/env python
# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

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
from kiara.data.onboarding.batch import BatchOnboard
from kiara.kiara import Kiara
from kiara.pipeline.config import PipelineConfig

from .utils import INVALID_PIPELINES_FOLDER, MODULE_CONFIGS_FOLDER, PIPELINES_FOLDER

TEMP_DIR = os.path.join(tempfile.gettempdir(), "kiara_tests")
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

DATA_FOLDER = os.path.join(ROOT_DIR, "examples", "data")


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

    session_id = str(uuid.uuid4())

    instance_data_store = os.path.join(TEMP_DIR, f"instance_{session_id}")
    conf = KiaraConfig(data_store=instance_data_store)

    kiara = Kiara(config=conf)
    return kiara


@pytest.fixture(scope="module")
def presseeded_data_store_minimal() -> Kiara:

    session_id = str(uuid.uuid4())

    instance_data_store = os.path.join(TEMP_DIR, f"instance_{session_id}")
    conf = KiaraConfig(data_store=instance_data_store)

    pipeline_file = os.path.join(PIPELINES_FOLDER, "table_import.json")

    kiara = Kiara(config=conf)
    kiara.register_pipeline_description(pipeline_file, module_type_name="preseed")
    results = kiara.run(
        "preseed",
        inputs={
            "import_file__file_path": os.path.join(
                ROOT_DIR, "examples", "data", "journals", "JournalNodes1902.csv"
            )
        },
    )

    table_value = results.get_value_obj("create_table_from_files__table")
    table_value.save(aliases=["journal_nodes"])

    return kiara


@pytest.fixture(scope="module")
def preseeded_data_store() -> Kiara:

    session_id = str(uuid.uuid4())

    instance_data_store = os.path.join(TEMP_DIR, f"instance_{session_id}")
    conf = KiaraConfig(data_store=instance_data_store)
    kiara = Kiara(config=conf)

    pipeline_folder = PIPELINES_FOLDER

    inputs = {
        "edges_file_path": os.path.join(DATA_FOLDER, "journals/JournalEdges1902.csv"),
        "nodes_file_path": os.path.join(DATA_FOLDER, "journals/JournalNodes1902.csv"),
        "journals_folder_path": os.path.join(DATA_FOLDER, "journals"),
        "text_corpus_folder_path": os.path.join(DATA_FOLDER, "text_corpus"),
        "city_column_name": "City",
        "label_column_name": "Label",
    }
    pipeline = os.path.join(pipeline_folder, "test_preseed_1.yaml")

    store_config_dict = {"outputs": [{"alias_template": "{{ field_name }}"}]}

    onboard_config = {
        "module_type": pipeline,
        "inputs": inputs,
        "store_config": store_config_dict,
    }

    # store_config = ValueStoreConfig(**store_config_dict)
    onboarder = BatchOnboard.create(kiara=kiara, **onboard_config)
    print(f"kiara data store: {kiara.data_store.base_path}")

    results = onboarder.run("tests_1")
    aliases = set()
    for _a in results.values():
        aliases.update(_a)
    print(f"Onboarded example data, available aliases: {', '.join(aliases)}")

    return kiara
