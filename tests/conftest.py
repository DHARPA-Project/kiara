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
import uuid
from pathlib import Path

from kiara import KiaraAPI
from kiara.context import Kiara
from kiara.context.config import KiaraConfig
from kiara.interfaces.python_api.batch import BatchOperation

from .utils import INVALID_PIPELINES_FOLDER, MODULE_CONFIGS_FOLDER, PIPELINES_FOLDER

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

DATA_FOLDER = os.path.join(ROOT_DIR, "examples", "data")


def create_temp_dir():
    session_id = str(uuid.uuid4())
    TEMP_DIR = Path(os.path.join(tempfile.gettempdir(), "kiara_tests"))

    instance_path = os.path.join(
        TEMP_DIR.resolve().absolute(), f"instance_{session_id}"
    )
    return instance_path


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
def kiara() -> Kiara:

    instance_path = create_temp_dir()
    kc = KiaraConfig.create_in_folder(instance_path)
    kiara = kc.create_context()
    return kiara


@pytest.fixture
def api() -> KiaraAPI:

    instance_path = create_temp_dir()
    kc = KiaraConfig.create_in_folder(instance_path)
    api = KiaraAPI(kc)
    return api


@pytest.fixture(scope="module")
def presseeded_data_store_minimal() -> Kiara:

    instance_path = create_temp_dir()

    pipeline_file = os.path.join(PIPELINES_FOLDER, "table_import.json")

    kc = KiaraConfig.create_in_folder(instance_path)

    kiara = kc.create_context()

    batch_op = BatchOperation.from_file(pipeline_file, kiara=kiara)

    inputs = {
        "import_file__path": os.path.join(
            ROOT_DIR, "examples", "data", "journals", "JournalNodes1902.csv"
        )
    }

    batch_op.run(inputs=inputs, save="preseed_minimal")

    return kiara


@pytest.fixture(scope="module")
def preseeded_data_store() -> Kiara:

    instance_path = create_temp_dir()
    kc = KiaraConfig.create_in_folder(instance_path)
    kiara = kc.create_context()

    pipeline = os.path.join(PIPELINES_FOLDER, "test_preseed_1.yaml")
    batch_op = BatchOperation.from_file(pipeline, kiara=kiara)

    inputs = {
        "edges_file_path": os.path.join(DATA_FOLDER, "journals/JournalEdges1902.csv"),
        "nodes_file_path": os.path.join(DATA_FOLDER, "journals/JournalNodes1902.csv"),
        "journals_folder_path": os.path.join(DATA_FOLDER, "journals"),
        "text_corpus_folder_path": os.path.join(DATA_FOLDER, "text_corpus"),
        "city_column_name": "City",
        "label_column_name": "Label",
    }

    batch_op.run(inputs=inputs, save="preseed")

    print(f"kiara data store: {kiara.data_registry.get_archive()}")

    return kiara
