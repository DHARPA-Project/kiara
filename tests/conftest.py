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

from kiara import Kiara
from kiara.interfaces.python_api.batch import BatchOperation

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
def kiara() -> Kiara:

    session_id = str(uuid.uuid4())

    instance_path = os.path.join(TEMP_DIR, f"instance_{session_id}")
    kiara = Kiara.create_in_path(path=instance_path)
    return kiara


@pytest.fixture(scope="module")
def presseeded_data_store_minimal() -> Kiara:

    session_id = str(uuid.uuid4())
    instance_path = os.path.join(TEMP_DIR, f"instance_{session_id}")

    pipeline_file = os.path.join(PIPELINES_FOLDER, "table_import.json")

    kiara = Kiara.create_in_path(instance_path)

    batch_op = BatchOperation.from_file(pipeline_file, kiara=kiara)

    inputs = {
        "import_file__path": os.path.join(
            ROOT_DIR, "examples", "data", "journals", "JournalNodes1902.csv"
        )
    }

    batch_op.run(save="journal_nodes", inputs=inputs)

    return kiara


@pytest.fixture(scope="module")
def preseeded_data_store() -> Kiara:

    session_id = str(uuid.uuid4())

    instance_path = os.path.join(TEMP_DIR, f"instance_{session_id}")
    kiara = Kiara.create_in_path(instance_path)

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

    batch_op.run(inputs=inputs, save="journals_data")

    print(f"kiara data store: {kiara.data_registry.get_archive()}")

    return kiara
