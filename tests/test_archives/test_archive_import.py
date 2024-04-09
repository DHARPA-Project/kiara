# -*- coding: utf-8 -*-
import os
import uuid
from pathlib import Path

from kiara.interfaces.python_api.base_api import BaseAPI

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
TEST_RESOURCES_FOLDER = os.path.join(ROOT_DIR, "tests", "resources")

VALUE_ID = "4c929a5b-c91a-449e-9d5f-f21124298ea7"


def test_archive_import_values_no_alias(api: BaseAPI):

    resources_folder = Path(TEST_RESOURCES_FOLDER)

    archive_file = resources_folder / "archives" / "nand_true.0.10.kiarchive"

    assert not api.list_all_value_ids()

    result = api.import_archive(archive_file, no_aliases=True)

    assert not result.errors

    assert len(result) == 6
    assert VALUE_ID in result.keys()

    assert uuid.UUID(VALUE_ID) in api.list_all_value_ids()

    assert ["nand_true.0.10#y"] == api.list_alias_names()


def test_archive_import_values_with_alias(api: BaseAPI):

    resources_folder = Path(TEST_RESOURCES_FOLDER)

    archive_file = resources_folder / "archives" / "nand_true.0.10.kiarchive"

    assert not api.list_all_value_ids()

    result = api.import_archive(archive_file, no_aliases=False)

    assert not result.errors

    assert len(result) == 6
    assert VALUE_ID in result.keys()

    assert uuid.UUID(VALUE_ID) in api.list_all_value_ids()

    assert {"y", "nand_true.0.10#y"} == set(api.list_alias_names())
