# -*- coding: utf-8 -*-
import os
import uuid
from pathlib import Path

from kiara.interfaces.python_api.base_api import BaseAPI

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
TEST_RESOURCES_FOLDER = os.path.join(ROOT_DIR, "tests", "resources")

VALUE_ID = "edbd6711-0432-430f-a147-08a6ae9df220"


def test_archive_import_values_no_alias(api: BaseAPI):

    resources_folder = Path(TEST_RESOURCES_FOLDER)
    if not resources_folder.exists():
        parent = resources_folder.parent.exists()
        parent_parent = resources_folder.parent.parent.exists()
        parent_parent_parent = resources_folder.parent.parent.parent.exists()
        raise FileNotFoundError(
            f"Resources folder not found: {resources_folder} - {parent} - {parent_parent} - {parent_parent_parent}"
        )

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
