# -*- coding: utf-8 -*-
import os
import uuid
from pathlib import Path

from kiara.interfaces.python_api.base_api import BaseAPI

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
TEST_RESOURCES_FOLDER = os.path.join(ROOT_DIR, "tests", "resources")


def test_archive_import_values_no_alias(api: BaseAPI):

    resources_folder = Path(TEST_RESOURCES_FOLDER)

    archive_file = resources_folder / "archives" / "nand_true.0.10.kiarchive"

    assert not api.list_all_value_ids()

    result = api.import_archive(archive_file, no_aliases=True)

    assert not result.errors

    assert len(result) == 6
    assert "b6bdc921-35d6-43e6-ba20-25e617b7e5ea" in result.keys()

    assert uuid.UUID("b6bdc921-35d6-43e6-ba20-25e617b7e5ea") in api.list_all_value_ids()

    assert ["export_test#y"] == api.list_alias_names()


def test_archive_import_values_with_alias(api: BaseAPI):

    resources_folder = Path(TEST_RESOURCES_FOLDER)

    archive_file = resources_folder / "archives" / "nand_true.0.10.kiarchive"

    assert not api.list_all_value_ids()

    result = api.import_archive(archive_file, no_aliases=False)

    assert not result.errors

    assert len(result) == 6
    assert "b6bdc921-35d6-43e6-ba20-25e617b7e5ea" in result.keys()

    assert uuid.UUID("b6bdc921-35d6-43e6-ba20-25e617b7e5ea") in api.list_all_value_ids()

    assert {"y", "export_test#y"} == set(api.list_alias_names())
