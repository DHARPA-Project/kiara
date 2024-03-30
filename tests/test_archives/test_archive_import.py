# -*- coding: utf-8 -*-
import os
import uuid
from pathlib import Path

from kiara.interfaces.python_api.base_api import BaseAPI

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
TEST_RESOURCES_FOLDER = os.path.join(ROOT_DIR, "tests", "resources")


def test_archive_import_values_no_alias(api: BaseAPI):

    resources_folder = Path(TEST_RESOURCES_FOLDER)

    archive_file = resources_folder / "archives" / "export_test.kiarchive"

    assert not api.list_all_value_ids()

    result = api.import_archive(archive_file, no_aliases=True)

    assert len(result) == 8
    assert "af83495c-9fbf-4155-a9ce-29f1e8be4da9" in result.keys()

    assert uuid.UUID("af83495c-9fbf-4155-a9ce-29f1e8be4da9") in api.list_all_value_ids()

    assert ["export_test#y"] == api.list_alias_names()


def test_archive_import_values_with_alias(api: BaseAPI):

    resources_folder = Path(TEST_RESOURCES_FOLDER)

    archive_file = resources_folder / "archives" / "export_test.kiarchive"

    assert not api.list_all_value_ids()

    result = api.import_archive(archive_file, no_aliases=False)

    assert len(result) == 8
    assert "af83495c-9fbf-4155-a9ce-29f1e8be4da9" in result.keys()

    assert uuid.UUID("af83495c-9fbf-4155-a9ce-29f1e8be4da9") in api.list_all_value_ids()

    assert ["export_test#y", "y"] == api.list_alias_names()
