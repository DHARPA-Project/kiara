# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import mmh3
import os
import sys
import typing
from appdirs import AppDirs
from enum import Enum

kiara_app_dirs = AppDirs("kiara", "DHARPA")

if not hasattr(sys, "frozen"):
    KIARA_MODULE_BASE_FOLDER = os.path.dirname(__file__)
    """Marker to indicate the base folder for the `kiara` module."""
else:
    KIARA_MODULE_BASE_FOLDER = os.path.join(sys._MEIPASS, "kiara")  # type: ignore
    """Marker to indicate the base folder for the `kiara` module."""

KIARA_RESOURCES_FOLDER = os.path.join(KIARA_MODULE_BASE_FOLDER, "resources")
"""Default resources folder for this package."""

KIARA_DB_FILE = os.path.join(kiara_app_dirs.user_data_dir, "kiara.db")
KIARA_DB_MIGRATIONS_CONFIG = os.path.join(
    KIARA_RESOURCES_FOLDER, "database", "alembic.ini"
)
KIARA_DB_MIGRATIONS_FOLDER = os.path.join(KIARA_RESOURCES_FOLDER, "database", "kiara")
KIARA_DB_URL = f"sqlite+pysqlite:///{os.path.abspath(KIARA_DB_FILE)}"
USER_PIPELINES_FOLDER = os.path.join(kiara_app_dirs.user_config_dir, "pipelines")

KIARA_DATA_STORE_DIR = os.path.join(
    kiara_app_dirs.user_data_dir, "data-store", "default"
)
KIARA_DATA_DIR = os.path.join(KIARA_DATA_STORE_DIR, "data")
KIARA_METADATA_DIR = os.path.join(KIARA_DATA_STORE_DIR, "metadata")
KIARA_ALIASES_DIR = os.path.join(KIARA_DATA_STORE_DIR, "aliases")

MODULE_TYPE_KEY = "module_type"
"""The key to specify the type of a module."""

STEP_ID_KEY = "step_id"
"""The key to specify the step id."""

INVALID_VALUE_NAMES = [
    "kiara",
    "registry",
    "items_are_valid",
    "set_values",
    "set_value",
    "ALL",
    "all",
    "metadata",
    "value",
    "value_obj",
    "items",
    "keys",
    "values",
    "data",
    "callbacks",
    "trigger_callbacks",
    "shared_metadata",
]
"""List of reserved names, inputs/outputs can't use those."""

PIPELINE_PARENT_MARKER = "__pipeline__"
"""Marker string in the pipeline structure that indicates a parent pipeline element."""

DEFAULT_EXCLUDE_DIRS = [".git", ".tox", ".cache"]
"""List of directory names to exclude by default when walking a folder recursively."""

DEFAULT_EXCLUDE_FILES = [".DS_Store"]
"""List of file names to exclude by default when reading folders."""

VALID_PIPELINE_FILE_EXTENSIONS = ["yaml", "yml", "json"]
"""File extensions a kiara pipeline/workflow file can have."""

MODULE_TYPE_NAME_KEY = "module_type_name"
"""The string for the module type name in a module configuration dict."""

DEFAULT_PIPELINE_PARENT_ID = "__kiara__"
"""Default parent id for pipeline objects that are not associated with a workflow."""

DEFAULT_NO_DESC_VALUE = "-- n/a --"

KIARA_MODULE_METADATA_ATTRIBUTE = "KIARA_METADATA"


class SpecialValue(Enum):

    NOT_SET = "__not_set__"
    NO_VALUE = "__no_value__"
    IGNORE = "__ignore__"


DEFAULT_PRETTY_PRINT_CONFIG = {
    "max_no_rows": 32,
    "max_row_height": 1,
    "max_cell_length": 80,
}

NO_HASH_MARKER = "--no-hash--"
"""Marker string to indicate no hash was calculated."""

NO_VALUE_ID_MARKER = "--no-value-id--"
"""Marker string to indicate no value id exists."""
DEFAULT_TO_JSON_CONFIG: typing.Mapping[str, typing.Any] = {
    "indent": 2,
}

COLOR_LIST = [
    "green",
    "blue",
    "bright_magenta",
    "dark_red",
    "gold3",
    "cyan",
    "orange1",
    "light_yellow3",
    "light_slate_grey",
    "deep_pink4",
]

KIARA_HASH_FUNCTION = mmh3.hash

VALUE_TYPES_CATEGORY_ALIAS = "value_types"
MODULE_TYPES_CATEGORY_ALIAS = "modules"
PIPELINE_TYPES_CATEGORY_ALIAS = "pipelines"
OPERATION_TYPES_CATEGORY_ALIAS = "operation_types"
OPERATIONS_CATEGORY_ALIAS = "operations"

MODULE_TYPE_CATEGORY_ALIAS = "metadata.module"
PIPELINE_TYPE_CATEGORY_ALIAS = "metadata.pipeline"
VALUE_TYPE_CATEGORY_ALIAS = "metadata.value_type"
OPERATION_TYPE_CATEGORY_ALIAS = "metadata.operation_type"
ENVIRONMENT_TYPE_CATEGORY_ALIAS = "metadata.environment"
PYDANTIC_USE_CONSTRUCT: bool = False
