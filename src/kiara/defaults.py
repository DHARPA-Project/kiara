# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import os
import typing
import uuid
from enum import Enum
from pathlib import Path

from appdirs import AppDirs

kiara_app_dirs = AppDirs("kiara", "DHARPA")

# if getattr(sys, 'oxidized', False):
#     KIARA_MODULE_BASE_FOLDER = "xxx"
#     raise NotImplementedError()
# elif not hasattr(sys, "_MEIPASS"):
#     KIARA_MODULE_BASE_FOLDER = os.path.dirname(__file__)
#     """Marker to indicate the base folder for the `kiara` module."""
# else:
#     KIARA_MODULE_BASE_FOLDER = os.path.join(sys._MEIPASS, "kiara")  # type: ignore
#     """Marker to indicate the base folder for the `kiara` module."""

# KIARA_RESOURCES_FOLDER = os.path.join(KIARA_MODULE_BASE_FOLDER, "resources")
# """Default resources folder for this package."""

KIARA_CONFIG_FILE_NAME = "kiara.config"
KIARA_DEV_CONFIG_FILE_NAME = "dev.config"
KIARA_MAIN_CONFIG_FILE = os.path.join(
    kiara_app_dirs.user_config_dir, KIARA_CONFIG_FILE_NAME
)
KIARA_DEV_CONFIG_FILE = os.path.join(
    kiara_app_dirs.user_config_dir, KIARA_DEV_CONFIG_FILE_NAME
)
KIARA_MAIN_CONTEXTS_PATH = os.path.join(kiara_app_dirs.user_config_dir, "contexts")
KIARA_MAIN_CONTEXT_DATA_PATH = os.path.join(
    kiara_app_dirs.user_data_dir, "context_data"
)
KIARA_MAIN_CONTEXT_LOCKS_PATH = os.path.join(
    kiara_app_dirs.user_data_dir, "context_locks"
)


KIARA_DEFAULT_STAGES_EXTRACTION_TYPE = "early"

INIT_EXAMPLE_NAME = "init"

# USER_PIPELINES_FOLDER = os.path.join(kiara_app_dirs.user_config_dir, "pipelines")


MODULE_TYPE_KEY = "module_type"
"""The key to specify the type of a module."""

STEP_ID_KEY = "step_id"
"""The key to specify the step id."""

# INVALID_VALUE_NAMES = [
#     "kiara",
#     "registry",
#     "items_are_valid",
#     "set_values",
#     "set_value",
#     "ALL",
#     "all",
#     "metadata",
#     "value",
#     "value_obj",
#     "items",
#     "keys",
#     "values",
#     "data",
#     "callbacks",
#     "trigger_callbacks",
#     "shared_metadata",
# ]
INVALID_VALUE_NAMES = [
    "kiara",
    "callbacks",
]
INVALID_ALIAS_NAMES = [
    "kiara",
    "__default__",
    "alias",
    "value",
    "value_id",
    "kiarchive",
]
"""List of reserved names, inputs/outputs can't use those."""
DEFAULT_STORE_MARKER = "default_store"

DEFAULT_DATA_STORE_MARKER = "default_data_store"
"""Name for the default context data store."""

DEFAULT_METADATA_STORE_MARKER = "default_metadata_store"
"""Name for the default context metadata store."""

DEFAULT_JOB_STORE_MARKER = "default_job_store"
"""Name for the default context job store."""

DEFAULT_ALIAS_STORE_MARKER = "default_alias_store"
"""Name for the default context alias store."""

DEFAULT_WORKFLOW_STORE_MARKER = "default_workflow_store"
"""Name for the default context workflow store."""

METADATA_PROPERTY_MARKER = "metadata"
"""Name for the default context destiny store."""

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
NOT_AVAILBLE_MARKER = "-- n/a --"

KIARA_MODULE_METADATA_ATTRIBUTE = "KIARA_METADATA"

KIARA_DEFAULT_ROOT_NODE_ID = "__self__"

KIARA_SQLITE_STORE_EXTENSION = "kiara"

VALUE_ATTR_DELIMITER = "::"
VALID_VALUE_QUERY_CATEGORIES = ["data", "properties"]

CHUNK_CACHE_BASE_DIR = Path(kiara_app_dirs.user_cache_dir) / "data" / "chunks"
CHUNK_CACHE_DIR_DEPTH = 2
CHUNK_CACHE_DIR_WIDTH = 1


class SpecialValue(Enum):

    NOT_SET = "__not_set__"
    NO_VALUE = "__no_value__"


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

VOID_KIARA_ID = uuid.UUID("00000000-0000-0000-0000-000000000000")
NOT_SET_VALUE_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
NONE_VALUE_ID = uuid.UUID("00000000-0000-0000-0000-000000000002")
NONE_STORE_ID = uuid.UUID("00000000-0000-0000-0000-000000000003")
ORPHAN_PEDIGREE_OUTPUT_NAME = "__orphan__"

NO_MODULE_TYPE = "EXTERNAL_DATA"

INVALID_HASH_MARKER = ""

INVALID_SIZE_MARKER = -1
NO_SERIALIZATION_MARKER = "-- serialization not supported --"
KIARA_ROOT_TYPE_NAME = "__kiara__"

SERIALIZED_data_type_name = "serialized_data"
LOAD_CONFIG_data_type_name = "load_config"

PYDANTIC_USE_CONSTRUCT: bool = False
STRICT_CHECKS: bool = False

ANY_TYPE_NAME = "any"

DEFAULT_ENV_HASH_KEY = "default"

LOAD_CONFIG_PLACEHOLDER = "__placeholder__"

DATA_TYPE_CATEGORY_ID = "metadata.type"
DATA_TYPES_CATEGORY_ID = "data_types"
DATA_TYPE_CLASS_CATEGORY_ID = "data_type_class"

DATA_WRAP_CATEGORY_ID = "instance.datawrap"
UNOLOADABLE_DATA_CATEGORY_ID = "instance.unloadable_data"
VALUE_CATEGORY_ID = "instance.value"
VALUES_CATEGORY_ID = "instance.values"
VALUE_METADATA_CATEGORY_ID = "instance.value_metadata"

MODULE_CONFIG_SCHEMA_CATEGORY_ID = "module_config_schema"
MODULE_CONFIG_CATEGORY_ID = "module_config"
MODULE_CONFIG_METADATA_CATEGORY_ID = "metadata.module_config"

MODULE_TYPE_CATEGORY_ID = "metadata.module"
MODULE_TYPES_CATEGORY_ID = "modules"

BATCH_CONFIG_TYPE_CATEGORY_ID = "instance.batch_config"

PIPELINE_TYPE_CATEGORY_ID = "metadata.pipeline"
PIPELINE_TYPES_CATEGORY_ID = "pipelines"
PIPELINE_STEP_TYPE_CATEGORY_ID = "instance.pipeline_step"
PIPELINE_CONFIG_TYPE_CATEGORY_ID = "instance.pipeline_config"
PIPELINE_STRUCTURE_TYPE_CATEGORY_ID = "instance.pipeline_structure"

PIPELINE_STEP_DETAILS_CATEGORY_ID = "instance.pipeline_step_details"

OPERATION_TYPE_CATEGORY_ID = "metadata.operation_type"
OPERATION_TYPES_CATEGORY_ID = "operation_types"
OPERATIONS_CATEGORY_ID = "operations"
OPERATION_CATEOGORY_ID = "instance.operation"
OPERATION_CONFIG_CATEOGORY_ID = "instance.operation_config"
OPERATION_DETAILS_CATEOGORY_ID = "instance.operation_details"
OPERATION_INPUTS_SCHEMA_CATEOGORY_ID = "instance.operation_input_schema"
OPERATION_OUTPUTS_SCHEMA_CATEOGORY_ID = "instance.operation_output_schema"

ENVIRONMENT_TYPE_CATEGORY_ID = "instance.environment"
DOCUMENTATION_CATEGORY_ID = "documentation"

VALUE_SCHEMA_CATEGORY_ID = "value_schema"

JOB_CATEGORY_ID = "instance.job"
JOB_LOG_CATEGORY_ID = "job_log"

DESTINY_CATEGORY_ID = "instance.destiny"

CONTEXT_INFO_CATEGORY_ID = "info.context"

CONTEXT_METADATA_CATEOGORY_ID = "metadata.context"
AUTHORS_METADATA_CATEGORY_ID = "metadata.authors"

JOB_CONFIG_TYPE_CATEGORY_ID = "instance.job"
JOB_RECORD_TYPE_CATEGORY_ID = "instance.job_record"
VALUE_PEDIGREE_TYPE_CATEGORY_ID = "instance.value_pedigree"

FILE_MODEL_CATEOGORY_ID = "instance.model.file"
FILE_BUNDLE_MODEL_CATEOGORY_ID = "instance.model.file_bundle"

ARRAY_MODEL_CATEOGORY_ID = "instance.model.array"
TABLE_MODEL_CATEOGORY_ID = "instance.model.table"
DEFAULT_CONTEXT_NAME = "default"

KIARA_MODEL_ID_KEY = "kiara_model_id"
KIARA_MODEL_DATA_KEY = "data"
KIARA_MODEL_SCHEMA_KEY = "schema"

ENVIRONMENT_MARKER_KEY = "environment"
"""Constant string to indicate this is a metadata entry of type 'environment'."""

SYMLINK_ISSUE_MSG = """Your operating system does not support symlinks, which is a requirement for kiara to work.

You can enable developer mode to fix this issue:

- open 'Settings'
- click 'Updates & Security'
- click 'For developers'
- make sure 'Developer Mode' is turned on
- log out of your Windows session, and log back in again

For more information, please visit:
- https://dharpa.org/kiara.documentation/latest/installation/#enable-developer-mode
- https://learn.microsoft.com/en-us/windows/apps/get-started/enable-your-device-for-development
"""


OFFICIAL_KIARA_PLUGINS = [
    "core_types",
    "tabular",
    "onboarding",
    "network_analysis",
    "language_processing",
]


class CHUNK_COMPRESSION_TYPE(Enum):
    NONE = 0
    ZSTD = 1
    LZMA = 2
    LZ4 = 3


DEFAULT_CHUNK_COMPRESSION = CHUNK_COMPRESSION_TYPE.ZSTD

ARCHIVE_NAME_MARKER = "archive_name"
DATA_ARCHIVE_DEFAULT_VALUE_MARKER = "default_value"
TABLE_NAME_ARCHIVE_METADATA = "archive_metadata"
TABLE_NAME_DATA_METADATA = "data_value_metadata"
TABLE_NAME_DATA_SERIALIZATION_METADATA = "data_serialization_metadata"
TABLE_NAME_DATA_CHUNKS = "data_chunks"
TABLE_NAME_DATA_PEDIGREE = "data_value_pedigree"
TABLE_NAME_DATA_DESTINIES = "data_value_destiny"
REQUIRED_TABLES_DATA_ARCHIVE = {
    TABLE_NAME_ARCHIVE_METADATA,
    TABLE_NAME_DATA_METADATA,
    TABLE_NAME_DATA_SERIALIZATION_METADATA,
    TABLE_NAME_DATA_CHUNKS,
    TABLE_NAME_DATA_PEDIGREE,
    TABLE_NAME_DATA_DESTINIES,
}

TABLE_NAME_ALIASES = "aliases"
REQUIRED_TABLES_ALIAS_ARCHIVE = {
    TABLE_NAME_ARCHIVE_METADATA,
    TABLE_NAME_ALIASES,
}

TABLE_NAME_JOB_RECORDS = "job_records"
REQUIRED_TABLES_JOB_ARCHIVE = {
    TABLE_NAME_ARCHIVE_METADATA,
    TABLE_NAME_JOB_RECORDS,
}

TABLE_NAME_METADATA = "metadata"
TABLE_NAME_METADATA_SCHEMAS = "metadata_schemas"
TABLE_NAME_METADATA_REFERENCES = "metadata_references"
REQUIRED_TABLES_METADATA = {
    TABLE_NAME_ARCHIVE_METADATA,
    TABLE_NAME_METADATA,
    TABLE_NAME_METADATA_SCHEMAS,
    TABLE_NAME_METADATA_REFERENCES,
}


ALL_REQUIRED_TABLES = set(REQUIRED_TABLES_DATA_ARCHIVE)
ALL_REQUIRED_TABLES.update(REQUIRED_TABLES_ALIAS_ARCHIVE)
ALL_REQUIRED_TABLES.update(REQUIRED_TABLES_JOB_ARCHIVE)
ALL_REQUIRED_TABLES.update(REQUIRED_TABLES_METADATA)
