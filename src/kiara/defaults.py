# -*- coding: utf-8 -*-
import os
import sys
from appdirs import AppDirs

kiara_app_dirs = AppDirs("kiara", "DHARPA")

if not hasattr(sys, "frozen"):
    KIARA_MODULE_BASE_FOLDER = os.path.dirname(__file__)
    """Marker to indicate the base folder for the `kiara` module."""
else:
    KIARA_MODULE_BASE_FOLDER = os.path.join(sys._MEIPASS, "kiara")  # type: ignore
    """Marker to indicate the base folder for the `kiara` module."""

KIARA_RESOURCES_FOLDER = os.path.join(KIARA_MODULE_BASE_FOLDER, "resources")
"""Default resources folder for this package."""

USER_PIPELINES_FOLDER = os.path.join(kiara_app_dirs.user_config_dir, "pipelines")

RELATIVE_PIPELINES_PATH = os.path.join("resources", "pipelines")

MODULE_TYPE_KEY = "module_type"
"""The key to specify the type of a module."""

STEP_ID_KEY = "step_id"
"""The key to specify the step id."""

INVALID_VALUE_NAMES = ["dict", "items_are_valid", "set_values", "set_value", "ALL"]
"""List of reserved names, inputs/outputs can't use those."""

PIPELINE_PARENT_MARKER = "__pipeline__"
"""Marker string in the pipeline structure that indicates a parent pipeline element."""

DEFAULT_EXCLUDE_DIRS = [".git", ".tox", ".cache"]
"""List of directory names to exclude by default when walking a folder recursively."""

VALID_PIPELINE_FILE_EXTENSIONS = ["yaml", "yml", "json"]
"""File extensions a kiara pipeline/workflow file can have."""

MODULE_TYPE_NAME_KEY = "module_type_name"
"""The string for the module type name in a module configuration dict."""

DEFAULT_PIPELINE_PARENT_ID = "__kiara__"
"""Default parent id for pipeline objects that are not associated with a workflow."""
