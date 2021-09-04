# -*- coding: utf-8 -*-
import os

KIARA_TEST_RESOURCES = os.path.join(os.path.dirname(__file__), "resources")
PIPELINES_FOLDER = os.path.join(KIARA_TEST_RESOURCES, "pipelines")
INVALID_PIPELINES_FOLDER = os.path.join(KIARA_TEST_RESOURCES, "invalid_pipelines")
MODULE_CONFIGS_FOLDER = os.path.join(KIARA_TEST_RESOURCES, "module_configs")


def get_workflow_config_path(workflow_name: str):
    return os.path.join(PIPELINES_FOLDER, workflow_name)


def get_module_config_path(module_config_name: str):
    return os.path.join(MODULE_CONFIGS_FOLDER, module_config_name)
