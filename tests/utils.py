# -*- coding: utf-8 -*-
import os

KIARA_TEST_RESOURCES = os.path.join(os.path.dirname(__file__), "resources")
PIPELINES_FOLDER = os.path.join(KIARA_TEST_RESOURCES, "pipelines")


def get_workflow_config_path(workflow_name: str):
    return os.path.join(PIPELINES_FOLDER, workflow_name)
