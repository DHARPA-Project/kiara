# -*- coding: utf-8 -*-

import os

from kiara import Kiara

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))


def test_pipeline_notebook_rendering(kiara: Kiara):

    pipeline = os.path.join(ROOT_DIR, "resources", "pipelines", "logic", "logic_3.json")
    # pipeline = "/home/markus/projects/dharpa/kiara-playground/examples/streamlit/geolocation_prototype/pipelines/geolocation_1.yml"
    # template = os.path.join(KIARA_RESOURCES_FOLDER, "templates", "python_script.j2")
    template = "notebook"

    rendered = kiara.template_mgmt.render(
        "pipeline", module=pipeline, template=template
    )

    assert rendered
    # TODO: test notebook content
