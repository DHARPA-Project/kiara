# -*- coding: utf-8 -*-
import os

import click

from kiara import Kiara
from kiara.defaults import KIARA_RESOURCES_FOLDER
from kiara.rendering import JinjaPipelineRenderer


@click.group("dev")
@click.pass_context
def dev_group(ctx):
    """Development helpers."""


@dev_group.command("test")
@click.pass_context
def test(ctx):

    kiara_obj: Kiara = ctx.obj["kiara"]

    pipeline = "/home/markus/projects/dharpa/kiara-playground/examples/streamlit/geolocation_prototype/pipelines/geolocation_1.yml"

    template = os.path.join(KIARA_RESOURCES_FOLDER, "templates", "python_script.py.j2")
    rendered = kiara_obj.template_mgmt.render(
        "pipeline_notebook", module=pipeline, template=template
    )

    print(rendered)

    # inputs = {
    #     "edges_file_path": "/home/markus/projects/dharpa/kiara-playground/examples/data/journals/JournalEdges1902.csv",
    #     "nodes_file_path": "/home/markus/projects/dharpa/kiara-playground/examples/data/journals/JournalNodes1902.csv"
    # }
    # pipeline = "/home/markus/projects/dharpa/kiara.streamlit/apps/component_gallery/pipelines/gallery_onboarding.yaml"
    #
    # store_config_dict = {
    #     "outputs": [
    #         {"alias_template": "{{ base_id }}__{{ field_name }}"}
    #     ]
    # }
    #
    # onboard_config = {
    #     "module_type": pipeline,
    #     "inputs": inputs,
    #     "store_config": store_config_dict
    # }
    #
    # # store_config = ValueStoreConfig(**store_config_dict)
    # onboarder = BatchOnboard.create(kiara=kiara_obj, **onboard_config)
    # results = onboarder.run("test")
    # dbg(results)
