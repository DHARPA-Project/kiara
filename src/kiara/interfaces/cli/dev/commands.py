# -*- coding: utf-8 -*-
import click

from kiara import Kiara


@click.group("dev")
@click.pass_context
def dev_group(ctx):
    """Development helpers."""


@dev_group.command("test")
@click.pass_context
def test(ctx):

    kiara_obj: Kiara = ctx.obj["kiara"]
    print(kiara_obj)

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
