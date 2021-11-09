# -*- coding: utf-8 -*-
import click


@click.group("dev")
@click.pass_context
def dev_group(ctx):
    """Development helpers."""


# @dev_group.command("test")
# @click.pass_context
# def test(ctx):
#
#     kiara_obj: Kiara = ctx.obj["kiara"]
#
#     pipeline = "/home/markus/projects/dharpa/kiara-playground/examples/streamlit/geolocation_prototype/pipelines/geolocation_1.yml"
#
#     template = os.path.join(KIARA_RESOURCES_FOLDER, "templates", "python_script.py.j2")
#     rendered = kiara_obj.template_mgmt.render(
#         "pipeline_notebook", module=pipeline, template=template
#     )
#
#     print(rendered)
