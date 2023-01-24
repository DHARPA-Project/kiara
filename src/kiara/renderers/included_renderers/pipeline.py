# -*- coding: utf-8 -*-
from typing import Any, Iterable, Literal, Mapping, Type

from jinja2 import Template
from pydantic import Field

from kiara.models.module.pipeline.pipeline import Pipeline
from kiara.renderers import KiaraRenderer, RenderInputsSchema
from kiara.renderers.jinja import BaseJinjaRenderer, JinjaEnv
from kiara.utils.graphs import create_image


class PipelineRendererHtml(BaseJinjaRenderer[Type[Pipeline], RenderInputsSchema]):

    _renderer_name = "pipeline_to_html"

    _render_profiles: Mapping[str, Mapping[str, Any]] = {"html": {}}

    @classmethod
    def retrieve_supported_render_source(cls) -> str:
        return "pipeline"

    @classmethod
    def retrieve_supported_python_classes(cls) -> Iterable[Type]:
        return [Pipeline]

    def retrieve_jinja_env(self) -> JinjaEnv:

        jinja_env = JinjaEnv(template_base="kiara")
        return jinja_env

    def get_template(self, render_config: RenderInputsSchema) -> Template:

        return self.get_jinja_env().get_template("pipeline/static_page/page.html.j2")

    def assemble_render_inputs(
        self, instance: Any, render_config: RenderInputsSchema
    ) -> Mapping[str, Any]:

        inputs = render_config.dict()
        inputs["pipeline"] = instance
        return inputs


class PipelineRendererPngConfig(RenderInputsSchema):

    graph_type: Literal["execution", "data-flow", "data-flow-simple"] = Field(
        description="The type of graph to render.", default="execution"
    )


class PipelineRendererPng(KiaraRenderer):

    _renderer_name = "pipeline_to_png"
    _renderer_config_cls = PipelineRendererPngConfig  # type: ignore

    _render_profiles: Mapping[str, Mapping[str, Any]] = {
        "execution-graph-image": {"graph_type": "execution"},
        "data-flow-graph-image": {"graph_type": "data-flow"},
        "data-flow-simple-graph-image": {"graph_type": "data-flow-simple"},
    }

    @classmethod
    def retrieve_supported_render_source(cls) -> str:
        return "pipeline"

    @classmethod
    def retrieve_supported_python_classes(self) -> Iterable[Type]:
        return [Pipeline]

    def _render(self, instance: Pipeline, render_config: RenderInputsSchema) -> bytes:

        graph_type = self.renderer_config.graph_type

        if graph_type == "execution":
            graph = instance.structure.execution_graph
        elif graph_type == "data-flow":
            graph = instance.structure.data_flow_graph
        elif graph_type == "data-flow-simple":
            graph = instance.structure.data_flow_graph_simple
        else:
            raise Exception(f"Invalid graph type: {graph_type}")

        image = create_image(graph)
        return image
