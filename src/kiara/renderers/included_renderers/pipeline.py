# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Any, Iterable, Literal, Mapping, Set, Type, Union

from jinja2 import Template
from pydantic import Field

from kiara.models.module.pipeline import PipelineConfig
from kiara.models.module.pipeline.pipeline import Pipeline
from kiara.models.module.pipeline.structure import PipelineStructure
from kiara.renderers import (
    KiaraRenderer,
    KiaraRendererConfig,
    RenderInputsSchema,
    SourceTransformer,
)
from kiara.renderers.jinja import BaseJinjaRenderer, JinjaEnv
from kiara.utils.graphs import create_image

if TYPE_CHECKING:
    from kiara.context import Kiara


class PipelineTransformer(SourceTransformer):
    def __init__(self, kiara: "Kiara"):
        self._kiara: "Kiara" = kiara
        super().__init__()

    def retrieve_supported_python_classes(self) -> Iterable[Type]:
        return [PipelineConfig, Pipeline, PipelineStructure, str, Mapping]

    def retrieve_supported_inputs_descs(self) -> Union[str, Iterable[str]]:
        return [
            "a registered pipeline operation",
            "a path to a pipeline file",
            "the pipeline configuration as a Python dict",
        ]

    def validate_and_transform(self, source: Any) -> Union[Pipeline, None]:

        if isinstance(source, Pipeline):
            return source
        elif isinstance(source, (PipelineConfig, PipelineStructure, Mapping, str)):
            pipeline = Pipeline.create_pipeline(kiara=self._kiara, pipeline=source)
            return pipeline
        else:
            return None


class PipelineRendererHtml(BaseJinjaRenderer[Type[Pipeline], RenderInputsSchema]):
    """Renders a pipeline structure as static html page."""

    _renderer_name = "pipeline_html"

    _render_profiles: Mapping[str, Mapping[str, Any]] = {"html": {}}

    def retrieve_supported_render_sources(cls) -> str:
        return "pipeline"

    def retrieve_supported_render_targets(self) -> Union[Set[str], str]:
        return "html"

    def retrieve_source_transformers(self) -> Iterable[SourceTransformer]:
        return [PipelineTransformer(kiara=self._kiara)]

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


class PipelineRendererPngConfig(KiaraRendererConfig):

    graph_type: Literal["execution", "data-flow", "data-flow-simple"] = Field(
        description="The type of graph to render.", default="execution"
    )


class PipelineRendererPng(
    KiaraRenderer[Pipeline, RenderInputsSchema, bytes, PipelineRendererPngConfig]
):

    _renderer_name = "pipeline_png"
    _renderer_config_cls = PipelineRendererPngConfig  # type: ignore

    _render_profiles: Mapping[str, Mapping[str, Any]] = {
        "execution-graph-image": {"graph_type": "execution"},
        "data-flow-graph-image": {"graph_type": "data-flow"},
        "data-flow-simple-graph-image": {"graph_type": "data-flow-simple"},
    }

    def retrieve_doc(self) -> Union[str, None]:

        graph_type = self.renderer_config.graph_type

        if graph_type == "execution":
            graph = "the execution graph"
        elif graph_type == "data-flow":
            graph = "the data flow graph"
        elif graph_type == "data-flow-simple":
            graph = "a simplified version of the data flow graph"
        else:
            raise Exception(f"Invalid graph type: {graph_type}")

        return f"Render {graph} of the given pipeline as a image (in png format)."

    def retrieve_source_transformers(self) -> Iterable[SourceTransformer]:
        return [PipelineTransformer(kiara=self._kiara)]

    def retrieve_supported_render_sources(self) -> str:
        return "pipeline"

    def retrieve_supported_render_targets(self) -> Union[Iterable[str], str]:
        return "png"

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
