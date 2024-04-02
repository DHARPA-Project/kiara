# -*- coding: utf-8 -*-
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Literal,
    Mapping,
    MutableMapping,
    Set,
    Type,
    Union,
)

from jinja2 import Template
from pydantic import Field

from kiara.defaults import KIARA_DEFAULT_STAGES_EXTRACTION_TYPE
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

    # _render_profiles: Mapping[str, Mapping[str, Any]] = {"html": {}}

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

        inputs = render_config.model_dump()
        inputs["pipeline"] = instance
        return inputs


class PipelineRendererMarkdown(BaseJinjaRenderer[Type[Pipeline], RenderInputsSchema]):
    """Renders a pipeline structure as static html page."""

    _renderer_name = "pipeline_markdown"

    # _render_profiles: Mapping[str, Mapping[str, Any]] = {"html": {}}

    def retrieve_supported_render_sources(cls) -> str:
        return "pipeline"

    def retrieve_supported_render_targets(self) -> Union[Set[str], str]:
        return "markdown"

    def retrieve_source_transformers(self) -> Iterable[SourceTransformer]:
        return [PipelineTransformer(kiara=self._kiara)]

    def retrieve_jinja_env(self) -> JinjaEnv:

        jinja_env = JinjaEnv(template_base="kiara")
        return jinja_env

    def get_template(self, render_config: RenderInputsSchema) -> Template:

        return self.get_jinja_env().get_template("pipeline/markdown/pipeline.md.j2")

    def assemble_render_inputs(
        self, instance: Any, render_config: RenderInputsSchema
    ) -> Mapping[str, Any]:

        inputs = render_config.model_dump()
        inputs["pipeline"] = instance
        return inputs


class PipelineRendererPngConfig(KiaraRendererConfig):

    graph_type: Literal["execution", "data-flow", "data-flow-simple", "stages"] = Field(
        description="The type of graph to render."
    )


class PipelineInputsSchema(RenderInputsSchema):

    stages_extraction_type: str = Field(
        description="The type of stages extraction to use.",
        default=KIARA_DEFAULT_STAGES_EXTRACTION_TYPE,
    )


class PipelineRendererPng(
    KiaraRenderer[Pipeline, PipelineInputsSchema, bytes, PipelineRendererPngConfig]
):

    _renderer_name = "pipeline_png"
    _renderer_config_cls = PipelineRendererPngConfig  # type: ignore
    _inputs_schema = PipelineInputsSchema

    _renderer_profiles: Mapping[str, Mapping[str, Any]] = {
        "execution-graph-image": {"graph_type": "execution"},
        "data-flow-graph-image": {"graph_type": "data-flow"},
        "data-flow-simple-graph-image": {"graph_type": "data-flow-simple"},
        "stages-graph-image": {"graph_type": "stages"},
    }

    def retrieve_doc(self) -> Union[str, None]:

        graph_type = self.renderer_config.graph_type

        if graph_type == "execution":
            graph = "the execution graph"
        elif graph_type == "data-flow":
            graph = "the data flow graph"
        elif graph_type == "data-flow-simple":
            graph = "a simplified version of the data flow graph"
        elif graph_type == "stages":
            graph = "the stages graph"
        else:
            raise Exception(f"Invalid graph type: {graph_type}")

        return f"Render {graph} of the given pipeline as a image (in png format)."

    def retrieve_source_transformers(self) -> Iterable[SourceTransformer]:
        return [PipelineTransformer(kiara=self._kiara)]

    def get_renderer_alias(self) -> str:
        return f"{self.renderer_config.graph_type}_png"

    def retrieve_supported_render_sources(self) -> str:
        return "pipeline"

    def retrieve_supported_render_targets(self) -> Union[Iterable[str], str]:
        return f"{self.renderer_config.graph_type}_png"

    def _render(self, instance: Pipeline, render_config: PipelineInputsSchema) -> bytes:

        graph_type = self.renderer_config.graph_type

        if graph_type == "execution":
            graph = instance.structure.execution_graph
        elif graph_type == "data-flow":
            graph = instance.structure.data_flow_graph
        elif graph_type == "data-flow-simple":
            graph = instance.structure.data_flow_graph_simple
        elif graph_type == "stages":
            graph = instance.structure.get_stages_graph(
                stages_extraction_type=render_config.stages_extraction_type
            )
        else:
            raise Exception(f"Invalid graph type: {graph_type}")

        image = create_image(graph)
        return image


class PipelineInfoRenderer(BaseJinjaRenderer[Pipeline, RenderInputsSchema]):
    """Renders a basic text file containing pipeline details from a pipeline.

    This is mostly used for debugging and as a base template to show how this is done.
    """

    _renderer_name = "pipeline_info"

    def retrieve_supported_render_sources(self) -> str:
        return "pipeline"

    def retrieve_supported_render_targets(cls) -> Union[Iterable[str], str]:
        return "pipeline_info"

    def retrieve_source_transformers(self) -> Iterable[SourceTransformer]:
        return [PipelineTransformer(kiara=self._kiara)]

    def retrieve_jinja_env(self) -> JinjaEnv:

        jinja_env = JinjaEnv(template_base="kiara")
        return jinja_env

    def get_template(self, render_config: RenderInputsSchema) -> Template:

        return self.get_jinja_env().get_template("pipeline/pipeline_info.md.j2")

    def assemble_render_inputs(
        self, instance: Any, render_config: RenderInputsSchema
    ) -> Mapping[str, Any]:

        inputs: MutableMapping[str, Any] = render_config.model_dump()
        inputs["pipeline"] = instance
        return inputs


class PythonScriptRenderInputSchema(RenderInputsSchema):
    inputs: Dict[str, Any] = Field(
        description="The pipeline inputs.", default_factory=dict
    )


class PipelinePythonScriptRenderer(
    BaseJinjaRenderer[Pipeline, PythonScriptRenderInputSchema]
):
    """Renders a simple executable python script from a pipeline.

        If the pipeline inputs have required inputs, you can either specify those in in the render config, or you have to edit the rendered Python in the places indicted with `<TODO_SET_INPUT>` before execution.

    ## Examples

    ### Terminal

    Example invoication from the command line (using [this](https://github.com/DHARPA-Project/kiara_plugin.tabular/blob/develop/examples/pipelines/tables_from_csv_files.yaml) pipeline):

    ```
    kiara render --source-type pipeline --target-type python_script item tables_from_csv_files.yaml inputs='{"path": "/home/markus/projects/kiara/kiara_plugin.tabular/examples/data/journals"}' > tables_from_csv_files.py

    python tables_from_csv_files.py
    ```

    ### Python API

    Example usage from the Python API:

    ``` python
    from kiara.api import KiaraAPI

    kiara = KiaraAPI.instance()

    pipeline = "logic.xor"  # any valid pipeline operation (or reference to one)
    pipeline_inputs = {
        "a": True,
        "b": False,
    }
    rendered = kiara.render(pipeline, source_type="pipeline", target_type="python_script", render_config={"inputs": pipeline_inputs})
    print("# Rendered python script for pipeline 'logic.xor':")
    print(rendered)
    ```

    """

    _renderer_name = "pipeline_to_python_script"
    _inputs_schema = PythonScriptRenderInputSchema

    def retrieve_supported_render_sources(self) -> str:
        return "pipeline"

    def retrieve_supported_render_targets(cls) -> Union[Iterable[str], str]:
        return "python_script"

    def retrieve_source_transformers(self) -> Iterable[SourceTransformer]:
        return [PipelineTransformer(kiara=self._kiara)]

    def retrieve_jinja_env(self) -> JinjaEnv:

        jinja_env = JinjaEnv(template_base="kiara")
        return jinja_env

    def get_template(self, render_config: PythonScriptRenderInputSchema) -> Template:

        return self.get_jinja_env().get_template("pipeline/python_script.py.j2")

    def assemble_render_inputs(
        self, instance: Any, render_config: PythonScriptRenderInputSchema
    ) -> Mapping[str, Any]:

        from kiara.utils.rendering import create_pipeline_render_inputs

        pipeline: Pipeline = instance
        pipeline_user_inputs: Mapping[str, Any] = render_config.inputs
        result = create_pipeline_render_inputs(pipeline, pipeline_user_inputs)
        return result
