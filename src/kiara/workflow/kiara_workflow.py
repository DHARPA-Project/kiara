# -*- coding: utf-8 -*-
import typing
from rich import box
from rich.console import Console, ConsoleOptions, RenderResult
from rich.table import Table
from slugify import slugify

from kiara.data import ValueSet
from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.info.pipelines import PipelineState, StepsInfo
from kiara.module_config import ModuleConfig
from kiara.pipeline import StepStatus
from kiara.pipeline.module import PipelineModule
from kiara.pipeline.pipeline import Pipeline, PipelineController, PipelineStructure

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara


class KiaraWorkflow(object):
    """A thin wrapper class around a [PipelineModule][kiara.pipeline.PipelineModule], mostly handling initialization from simplified configuration data."""

    def __init__(
        self,
        workflow_id: str,
        config: ModuleConfig,
        kiara: "Kiara",
        controller: typing.Optional[PipelineController] = None,
    ):

        self._controller: typing.Optional[PipelineController] = controller
        self._workflow_id: str = workflow_id
        self._workflow_config: ModuleConfig = config
        self._kiara: Kiara = kiara

        root_module_args: typing.Dict[str, typing.Any] = {"id": self._workflow_id}
        if self._workflow_config.module_type == "pipeline":
            root_module_args["module_type"] = "pipeline"
            root_module_args["module_config"] = self._workflow_config.module_config
        elif self._kiara.is_pipeline_module(self._workflow_config.module_type):
            root_module_args["module_type"] = self._workflow_config.module_type
            root_module_args["module_config"] = self._workflow_config.module_config
        else:
            # means it's a python module, and we wrap it into a single-module pipeline
            root_module_args["module_type"] = "pipeline"
            steps_conf = {
                "steps": [
                    {
                        "module_type": self._workflow_config.module_type,
                        "step_id": slugify(
                            self._workflow_config.module_type, separator="_"
                        ),
                        "module_config": self._workflow_config.module_config,
                    }
                ],
                "input_aliases": "auto",
                "output_aliases": "auto",
            }
            root_module_args["module_config"] = steps_conf

        self._root_module: PipelineModule = self._kiara.create_module(**root_module_args)  # type: ignore
        assert isinstance(self._root_module, PipelineModule)
        self._pipeline: typing.Optional[Pipeline] = None

    @property
    def structure(self) -> PipelineStructure:
        return self._root_module.structure

    @property
    def pipeline(self) -> Pipeline:

        if self._pipeline is None:
            self._pipeline = Pipeline(self.structure, controller=self._controller)
        return self._pipeline

    @property
    def controller(self) -> PipelineController:
        return self.pipeline.controller

    @property
    def status(self) -> StepStatus:
        return self.pipeline.status

    @property
    def inputs(self) -> ValueSet:
        return self.pipeline.inputs

    @property
    def outputs(self) -> ValueSet:
        return self.pipeline.outputs

    def get_current_state(self) -> PipelineState:
        return self.pipeline.get_current_state()

    @property
    def current_state(self) -> PipelineState:
        return self.get_current_state()

    # @inputs.setter
    # def inputs(self, inputs: typing.Mapping[str, typing.Any]):
    #     self.pipeline.set_pipeline_inputs(**inputs)

    @property
    def input_names(self) -> typing.List[str]:
        return list(self.inputs.get_all_field_names())

    @property
    def output_names(self) -> typing.List[str]:
        return list(self.outputs.get_all_field_names())

    @property
    def workflow_id(self) -> str:
        return self._workflow_id

    @property
    def steps(self) -> StepsInfo:
        return self.pipeline.structure.to_details().steps_info

    def __repr__(self):

        return f"{self.__class__.__name__}(workflow_id={self.workflow_id}, root_module={self._root_module})"

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        yield f"[b]Workflow: {self.workflow_id}"

        doc = self._root_module.get_type_metadata().documentation.description
        if doc and doc != DEFAULT_NO_DESC_VALUE:
            yield f"\n{doc}\n"

        table = Table(box=box.SIMPLE, show_header=False)
        table.add_column("property", style="i")
        table.add_column("value")
        doc_link = self._root_module.get_type_metadata().context.references.get(
            "documentation", None
        )
        if doc_link:
            # TODO: use direct link
            url = doc_link.url
            module_str = f"[link={url}]{self._root_module._module_type_id}[/link]"  # type: ignore
        else:
            module_str = self._root_module._module_type_id  # type: ignore
        table.add_row("root module", module_str)
        table.add_row("current status", self.status.name)
        inputs_table = self.inputs._create_rich_table(show_headers=True)
        table.add_row("inputs", inputs_table)
        outputs_table = self.outputs._create_rich_table(show_headers=True)
        table.add_row("outputs", outputs_table)

        yield table
