# -*- coding: utf-8 -*-
import typing
from rich import box
from rich.console import Console, ConsoleOptions, RenderResult
from rich.table import Table

from kiara.config import KiaraWorkflowConfig
from kiara.data.values import ValueSet
from kiara.pipeline.controller import PipelineController
from kiara.pipeline.module import PipelineModule
from kiara.pipeline.pipeline import Pipeline, PipelineState, StepStatus
from kiara.pipeline.structure import PipelineStructure

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara


class KiaraWorkflow(object):
    """A thin wrapper class around a [PipelineModule][kiara.pipeline.PipelineModule], mostly handling initialization from simplified configuration data."""

    def __init__(
        self,
        workflow_id: str,
        config: KiaraWorkflowConfig,
        kiara: "Kiara",
        controller: typing.Optional[PipelineController] = None,
    ):

        self._controller: typing.Optional[PipelineController] = controller
        self._workflow_id: str = workflow_id
        self._workflow_config: KiaraWorkflowConfig = config
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
                        "step_id": self._workflow_config.module_type,
                        "module_config": self._workflow_config.module_config,
                    }
                ]
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
    def status(self) -> StepStatus:
        return self.pipeline.status

    @property
    def inputs(self) -> ValueSet:
        return self.pipeline.inputs

    def get_current_state(self) -> PipelineState:
        return self.pipeline.get_current_state()

    # @inputs.setter
    # def inputs(self, inputs: typing.Mapping[str, typing.Any]):
    #     self.pipeline.set_pipeline_inputs(**inputs)

    @property
    def outputs(self) -> ValueSet:
        return self.pipeline.outputs

    @property
    def input_names(self) -> typing.List[str]:
        return list(self.inputs.keys())

    @property
    def output_names(self) -> typing.List[str]:
        return list(self.outputs.keys())

    @property
    def workflow_id(self) -> str:
        return self._workflow_id

    def __repr__(self):

        return f"{self.__class__.__name__}(workflow_id={self.workflow_id}, root_module={self._root_module})"

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        yield f"[b]Workflow: {self.workflow_id}"

        doc = self._root_module.doc()
        if doc and doc != "-- n/a --":
            yield f"\n{self._root_module.doc()}\n"

        table = Table(box=box.SIMPLE, show_header=False)
        table.add_column("property", style="i")
        table.add_column("value")
        doc_link = self._root_module.doc_link()
        if doc_link:
            module_str = f"[link={doc_link}]{self._root_module._module_type_id}[/link]"  # type: ignore
        else:
            module_str = self._root_module._module_type_id  # type: ignore
        table.add_row("root module", module_str)
        table.add_row("current status", self.status.name)
        inputs_table = self.inputs._create_rich_table(show_headers=True)
        table.add_row("inputs", inputs_table)
        outputs_table = self.outputs._create_rich_table(show_headers=True)
        table.add_row("outputs", outputs_table)

        yield table
