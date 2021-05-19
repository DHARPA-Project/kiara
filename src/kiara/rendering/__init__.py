# -*- coding: utf-8 -*-
import typing

from kiara import Kiara, PipelineController
from kiara.config import KiaraWorkflowConfig
from kiara.data import ValueSet
from kiara.workflow import KiaraWorkflow


class WorkflowRenderer(object):
    def __init__(
        self,
        config: typing.Union[KiaraWorkflowConfig, typing.Mapping[str, typing.Any], str],
        workflow_id: typing.Optional[str] = None,
        module_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        controller: typing.Optional[PipelineController] = None,
        kiara: typing.Optional[Kiara] = None,
    ):

        if kiara is None:
            kiara = Kiara.instance()
        self._kiara = kiara
        self._config: typing.Union[
            KiaraWorkflowConfig, typing.Mapping[str, typing.Any], str
        ] = config
        self._workflow_id: typing.Optional[str] = workflow_id
        self._module_config: typing.Optional[
            typing.Mapping[str, typing.Any]
        ] = module_config
        self._controller: typing.Optional[PipelineController] = controller
        self._workflow: typing.Optional[KiaraWorkflow] = None

    @property
    def workflow(self) -> KiaraWorkflow:
        if self._workflow is None:
            self._workflow = self._kiara.create_workflow(
                config=self._config,
                workflow_id=self._workflow_id,
                module_config=self._module_config,
                controller=self._controller,
            )
        return self._workflow

    @property
    def inputs(self) -> ValueSet:
        return self.workflow.pipeline.inputs

    @property
    def outputs(self) -> ValueSet:
        return self.workflow.pipeline.outputs
