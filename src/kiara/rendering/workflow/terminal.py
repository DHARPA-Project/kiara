# -*- coding: utf-8 -*-
import typing

from kiara import Kiara, PipelineController
from kiara.module_config import ModuleConfig
from kiara.rendering.workflow import WorkflowRenderer


class TerminalRenderer(WorkflowRenderer):
    def __init__(
        self,
        config: typing.Union[ModuleConfig, typing.Mapping[str, typing.Any], str],
        workflow_id: typing.Optional[str] = None,
        module_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        controller: typing.Optional[PipelineController] = None,
        kiara: typing.Optional[Kiara] = None,
    ):

        super().__init__(
            config=config,
            workflow_id=workflow_id,
            module_config=module_config,
            controller=controller,
            kiara=kiara,
        )
