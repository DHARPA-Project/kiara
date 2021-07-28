# -*- coding: utf-8 -*-
import typing
import uuid

from kiara import Kiara
from kiara.interfaces.python_api import ApiController
from kiara.module_config import ModuleInstanceConfig
from kiara.workflow.kiara_workflow import KiaraWorkflow


class WorkflowSessionMgmt(object):
    def __init__(self, kiara: Kiara):

        self._kiara: Kiara = kiara
        self._workflows: typing.Dict[str, KiaraWorkflow] = {}

    def add_session(
        self,
        config: typing.Union[
            ModuleInstanceConfig, typing.Mapping[str, typing.Any], str
        ],
        module_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        session_id: typing.Optional[str] = None,
    ) -> str:

        if session_id is None:
            session_id = str(uuid.uuid4())

        if session_id in self._workflows.keys():
            raise Exception(
                f"Can't add session: workflow session with id '{session_id}' already exists."
            )

        controller = ApiController()
        workflow = self._kiara.create_workflow(
            config=config,
            workflow_id=session_id,
            module_config=module_config,
            controller=controller,
        )

        self._workflows[session_id] = workflow

        return session_id

    def get_session(self, workflow_id: str) -> KiaraWorkflow:

        if workflow_id not in self._workflows.keys():
            raise Exception(
                f"Can't retrieve workflow: no workflow with id '{workflow_id}' registered."
            )

        return self._workflows[workflow_id]
