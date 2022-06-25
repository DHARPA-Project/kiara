# -*- coding: utf-8 -*-
import orjson
import shutil
import uuid
from pathlib import Path
from typing import Dict, Mapping, Union

from kiara.models.workflow import WorkflowDetails, WorkflowState, WorkflowStateFilter
from kiara.registries import ARCHIVE_CONFIG_CLS, FileSystemArchiveConfig
from kiara.registries.workflows import WorkflowArchive, WorkflowStore


class FileSystemWorkflowArchive(WorkflowArchive):

    _archive_type_name = "filesystem_workflow_archive"
    _config_cls = FileSystemArchiveConfig

    def __init__(self, archive_id: uuid.UUID, config: ARCHIVE_CONFIG_CLS):

        super().__init__(archive_id=archive_id, config=config)

        self._base_path: Union[Path, None] = None
        self.alias_store_path.mkdir(parents=True, exist_ok=True)

    @property
    def workflow_store_path(self) -> Path:

        if self._base_path is not None:
            return self._base_path

        self._base_path = Path(self.config.archive_path).absolute()  # type: ignore
        self._base_path.mkdir(parents=True, exist_ok=True)
        return self._base_path

    @property
    def alias_store_path(self) -> Path:

        return self.workflow_store_path / "aliases"

    def _delete_archive(self):
        shutil.rmtree(self.workflow_store_path)

    @property
    def workflow_path(self) -> Path:

        return self.workflow_store_path / "workflows"

    def get_workflow_details_path(self, workflow_id: uuid.UUID) -> Path:

        return self.workflow_path / str(workflow_id) / "workflow.json"

    def get_alias_path(self, alias: str):

        return self.alias_store_path / f"{alias}.alias"

    def retrieve_all_workflow_aliases(self) -> Mapping[str, uuid.UUID]:

        all_aliases = self.alias_store_path.glob("*.alias")
        result: Dict[str, uuid.UUID] = {}
        for path in all_aliases:
            alias = path.name[0:-6]
            workflow_path = path.resolve()
            workflow_id = uuid.UUID(workflow_path.parent.name)
            if alias in result.keys():
                raise Exception(
                    f"Invalid internal state for workflow archive '{self.archive_id}': duplicate alias '{alias}'."
                )
            result[alias] = workflow_id

        return result

    def retrieve_workflow_details(self, workflow_id: uuid.UUID) -> WorkflowDetails:

        workflow_path = self.get_workflow_details_path(workflow_id=workflow_id)
        if not workflow_path.exists():
            raise Exception(
                f"Can't retrieve workflow with id '{workflow_id}': id does not exist."
            )

        workflow_json = workflow_path.read_text()

        workflow_data = orjson.loads(workflow_json)
        workflow = WorkflowDetails(**workflow_data)

        return workflow

    def retrieve_workflow_states(
        self, workflow_id: uuid.UUID, filter: Union[WorkflowStateFilter, None] = None
    ) -> Dict[uuid.UUID, WorkflowState]:

        workflow_path = self.get_workflow_details_path(workflow_id=workflow_id)
        workflow_state_paths = workflow_path.parent.glob("*.state")

        assert filter is None

        states = {}
        for path in workflow_state_paths:
            _data = path.read_text()
            _json = orjson.loads(_data)
            _state = WorkflowState(**_json)
            states[_state.workflow_state_id] = _state

        return states

    def retrieve_workflow_state(
        self, workflow_id: uuid.UUID, workflow_state_id: uuid.UUID
    ) -> WorkflowState:

        workflow_path = self.get_workflow_details_path(workflow_id=workflow_id)
        workflow_state_path = workflow_path.parent / f"{workflow_state_id}.state"

        if not workflow_state_path.exists():
            raise Exception(
                f"No workflow state with id '{workflow_state_id}' exists for workflow '{workflow_id}'."
            )

        _data = workflow_state_path.read_text()
        _json = orjson.loads(_data)
        _state = WorkflowState(**_json)
        return _state


class FileSystemWorkflowStore(FileSystemWorkflowArchive, WorkflowStore):

    _archive_type_name = "filesystem_workflow_store"

    def _register_workflow_details(self, workflow_details: WorkflowDetails):

        workflow_path = self.get_workflow_details_path(
            workflow_id=workflow_details.workflow_id
        )

        if workflow_path.exists():
            raise Exception(
                f"Can't register workflow with id '{workflow_details.workflow_id}': id already registered."
            )

        workflow_path.parent.mkdir(parents=True, exist_ok=False)

        workflow_json = workflow_details.json()
        workflow_path.write_text(workflow_json)

    def _update_workflow_details(self, workflow_details: WorkflowDetails):

        workflow_path = self.get_workflow_details_path(
            workflow_id=workflow_details.workflow_id
        )

        if not workflow_path.exists():
            raise Exception(
                f"Can't update workflow with id '{workflow_details.workflow_id}': id not registered."
            )

        workflow_json = workflow_details.json()
        workflow_path.write_text(workflow_json)

    def register_alias(self, workflow_id: uuid.UUID, alias: str, force: bool = False):

        alias_path = self.get_alias_path(alias=alias)
        if not force and alias_path.exists():
            raise Exception(
                f"Can't register workflow alias '{alias}': alias already registered."
            )
        elif alias_path.exists():
            alias_path.unlink()

        workflow_path = self.get_workflow_details_path(workflow_id=workflow_id)
        if not workflow_path.exists():
            raise Exception(
                f"Can't register workflow alias '{alias}': target id '{workflow_id}' not registered."
            )

        alias_path.symlink_to(workflow_path)

    def add_workflow_state(self, workflow_id: uuid.UUID, workflow_state: WorkflowState):

        workflow_path = self.get_workflow_details_path(workflow_id=workflow_id)

        workflow_state_path = (
            workflow_path.parent / f"{workflow_state.workflow_state_id}.state"
        )

        workflow_state_json = workflow_state.json()
        workflow_state_path.write_text(workflow_state_json)
