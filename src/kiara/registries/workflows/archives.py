# -*- coding: utf-8 -*-
import shutil
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Union

import orjson

from kiara.exceptions import NoSuchWorkflowException
from kiara.models.workflow import WorkflowMetadata, WorkflowState
from kiara.registries import ARCHIVE_CONFIG_CLS, FileSystemArchiveConfig
from kiara.registries.workflows import WorkflowArchive, WorkflowStore
from kiara.utils.windows import fix_windows_longpath


class FileSystemWorkflowArchive(WorkflowArchive):

    _archive_type_name = "filesystem_workflow_archive"
    _config_cls = FileSystemArchiveConfig  # type: ignore

    def __init__(
        self,
        archive_name: str,
        archive_config: ARCHIVE_CONFIG_CLS,
        force_read_only: bool = False,
    ):

        super().__init__(
            archive_name=archive_name,
            archive_config=archive_config,
            force_read_only=force_read_only,
        )

        self._base_path: Union[Path, None] = None
        self.alias_store_path.mkdir(parents=True, exist_ok=True)

    def _retrieve_archive_metadata(self) -> Mapping[str, Any]:

        if not self.archive_metadata_path.is_file():
            _archive_metadata = {}
        else:
            _archive_metadata = orjson.loads(self.archive_metadata_path.read_bytes())

        archive_id = _archive_metadata.get("archive_id", None)
        if not archive_id:
            try:
                _archive_id = uuid.UUID(self.workflow_store_path.name)
                _archive_metadata["archive_id"] = str(_archive_id)
            except Exception:
                raise Exception(
                    f"Could not retrieve archive id for alias archive '{self.archive_name}'."
                )

        return _archive_metadata

    @property
    def archive_metadata_path(self) -> Path:
        return self.workflow_store_path / "store_metadata.json"

    @property
    def workflow_store_path(self) -> Path:

        if self._base_path is not None:
            return self._base_path

        self._base_path = Path(self.config.archive_path).absolute()  # type: ignore
        self._base_path = fix_windows_longpath(self._base_path)
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

    @property
    def workflow_states_path(self) -> Path:
        return self.workflow_store_path / "states"

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

    def retrieve_all_workflow_ids(self) -> Iterable[uuid.UUID]:

        all_ids = self.workflow_path.glob("*")
        result = []
        for path in all_ids:
            workflow_id = uuid.UUID(path.name)
            result.append(workflow_id)
        return result

    def retrieve_workflow_metadata(self, workflow_id: uuid.UUID) -> WorkflowMetadata:

        workflow_path = self.get_workflow_details_path(workflow_id=workflow_id)
        if not workflow_path.exists():
            raise NoSuchWorkflowException(
                workflow=workflow_id,
                msg=f"Can't retrieve workflow with id '{workflow_id}': id does not exist.",
            )

        workflow_json = workflow_path.read_text()

        workflow_data = orjson.loads(workflow_json)
        workflow = WorkflowMetadata(**workflow_data)
        workflow._kiara = self.kiara_context

        return workflow

    def retrieve_workflow_state(self, workflow_state_id: str) -> WorkflowState:

        workflow_state_path = self.workflow_states_path / f"{workflow_state_id}.state"

        if not workflow_state_path.exists():
            raise Exception(f"No workflow state with id '{workflow_state_id}' exists.")

        _data = workflow_state_path.read_text()
        _json = orjson.loads(_data)
        # _json["pipeline_info"]["pipeline_structure"] = {
        #     "pipeline_config": _json["pipeline_info"]["pipeline_structure"][
        #         "pipeline_config"
        #     ]
        # }
        _state = WorkflowState(**_json)
        _state.pipeline_info._kiara = self.kiara_context
        _state._kiara = self.kiara_context
        return _state

    def retrieve_all_states_for_workflow(
        self, workflow_id: uuid.UUID
    ) -> Mapping[str, WorkflowState]:

        details = self.retrieve_workflow_metadata(workflow_id=workflow_id)

        result = {}
        for ws_id in details.workflow_history.values():
            ws_state = self.retrieve_workflow_state(workflow_state_id=ws_id)
            result[ws_id] = ws_state

        return result


class FileSystemWorkflowStore(FileSystemWorkflowArchive, WorkflowStore):

    _archive_type_name = "filesystem_workflow_store"

    def _register_workflow_metadata(self, workflow_metadata: WorkflowMetadata):

        workflow_path = self.get_workflow_details_path(
            workflow_id=workflow_metadata.workflow_id
        )

        if workflow_path.exists():
            raise Exception(
                f"Can't register workflow with id '{workflow_metadata.workflow_id}': id already registered."
            )

        workflow_path.parent.mkdir(parents=True, exist_ok=False)

        workflow_json = workflow_metadata.model_dump_json()
        workflow_path.write_text(workflow_json)

    def _update_workflow_metadata(self, workflow_metadata: WorkflowMetadata):

        workflow_path = self.get_workflow_details_path(
            workflow_id=workflow_metadata.workflow_id
        )

        if not workflow_path.exists():
            raise Exception(
                f"Can't update workflow with id '{workflow_metadata.workflow_id}': id not registered."
            )

        workflow_json = workflow_metadata.json(option=orjson.OPT_NON_STR_KEYS)
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

    def unregister_alias(self, alias: str) -> bool:

        alias_path = self.get_alias_path(alias=alias)

        if not alias_path.exists():
            return False

        alias_path.unlink()
        return True

    def add_workflow_state(self, workflow_state: WorkflowState):

        self.workflow_states_path.mkdir(exist_ok=True, parents=True)
        workflow_state_path = (
            self.workflow_states_path / f"{workflow_state.instance_id}.state"
        )

        workflow_state_json = workflow_state.model_dump_json()
        workflow_state_path.write_text(workflow_state_json)
