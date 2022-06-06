# -*- coding: utf-8 -*-
import uuid
from pathlib import Path
from typing import Dict, Mapping, Optional

from kiara.registries import ARCHIVE_CONFIG_CLS, FileSystemArchiveConfig
from kiara.registries.ids import ID_REGISTRY
from kiara.registries.workflows import WorkflowArchive, WorkflowStore


class FileSystemWorkflowArchive(WorkflowArchive):

    _archive_type_name = "filesystem_workflow_archive"
    _config_cls = FileSystemArchiveConfig

    def __init__(self, archive_id: uuid.UUID, config: ARCHIVE_CONFIG_CLS):

        super().__init__(archive_id=archive_id, config=config)

        self._base_path: Optional[Path] = None

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

    @property
    def workflow_path(self) -> Path:

        return self.workflow_store_path / "workflows"

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

    def retrieve_workflow(self, workflow_id: uuid.UUID):
        raise NotImplementedError()


class FileSystemWorkflowStore(FileSystemWorkflowArchive, WorkflowStore):

    _archive_type_name = "filesystem_workflow_store"

    def create_workflow(self) -> uuid.UUID:

        workflow_id = ID_REGISTRY.generate(comment="new workflow")
        path = self.workflow_store_path / str(workflow_id) / "workflow.json"

        path.parent.mkdir(parents=True, exist_ok=False)
        path.touch()

        return workflow_id

    def register_alias(self, workflow_id: uuid.UUID, alias: str):

        pass
