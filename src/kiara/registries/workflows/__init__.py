# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import abc
import structlog
import uuid
from typing import TYPE_CHECKING, Callable, Dict, Iterable, Mapping, Optional

from kiara.models.events.workflow_registry import WorkflowArchiveAddedEvent
from kiara.models.workflow import Workflow
from kiara.registries import BaseArchive

if TYPE_CHECKING:
    from kiara.context import Kiara

logger = structlog.getLogger()


class WorkflowArchive(BaseArchive):
    @classmethod
    def supported_item_types(cls) -> Iterable[str]:
        return ["workflow"]

    @classmethod
    def is_writeable(cls) -> bool:
        return False

    @abc.abstractmethod
    def retrieve_all_workflow_aliases(self) -> Mapping[str, uuid.UUID]:
        pass

    @abc.abstractmethod
    def retrieve_workflow(self, workflow: uuid.UUID):
        pass


class WorkflowStore(WorkflowArchive):
    @classmethod
    def is_writeable(cls) -> bool:
        return True

    def register_workflow(self, *workflow_aliases: str) -> uuid.UUID:

        workflow_id = self.create_workflow()
        for workflow_alias in workflow_aliases:
            self.register_alias(workflow_id=workflow_id, alias=workflow_alias)
        return workflow_id

    @abc.abstractmethod
    def create_workflow(self) -> uuid.UUID:
        pass

    @abc.abstractmethod
    def register_alias(self, workflow_id: uuid.UUID, alias: str):
        pass


class WorkflowRegistry(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._event_callback: Callable = self._kiara.event_registry.add_producer(self)

        self._workflow_archives: Dict[str, WorkflowArchive] = {}
        """All registered archives/stores."""

        self._default_alias_store: Optional[str] = None
        """The alias of the store where new aliases are stored by default."""

        self._cached_workflows: Optional[Dict[str, Workflow]] = None
        self._cached_workflows_by_id: Optional[Dict[uuid.UUID, Workflow]] = None

    def register_archive(
        self,
        archive: WorkflowArchive,
        alias: str = None,
        set_as_default_store: Optional[bool] = None,
    ):

        workflow_archive_id = archive.archive_id
        archive.register_archive(kiara=self._kiara)

        if alias is None:
            alias = str(workflow_archive_id)

        if "." in alias:
            raise Exception(
                f"Can't register workflow archive with as '{alias}': registered name is not allowed to contain a '.' character (yet)."
            )

        if alias in self._workflow_archives.keys():
            raise Exception(
                f"Can't add store, workflow archive alias '{alias}' already registered."
            )

        self._workflow_archives[alias] = archive
        is_store = False
        is_default_store = False
        if isinstance(archive, WorkflowStore):
            is_store = True
            if set_as_default_store and self._default_alias_store is not None:
                raise Exception(
                    f"Can't set alias store '{alias}' as default store: default store already set."
                )

            if self._default_alias_store is None:
                is_default_store = True
                self._default_alias_store = alias

        event = WorkflowArchiveAddedEvent.construct(
            kiara_id=self._kiara.id,
            workflow_archive_id=archive.archive_id,
            workflow_archive_alias=alias,
            is_store=is_store,
            is_default_store=is_default_store,
        )
        self._event_callback(event)

    @property
    def default_alias_store(self) -> str:

        if self._default_alias_store is None:
            raise Exception("No default alias store set (yet).")
        return self._default_alias_store

    @property
    def workflow_archives(self) -> Mapping[str, WorkflowArchive]:
        return self._workflow_archives

    def get_archive(
        self, archive_id: Optional[str] = None
    ) -> Optional[WorkflowArchive]:
        if archive_id is None:
            archive_id = self.default_alias_store
            if archive_id is None:
                raise Exception("Can't retrieve default alias archive, none set (yet).")

        archive = self._workflow_archives.get(archive_id, None)
        return archive

    @property
    def workflows(self) -> Dict[str, Workflow]:

        if self._cached_workflows is not None:
            return self._cached_workflows

        all_workflows: Dict[str, Workflow] = {}
        all_workflows_by_id: Dict[uuid.UUID, Workflow] = {}
        for archive_alias, archive in self._workflow_archives.items():
            workflow_map = archive.retrieve_all_workflow_aliases()
            for alias, w_id in workflow_map.items():
                if archive_alias == self.default_alias_store:
                    final_alias = alias
                else:
                    final_alias = f"{archive_alias}.{alias}"

                if final_alias in all_workflows.keys():
                    raise Exception(
                        f"Inconsistent alias registry: alias '{final_alias}' available more than once."
                    )
                item = Workflow(
                    full_alias=final_alias,
                    rel_alias=alias,
                    workflow_id=w_id,
                    workflow_archive=archive_alias,
                    workflow_archive_id=archive.archive_id,
                )
                all_workflows[final_alias] = item
                all_workflows_by_id[w_id] = item
        self._cached_workflows = all_workflows
        self._cached_workflows_by_id = all_workflows_by_id
        return self._cached_workflows

    def register_workflow(self, *workflow_aliases: str) -> uuid.UUID:

        for workflow_alias in workflow_aliases:
            if workflow_alias in self.workflows.keys():
                raise Exception(
                    f"Can't register workflow with alias '{workflow_alias}': alias already registered."
                )

        store_name = self.default_alias_store
        store: WorkflowStore = self.get_archive(archive_id=store_name)  # type: ignore

        workflow_id = store.register_workflow(*workflow_aliases)
        return workflow_id

        # workflow_item = Workflow(
        #     full_alias=workflow_alias,
        #     rel_alias=workflow_alias,
        #     workflow_id=workflow_id,
        #     workflow_archive=store_name,
        #     workflow_archive_id=store.archive_id,
        # )
        # self._cached_workflows[workflow_alias] = workflow_item
        # self._cached_workflows_by_id[workflow_id] = workflow_item
        # return workflow_item

        # workflow = Workflow(kiara=self._kiara, workflow_alias=workflow_alias)
        # and_step_id = workflow.add_step("logic.and")
        # not_step_id = workflow.add_step("logic.not")
        #
        # workflow.add_input_link(f"{not_step_id}.a", f"{and_step_id}.y")
        #
        # workflow.set_inputs(logic_and__a=True, logic_and__b=True, x=22)
        # workflow.apply()
        # workflow.set_inputs(logic_and__a=True, logic_and__b=True, x=22)
