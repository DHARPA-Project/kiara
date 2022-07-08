# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import abc
import datetime
import structlog
import uuid
from typing import TYPE_CHECKING, Callable, Dict, Iterable, Mapping, Union

from kiara.models.events.workflow_registry import WorkflowArchiveAddedEvent
from kiara.models.workflow import WorkflowDetails, WorkflowState
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
    def retrieve_all_workflow_ids(self) -> Iterable[uuid.UUID]:
        pass

    @abc.abstractmethod
    def retrieve_workflow_details(self, workflow_id: uuid.UUID):
        pass

    # @abc.abstractmethod
    # def retrieve_workflow_states(
    #     self, workflow_id: uuid.UUID, filter: Union[WorkflowStateFilter, None] = None
    # ) -> Dict[str, WorkflowState]:
    #     pass

    @abc.abstractmethod
    def retrieve_workflow_state(self, workflow_state_id: str) -> WorkflowState:
        """Retrieve workflow state details for the provided state id.

        Arguments:
            workflow_id: id of the workflow
            workflow_state_id: the id of the workflow state
        """

    @abc.abstractmethod
    def retrieve_all_states_for_workflow(
        self, workflow_id: uuid.UUID
    ) -> Mapping[str, WorkflowState]:
        """Retrieve workflow state details for the provided state id.

        Arguments:
            workflow_id: id of the workflow
            workflow_state_id: the id of the workflow state
        """


class WorkflowStore(WorkflowArchive):
    @classmethod
    def is_writeable(cls) -> bool:
        return True

    def register_workflow(
        self, workflow_details: WorkflowDetails, workflow_aliases: Iterable[str] = None
    ):

        self._register_workflow_details(workflow_details=workflow_details)
        if workflow_aliases:
            if isinstance(workflow_aliases, str):
                workflow_aliases = [workflow_aliases]
            for workflow_alias in workflow_aliases:
                self.register_alias(
                    workflow_id=workflow_details.workflow_id, alias=workflow_alias
                )
        return workflow_details

    def update_workflow(self, workflow_details: WorkflowDetails):

        self._update_workflow_details(workflow_details=workflow_details)

    @abc.abstractmethod
    def _register_workflow_details(self, workflow_details: WorkflowDetails):
        pass

    @abc.abstractmethod
    def _update_workflow_details(self, workflow_details: WorkflowDetails):
        pass

    @abc.abstractmethod
    def add_workflow_state(self, workflow_state: WorkflowState):
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

        self._default_alias_store: Union[str, None] = None
        """The alias of the store where new aliases are stored by default."""

        self._all_aliases: Union[Dict[str, uuid.UUID], None] = None
        """All workflow aliases."""

        self._all_workflow_ids: Union[Dict[uuid.UUID, str], None] = None
        """All workflow ids, with store alias as values"""

        self._cached_workflows: Dict[uuid.UUID, WorkflowDetails] = {}

    def register_archive(
        self,
        archive: WorkflowArchive,
        alias: str = None,
        set_as_default_store: Union[bool, None] = None,
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
        self, archive_id: Union[str, None] = None
    ) -> Union[WorkflowArchive, None]:
        if archive_id is None:
            archive_id = self.default_alias_store
            if archive_id is None:
                raise Exception("Can't retrieve default alias archive, none set (yet).")

        archive = self._workflow_archives.get(archive_id, None)
        return archive

    @property
    def workflow_aliases(self) -> Dict[str, uuid.UUID]:
        """Retrieve all registered workflow aliases."""

        if self._all_aliases is not None:
            return self._all_aliases

        all_workflows: Dict[str, uuid.UUID] = {}
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
                all_workflows[final_alias] = w_id
        self._all_aliases = all_workflows
        return self._all_aliases

    @property
    def all_workflow_ids(self) -> Iterable[uuid.UUID]:

        if self._all_workflow_ids is not None:
            return self._all_workflow_ids.keys()

        all_ids: Dict[uuid.UUID, str] = {}
        for archive_alias, archive in self._workflow_archives.items():
            ids = archive.retrieve_all_workflow_ids()

            for _id in ids:
                assert _id not in all_ids.keys()
                all_ids[_id] = archive_alias

        self._all_workflow_ids = all_ids
        return self._all_workflow_ids.keys()

    def get_workflow_details(self, workflow: Union[str, uuid.UUID]) -> WorkflowDetails:

        if isinstance(workflow, str):
            workflow_id: Union[uuid.UUID, None] = self.workflow_aliases.get(
                workflow, None
            )
            if workflow_id is None:
                try:
                    workflow_id = uuid.UUID(workflow)
                except Exception:
                    pass
                if workflow_id is None:
                    raise Exception(
                        f"Can't retrieve workflow with alias '{workflow}': alias not registered."
                    )
        else:
            workflow_id = workflow

        if workflow_id in self._cached_workflows.keys():
            return self._cached_workflows[workflow_id]

        if self._all_workflow_ids is None:
            self.all_workflow_ids  # noqa

        store_alias = self._all_workflow_ids[workflow_id]  # type: ignore
        store = self._workflow_archives[store_alias]

        workflow_details = store.retrieve_workflow_details(workflow_id=workflow_id)
        # workflow_details._kiara = self._kiara
        # workflow = Workflow(kiara=self._kiara, workflow_details=workflow_details)
        self._cached_workflows[workflow_id] = workflow_details

        # states = store.retrieve_workflow_states(workflow_id=workflow_id)
        # workflow._snapshots = states
        # workflow.load_state()

        return workflow_details

    def register_workflow(
        self,
        workflow_details: Union[None, WorkflowDetails, str] = None,
        workflow_aliases: Union[Iterable[str], None] = None,
    ) -> WorkflowDetails:

        if workflow_aliases:
            for workflow_alias in workflow_aliases:
                if workflow_alias in self.workflow_aliases.keys():
                    raise Exception(
                        f"Can't register workflow with alias '{workflow_alias}': alias already registered."
                    )

        store_name = self.default_alias_store
        store: WorkflowStore = self.get_archive(archive_id=store_name)  # type: ignore

        if workflow_details is None:
            workflow_details = WorkflowDetails()
            workflow_details._kiara = self._kiara
        elif isinstance(workflow_details, str):
            workflow_details = WorkflowDetails(documentation=workflow_details)  # type: ignore
            workflow_details._kiara = self._kiara

        # workflow_details._kiara = self._kiara

        store.register_workflow(
            workflow_details=workflow_details, workflow_aliases=workflow_aliases
        )
        # workflow = Workflow(kiara=self._kiara, workflow_details=workflow_details)
        if self._all_workflow_ids is None:
            self.all_workflow_ids  # noqa
        self._all_workflow_ids[workflow_details.workflow_id] = store_name  # type: ignore
        self._cached_workflows[workflow_details.workflow_id] = workflow_details

        if workflow_aliases:
            for workflow_alias in workflow_aliases:
                self._all_workflow_ids[workflow_details.workflow_id] = store_name  # type: ignore
                self.workflow_aliases[workflow_alias] = workflow_details.workflow_id

        # if steps:
        #     workflow.add_steps(*steps)
        #     self.save_workflow_state(workflow=workflow.workflow_id)

        return workflow_details

    # def get_workflow_states(
    #     self, workflow: Union[uuid.UUID, str]
    # ) -> Dict[uuid.UUID, WorkflowState]:
    #
    #     workflow_details = self.get_workflow_details(workflow=workflow)
    #     archive_alias = self._workflow_locations[workflow_details.workflow_id]
    #
    #     archive = self.get_archive(archive_alias)
    #     if archive is None:
    #         raise Exception(
    #             f"Can't retrieve workflow archive '{archive_alias}', this is most likely a bug."
    #         )
    #     states = archive.retrieve_workflow_states(
    #         workflow_id=workflow_details.workflow_id
    #     )
    #
    #     return states

    def get_workflow_state(
        self,
        workflow_state_id: Union[str, None] = None,
        workflow: Union[None, uuid.UUID, str] = None,
    ) -> WorkflowState:

        if workflow is None and workflow_state_id is None:
            raise Exception(
                "Can't retrieve workflow state, neither workflow nor workflow state id specified."
            )

        if workflow:
            workflow_details = self.get_workflow_details(workflow=workflow)
            if workflow_state_id is None:
                workflow_state_id = workflow_details.current_state
            else:
                if workflow_state_id not in workflow_details.workflow_states.values():
                    raise Exception(
                        f"Can't retrieve workflow state '{workflow_state_id}' for workflow '{workflow}': state not registered for workflow."
                    )
        else:
            raise NotImplementedError()

        if workflow_state_id is None:
            raise Exception(
                f"Can't retrieve current workflow state, no state exists yet for workflow '{workflow}'."
            )

        if self._all_workflow_ids is None:
            self.all_workflow_ids  # noqa
        archive_alias = self._all_workflow_ids[workflow_details.workflow_id]  # type: ignore

        archive = self.get_archive(archive_alias)
        if archive is None:
            raise Exception(
                f"Can't retrieve workflow archive '{archive_alias}', this is most likely a bug."
            )
        state = archive.retrieve_workflow_state(
            workflow_state_id=workflow_state_id,
        )
        state._kiara = self._kiara

        return state

    def get_all_states_for_workflow(
        self, workflow: Union[uuid.UUID, str]
    ) -> Mapping[str, WorkflowState]:

        workflow_details = self.get_workflow_details(workflow=workflow)

        if self._all_workflow_ids is None:
            self.all_workflow_ids  # noqa
        archive_alias = self._all_workflow_ids[workflow_details.workflow_id]  # type: ignore

        archive = self.get_archive(archive_alias)
        if archive is None:
            raise Exception(
                f"Can't retrieve workflow archive '{archive_alias}', this is most likely a bug."
            )

        states = archive.retrieve_all_states_for_workflow(
            workflow_id=workflow_details.workflow_id
        )
        return states

    def add_workflow_state(
        self,
        workflow: Union[str, uuid.UUID, WorkflowDetails],
        workflow_state: WorkflowState,
        set_current: bool = True,
    ) -> WorkflowDetails:

        # make sure the workflow is registed
        created = datetime.datetime.now()
        if not isinstance(workflow, WorkflowDetails):
            workflow_details = self.get_workflow_details(workflow=workflow)
        else:
            workflow_details = workflow

        workflow_details.workflow_states[created] = workflow_state.instance_id

        for field_name, value_id in workflow_state.inputs.items():
            self._kiara.data_registry.store_value(value=value_id)

        # for field_name, value_id in workflow_state.outputs.items():
        #     self._kiara.data_registry.store_value(value=value_id)

        store_name = self.default_alias_store
        store: WorkflowStore = self.get_archive(archive_id=store_name)  # type: ignore

        store.add_workflow_state(workflow_state=workflow_state)
        if set_current:
            workflow_details.current_state = workflow_state.instance_id

        store.update_workflow(workflow_details)

        return workflow_details
