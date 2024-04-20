# -*- coding: utf-8 -*-
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Hashable, List, Mapping, Set, Tuple, Union

import structlog
from boltons.strutils import slugify

from kiara.defaults import (
    KIARA_DEFAULT_STAGES_EXTRACTION_TYPE,
    NONE_VALUE_ID,
    NOT_SET_VALUE_ID,
)
from kiara.exceptions import NoSuchWorkflowException
from kiara.models import KiaraModel
from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.events.pipeline import ChangedValue, PipelineEvent
from kiara.models.module.jobs import ActiveJob, ExecutionContext, JobConfig, JobStatus
from kiara.models.module.manifest import Manifest
from kiara.models.module.pipeline import (
    PipelineConfig,
    PipelineStep,
    StepStatus,
    StepValueAddress,
    generate_pipeline_endpoint_name,
)
from kiara.models.module.pipeline.controller import SinglePipelineController
from kiara.models.module.pipeline.pipeline import Pipeline, PipelineInfo
from kiara.models.python_class import KiaraModuleInstance
from kiara.models.values.value import Value, ValueMap
from kiara.models.values.value_schema import ValueSchema
from kiara.models.workflow import WorkflowInfo, WorkflowMetadata, WorkflowState
from kiara.registries.ids import ID_REGISTRY
from kiara.utils import find_free_id, log_exception
from kiara.utils.dates import get_current_time_incl_timezone

if TYPE_CHECKING:
    from kiara.context import Kiara

logger = structlog.getLogger()


class WorkflowPipelineController(SinglePipelineController):
    """
    A [PipelineController][kiara.models.modules.pipeline.controller.PipelineController] that executes all pipeline steps non-interactively.

    This is the default implementation of a ``PipelineController``, and probably the most simple implementation of one.
    It waits until all inputs are set, after which it executes all pipeline steps in the required order.

    """

    def __init__(
        self,
        kiara: "Kiara",
    ):

        self._is_running: bool = False
        super().__init__(job_registry=kiara.job_registry)

    def _pipeline_event_occurred(self, event: PipelineEvent):

        if event.pipeline_id != self.pipeline.pipeline_id:
            return

        self._pipeline_details = None

    def process_pipeline(
        self,
    ) -> Tuple[Mapping[uuid.UUID, uuid.UUID], Mapping[str, ActiveJob]]:

        log = logger.bind(pipeline_id=self.pipeline.pipeline_id)
        if self._is_running:
            log.debug(
                "ignore.pipeline_process",
                reason="Pipeline already running.",
            )
            raise Exception("Pipeline already running.")

        log.debug("execute.pipeline")
        self._is_running = True

        result: Dict[uuid.UUID, uuid.UUID] = {}
        errors: Dict[str, ActiveJob] = {}

        try:
            stages = self.pipeline.structure.extract_processing_stages(
                stages_extraction_type=KIARA_DEFAULT_STAGES_EXTRACTION_TYPE
            )
            for idx, stage in enumerate(stages, start=1):

                log.debug(
                    "execute.pipeline.stage",
                    stage=idx,
                )

                job_ids = {}
                stage_failed = False
                for step_id in stage:

                    log.debug(
                        "execute.pipeline.step",
                        step_id=step_id,
                    )

                    try:
                        job_id = self.process_step(step_id)
                        job_ids[step_id] = job_id
                    except Exception as e:
                        # TODO: cancel running jobs?
                        log_exception(e)
                        log.error(
                            "error.processing.workflow_pipeline",
                            step_id=step_id,
                            error=e,
                        )
                        stage_failed = True

                self._job_registry.wait_for(*job_ids.values())
                for step_id, job_id in job_ids.items():
                    j = self._job_registry.get_job(job_id)
                    if j.status != JobStatus.SUCCESS:
                        errors[step_id] = j
                output_job_map = self.set_processing_results(job_ids=job_ids)
                result.update(output_job_map)
                if not stage_failed:
                    log.debug(
                        "execute_finished.pipeline.stage",
                        stage=idx,
                    )
                else:
                    log.debug(
                        "execute_failed.pipeline.stage",
                        stage=idx,
                    )
                    break
        except Exception as e:
            log_exception(e)
        finally:
            self._is_running = False

        log.debug("execute_finished.pipeline")
        return result, errors


class WorkflowStatus(KiaraModel):
    pass


class Workflow(object):
    """
    A wrapper object to make working with workflows easier for frontend code.

    This is the type of class everyone advises you against creating in Object-Oriented code, all it contains is basically
    internal state. In this case, I believe it's warranted because otherwise frontend code would spin out of control,
    complexity-wise. I'm happy to be proven wrong, though.

    None of this is thread-safe (yet).

    This object can be initialized in different ways, depending on circumstances:

     - if you want to create a new workflow object: use 'None' or a WorkflowMetadata object you createed earlier
         (ensure you don't re-use an existing workflow_id, otherwise it might get overwrittern in the backend)
     - if you want to create the wrapper object from an existing workflow: provide its id or alias string

    Arguments:
    ---------
        kiara: the kiara context in which this workflow lives
        workflow: the workflow metadata (or reference to it)
        load_existing: if set to 'False', a workflow with the provided id/alias can't already exist, if 'True', it must, if set to 'None', kiara tries to be smart and loads if exists, otherwise creates a new one
    """

    @classmethod
    def load(
        cls,
        workflow: Union[uuid.UUID, str],
        kiara: Union["Kiara", None] = None,
        create: bool = False,
    ):
        """Load an existing workflow using a workflow id or alias."""
        try:
            workflow_obj = Workflow(workflow=workflow, kiara=kiara, load_existing=True)
        except NoSuchWorkflowException as nswe:
            if create:

                if isinstance(workflow, uuid.UUID):
                    raise nswe
                temp = None
                try:
                    temp = uuid.UUID(workflow)
                except Exception:
                    pass
                if temp is not None:
                    raise nswe

                if kiara is None:
                    from kiara.context import Kiara

                    kiara = Kiara.instance()

                workflow_metadata = kiara.workflow_registry.register_workflow(
                    workflow_aliases=[workflow]
                )
                workflow_obj = Workflow(workflow=workflow_metadata.workflow_id)

        return workflow_obj

    @classmethod
    def create(
        cls,
        alias: Union[None, str] = None,
        replace_existing_alias: bool = False,
        doc: Union[Any, None] = None,
        kiara: Union["Kiara", None] = None,
    ):
        """Create a new workflow object."""
        if replace_existing_alias and alias is not None:
            if kiara is None:
                from kiara.context import Kiara

                kiara = Kiara.instance()

            kiara.workflow_registry.unregister_alias(alias=alias)

        workflow = Workflow(workflow=alias, kiara=kiara, load_existing=False)
        if doc:
            workflow.documentation = doc
        return workflow

    def __init__(
        self,
        workflow: Union[None, WorkflowMetadata, uuid.UUID, str] = None,
        kiara: Union["Kiara", None] = None,
        load_existing: Union[bool, None] = None,
    ):

        if kiara is None:
            from kiara.context import Kiara

            kiara = Kiara.instance()

        self._kiara: "Kiara" = kiara
        self._metadata_is_stored: bool = False
        self._metadata_is_synced: bool = False

        self._pending_aliases: Set[str] = set()

        _workflow_id: Union[None, uuid.UUID] = None
        _workflow_alias: Union[None, str] = None
        _workflow_metadata: Union[None, WorkflowMetadata] = None

        if workflow is None:
            if load_existing is True:
                raise Exception(
                    "Can't create workflow: no workflow reference provided, but 'load_existing' forced to 'True'."
                )
            _w_id = ID_REGISTRY.generate(comment="New workflow object.")
            _workflow_metadata = WorkflowMetadata(workflow_id=_w_id)
        elif isinstance(workflow, str):
            try:
                _workflow_id = uuid.UUID(workflow)
                _workflow_metadata = (
                    self._kiara.workflow_registry.get_workflow_metadata(
                        workflow=_workflow_id
                    )
                )
                if load_existing is False:
                    raise Exception(
                        f"Can't create workflow for id '{_workflow_id}': workflow with this id already registered and 'load_existing' set to 'False'."
                    )
                self._metadata_is_stored = True
                self._metadata_is_synced = True
            except NoSuchWorkflowException as nswe:
                # means an uuid was provided
                raise nswe
            except Exception:
                # means it's an alias
                _workflow_alias = workflow
                try:
                    _workflow_id = self._kiara.workflow_registry.get_workflow_id(
                        workflow
                    )
                    if load_existing is False:
                        raise Exception(
                            f"Can't create workflow with alias '{_workflow_alias}': alias already registered, and 'load_existing' set to 'False'."
                        )
                    _workflow_metadata = (
                        self._kiara.workflow_registry.get_workflow_metadata(
                            workflow=_workflow_id
                        )
                    )
                    self._metadata_is_stored = True
                    self._metadata_is_synced = True
                except NoSuchWorkflowException:
                    # does not exist yet
                    if load_existing is True:
                        raise NoSuchWorkflowException(
                            msg=f"Can't load workflow with alias '{_workflow_alias}': no workflow with this alias registered.",
                            workflow=_workflow_alias,
                        )
                    self._pending_aliases.add(_workflow_alias)
                    _workflow_id = ID_REGISTRY.generate(comment="New workflow object.")
                    _workflow_metadata = WorkflowMetadata(workflow_id=_workflow_id)
        elif isinstance(workflow, uuid.UUID):
            _workflow_id = workflow
            if load_existing is False:
                raise Exception(
                    f"Can't create workflow for id '{_workflow_id}': 'load_existing' set to 'False'."
                )
            _workflow_metadata = self._kiara.workflow_registry.get_workflow_metadata(
                workflow=_workflow_id
            )
            self._metadata_is_stored = True
            self._metadata_is_synced = True
        elif isinstance(workflow, WorkflowMetadata):
            if load_existing is True:
                raise Exception(
                    f"Can't create workflow with id '{workflow.workflow_id}': 'load_existing' forced to 'True'."
                )
            temp = None
            _workflow_metadata = None
            try:
                temp = self._kiara.workflow_registry.get_workflow_metadata(
                    workflow.workflow_id
                )
            except Exception:
                _workflow_metadata = workflow
                _workflow_id = _workflow_metadata.workflow_id

            if temp is not None:
                raise Exception(
                    f"Can't create new workflow with id '{workflow.workflow_id}': id already registered."
                )

        else:
            raise Exception(
                f"Can't find workflow metadata for '{workflow}: invalid type '{type(workflow)}'."
            )

        assert _workflow_id is not None
        assert _workflow_metadata is not None

        self._workflow_metadata: WorkflowMetadata = _workflow_metadata
        self._workflow_id: uuid.UUID = self.workflow_metadata.workflow_id

        self._execution_context: ExecutionContext = ExecutionContext()
        self._pipeline_controller: WorkflowPipelineController = (
            WorkflowPipelineController(kiara=self._kiara)
        )

        self._all_inputs: Dict[str, Union[None, uuid.UUID]] = {}
        self._all_inputs_optimistic_lookup: Dict[str, Dict[Hashable, uuid.UUID]] = {}
        self._current_pipeline_inputs: Union[Dict[str, uuid.UUID], None] = None
        self._current_pipeline_outputs: Union[Dict[str, uuid.UUID], None] = None

        self._steps: Dict[str, PipelineStep] = {}

        self._current_workflow_inputs_schema: Union[Dict[str, ValueSchema], None] = None
        self._current_workflow_outputs_schema: Union[Dict[str, ValueSchema], None] = (
            None
        )
        self._workflow_input_aliases: Dict[str, str] = dict(
            _workflow_metadata.input_aliases
        )
        self._workflow_output_aliases: Dict[str, str] = dict(
            _workflow_metadata.output_aliases
        )

        self._current_workflow_inputs: Union[Dict[str, uuid.UUID], None] = None
        self._current_workflow_outputs: Union[Dict[str, uuid.UUID], None] = None

        # self._current_state_cid: Union[None, CID] = None
        # self._state_history: Dict[datetime, str] = {}
        self._state_cache: Dict[str, WorkflowState] = {}
        self._state_output_cache: Dict[str, Set[uuid.UUID]] = {}
        self._state_jobrecord_cache: Dict[str, Set[uuid.UUID]] = {}

        self._job_id_cache: Dict[uuid.UUID, uuid.UUID] = {}
        """Cache to save job ids per output value(s), in order to save jobs if output values are saved."""

        self._pipeline: Union[Pipeline, None] = None
        self._pipeline_info: Union[PipelineInfo, None] = None
        self._current_info: Union[WorkflowInfo, None] = None
        self._current_state: Union[WorkflowState, None] = None

        if self._workflow_metadata.workflow_history:
            self.load_state()

    def _sync_workflow_metadata(self):
        """Store/update the metadata of this workflow."""
        self._workflow_metadata = self._kiara.workflow_registry.register_workflow(
            workflow_metadata=self._workflow_metadata,
            workflow_aliases=self._pending_aliases,
        )
        self._pending_aliases.clear()
        self._metadata_is_stored = True
        self._metadata_is_synced = True

    @property
    def workflow_id(self) -> uuid.UUID:
        """Retrieve the globally unique id for this workflow."""
        return self._workflow_metadata.workflow_id

    @property
    def workflow_metadata(self) -> WorkflowMetadata:
        """Retrieve an object that contains metadata for this workflow."""
        return self._workflow_metadata

    @property
    def documentation(self) -> DocumentationMetadataModel:
        return self.workflow_metadata.documentation

    @documentation.setter
    def documentation(self, documentation: Any):

        doc = DocumentationMetadataModel.create(documentation)
        self.workflow_metadata.documentation = doc

    @property
    def is_persisted(self) -> bool:
        """Check whether this workflow is persisted in it's current state."""
        return self.workflow_metadata.is_persisted

    @property
    def current_pipeline_inputs_schema(self) -> Mapping[str, ValueSchema]:
        return self.pipeline.structure.pipeline_inputs_schema

    @property
    def current_pipeline_inputs(self) -> Mapping[str, uuid.UUID]:

        if self._current_pipeline_inputs is None:
            self._apply_inputs()
        assert self._current_pipeline_inputs is not None
        return self._current_pipeline_inputs

    @property
    def current_pipeline_input_values(self) -> ValueMap:
        return self._kiara.data_registry.load_values(
            values=self.current_pipeline_inputs
        )

    @property
    def current_pipeline_outputs_schema(self) -> Mapping[str, ValueSchema]:
        return self.pipeline.structure.pipeline_outputs_schema

    @property
    def current_pipeline_outputs(self) -> Mapping[str, uuid.UUID]:

        if self._current_pipeline_outputs is None:
            try:
                self.process_steps()
            except Exception:
                self._current_pipeline_outputs = (
                    self.pipeline.get_current_pipeline_outputs()
                )

        assert self._current_pipeline_outputs is not None
        return self._current_pipeline_outputs

    @property
    def current_pipeline_output_values(self) -> ValueMap:
        return self._kiara.data_registry.load_values(
            values=self.current_pipeline_outputs
        )

    @property
    def input_aliases(self) -> Mapping[str, str]:
        return self._workflow_input_aliases

    @property
    def output_aliases(self) -> Mapping[str, str]:
        return self._workflow_output_aliases

    def clear_current_inputs_for_step(self, step_id):

        fields = self.get_current_inputs_schema_for_step(step_id)
        for field in fields.keys():
            self.set_inputs(**{k: None for k in fields.keys()})

    @property
    def current_inputs_schema(self) -> Mapping[str, ValueSchema]:

        if self._current_workflow_inputs_schema is not None:
            return self._current_workflow_inputs_schema

        temp = {}
        # TODO; check correctness when an alias refers to two different inputs
        for k, v in self.current_pipeline_inputs_schema.items():
            if k in self._workflow_input_aliases.keys():
                temp[self._workflow_input_aliases[k]] = v
            else:
                temp[k] = v
        self._current_workflow_inputs_schema = temp
        return self._current_workflow_inputs_schema

    def get_current_inputs_schema_for_step(
        self, step_id: str
    ) -> Mapping[str, ValueSchema]:
        return self.pipeline.structure.get_pipeline_inputs_schema_for_step(
            step_id=step_id
        )

    def get_current_outputs_schema_for_step(
        self, step_id: str
    ) -> Mapping[str, ValueSchema]:
        return self.pipeline.structure.get_pipeline_outputs_schema_for_step(
            step_id=step_id
        )

    @property
    def current_input_names(self) -> List[str]:
        return sorted(self.current_inputs_schema.keys())

    @property
    def current_inputs(self) -> Mapping[str, uuid.UUID]:

        if self._current_workflow_inputs is not None:
            return self._current_workflow_inputs

        temp = {}
        for k, v in self.current_pipeline_inputs.items():
            if k in self._workflow_input_aliases.keys():
                temp[self._workflow_input_aliases[k]] = v
            else:
                temp[k] = v
        self._current_workflow_inputs = temp
        return self._current_workflow_inputs

    @property
    def current_input_values(self) -> ValueMap:
        return self._kiara.data_registry.load_values(values=self.current_inputs)

    @property
    def current_outputs_schema(self) -> Mapping[str, ValueSchema]:

        if self._current_workflow_outputs_schema is not None:
            return self._current_workflow_outputs_schema

        if not self._workflow_output_aliases:
            self._current_workflow_outputs_schema = dict(
                self.current_pipeline_outputs_schema
            )
        else:
            temp = {}
            for k, v in self._workflow_output_aliases.items():
                temp[v] = self.current_pipeline_outputs_schema[k]
            self._current_workflow_outputs_schema = temp

        return self._current_workflow_outputs_schema

    @property
    def current_output_names(self) -> List[str]:
        return sorted(self.current_outputs_schema.keys())

    @property
    def current_outputs(self) -> Mapping[str, uuid.UUID]:

        if self._current_workflow_outputs is not None:
            return self._current_workflow_outputs

        if not self._workflow_output_aliases:
            self._current_workflow_outputs = dict(self.current_pipeline_outputs)
        else:
            temp: Dict[str, uuid.UUID] = {}
            for k, v in self._workflow_output_aliases.items():
                temp[v] = self.current_pipeline_outputs[k]
            self._current_workflow_outputs = temp

        return self._current_workflow_outputs

    @property
    def current_output_values(self) -> ValueMap:
        return self._kiara.data_registry.load_values(values=self.current_outputs)

    @property
    def current_state(self) -> WorkflowState:

        if self._current_state is not None:
            return self._current_state

        self._current_state = WorkflowState.create_from_workflow(self)
        self._state_cache[self._current_state.instance_id] = self._current_state
        return self._current_state

    @property
    def pipeline(self) -> Pipeline:

        if self._pipeline is not None:
            return self._pipeline

        self._invalidate_pipeline()

        steps = list(self._steps.values())
        # input_aliases_temp = create_input_alias_map(steps=steps)
        # input_aliases = {}
        # for k, v in input_aliases_temp.items():
        #     if k in self._pipeline_input_aliases.keys():
        #         input_aliases[k] = self._pipeline_input_aliases[k]
        #     else:
        #         input_aliases[k] = v
        #
        # if not self._pipeline_output_aliasess:
        #     output_aliases = create_output_alias_map(steps=steps)
        # else:
        #     output_aliases = self._pipeline_output_aliasess

        pipeline_config = PipelineConfig.from_config(
            pipeline_name="__workflow__",
            data={
                "steps": steps,
                "doc": self.workflow_metadata.documentation,
                # "input_aliases": input_aliases,
                # "output_aliases": output_aliases,
            },
        )
        structure = pipeline_config.structure
        self._pipeline = Pipeline(structure=structure, kiara=self._kiara)
        self._pipeline_controller.pipeline = self._pipeline
        return self._pipeline

    def _apply_inputs(self) -> Mapping[str, Mapping[str, Mapping[str, ChangedValue]]]:

        pipeline = self.pipeline

        inputs_to_set = {}
        for field_name, value in self._all_inputs.items():
            # if value in [None, NONE_VALUE_ID, NOT_SET_VALUE_ID]:
            #     continue
            if field_name in pipeline.structure.pipeline_inputs_schema.keys():
                inputs_to_set[field_name] = value

        logger.debug(
            "workflow.apply_inputs",
            workflow_id=str(self.workflow_id),
            keys=", ".join(inputs_to_set.keys()),
        )

        changed: Mapping[str, Mapping[str, Mapping[str, ChangedValue]]] = (
            pipeline.set_pipeline_inputs(inputs=inputs_to_set)
        )

        self._current_pipeline_inputs = pipeline.get_current_pipeline_inputs()

        for field_name, value_id in self._current_pipeline_inputs.items():
            self._all_inputs[field_name] = value_id
        self._current_pipeline_outputs = None

        for stage, steps in pipeline.get_steps_by_stage().items():
            stage_valid = True
            cached_steps = []
            for step_id in steps.keys():
                step_details = pipeline.get_step_details(step_id=step_id)
                if step_details.status == StepStatus.INPUTS_INVALID:
                    stage_valid = False
                    break
                elif step_details.status == StepStatus.INPUTS_READY:
                    job_config = JobConfig(
                        module_type=step_details.step.module.manifest.module_type,
                        module_config=step_details.step.module.manifest.module_config,
                        is_resolved=step_details.step.module.manifest.is_resolved,
                        inputs=step_details.inputs,
                    )
                    match = self._kiara.job_registry.find_job_record_for_manifest(
                        inputs_manifest=job_config
                    )
                    if match:
                        cached_steps.append(step_id)
            if cached_steps:
                self.process_steps(*cached_steps)
            if not stage_valid:
                break

        self._current_state = None
        self._current_info = None
        self._pipeline_info = None
        return changed

    def process_steps(
        self, *step_ids: str
    ) -> Tuple[Mapping[uuid.UUID, uuid.UUID], Mapping[str, ActiveJob]]:

        self.pipeline

        if not step_ids:
            output_job_map, errors = self._pipeline_controller.process_pipeline()
        else:
            job_ids = {}
            for step_id in step_ids:
                job_id = self._pipeline_controller.process_step(
                    step_id=step_id, wait=True
                )
                job_ids[step_id] = job_id

            self._pipeline_controller._job_registry.wait_for(*job_ids.values())
            errors = {}
            for step_id, job_id in job_ids.items():
                j = self._pipeline_controller._job_registry.get_job(job_id)
                if j.status != JobStatus.SUCCESS:
                    errors[step_id] = j
            output_job_map = self._pipeline_controller.set_processing_results(
                job_ids=job_ids
            )

        self._job_id_cache.update(output_job_map)

        self._current_pipeline_outputs = self.pipeline.get_current_pipeline_outputs()
        self._current_state = None
        self._pipeline_info = None
        self._current_info = None

        return output_job_map, errors

    def _invalidate_pipeline(self):

        self._pipeline_controller.pipeline = None
        self._pipeline = None
        self._pipeline_info = None
        self._current_info = None
        self._current_state = None
        self._current_workflow_inputs_schema = None
        self._current_workflow_outputs_schema = None
        self._current_pipeline_inputs = None
        self._current_pipeline_outputs = None

    def set_input(self, field_name: str, value: Any) -> Union[None, uuid.UUID]:
        """
        Set a single pipeline input.

        Arguments:
        ---------
            field_name: The name of the input field.
            value: The value to set.

        Returns:
        -------
            None if the value for that field in the pipeline didn't change, otherwise the value_id of the new (registered) value.

        """
        diff = self.set_inputs(**{field_name: value})
        return diff.get(field_name, None)

    def set_inputs(self, **inputs: Any) -> Dict[str, Union[uuid.UUID, None]]:
        """
        Set multiple pipeline inputs, at once.

        Arguments:
        ---------
            inputs: The inputs to set.

        Returns:
        -------
            a dict containing only the newly set or changed values with field name as keys, and value_id (or None) as values.
        """
        _inputs = {}
        for k, v in inputs.items():
            # translate aliases
            match = False
            for field, alias in self._workflow_input_aliases.items():
                if k == alias:
                    match = True
                    _inputs[field] = v

            if not match and k in self.current_pipeline_inputs_schema.keys():
                _inputs[k] = v

        inputs = _inputs

        invalid = []
        for k, v in inputs.items():
            if k not in self.pipeline.structure.pipeline_inputs_schema.keys():
                invalid.append(k)
        if invalid:
            raise Exception(
                f"Can't set pipeline inputs, invalid field(s): {', '.join(invalid)}. Available inputs: '{', '.join(self.pipeline.structure.pipeline_inputs_schema.keys())}'"
            )

        diff: Dict[str, Union[None, uuid.UUID]] = {}
        for k, val_new in inputs.items():

            val_old = self._all_inputs.get(k, None)
            if val_old is None and val_new is None:
                continue

            if val_new is None:
                self._all_inputs[k] = None
                diff[k] = None
                continue

            if isinstance(val_new, uuid.UUID):
                if val_new == val_old:
                    continue
                else:
                    self._all_inputs[k] = val_new
                    diff[k] = val_new
                    continue

            if isinstance(val_new, Value):
                if val_new.value_id == val_old:
                    continue
                else:
                    self._all_inputs[k] = val_new.value_id
                    diff[k] = val_new.value_id
                    continue

            # TODO: check for aliases?
            try:
                _new_item_hash = hash(val_new)

                _match: Union[None, uuid.UUID] = None
                for _item_hash, _value_id in self._all_inputs_optimistic_lookup.get(
                    k, {}
                ).items():
                    if _item_hash == _new_item_hash:
                        if _value_id == val_old:
                            _match = val_old
                            break

                if not _match:
                    _schema = self.current_pipeline_inputs_schema[k]
                    val = self._kiara.data_registry.register_data(
                        data=val_new, schema=_schema, reuse_existing=True
                    )
                    _match = val.value_id
                    self._all_inputs_optimistic_lookup.setdefault(k, {})[
                        _new_item_hash
                    ] = _match

                if _match == val_old:
                    continue
                else:
                    self._all_inputs[k] = _match
                    diff[k] = _match
                    continue

            except Exception:
                # value can't be hashed, so we have to accept we can not re-use an existing value for this
                _schema = self.current_pipeline_inputs_schema[k]
                val = self._kiara.data_registry.register_data(
                    data=val_new, schema=_schema, reuse_existing=True
                )
                new_value_id = val.value_id
                if new_value_id == val_old:
                    continue
                else:
                    self._all_inputs[k] = new_value_id
                    diff[k] = new_value_id

        self._all_inputs.update(diff)

        if diff:
            self._current_info = None
            self._current_state = None
            self._current_pipeline_inputs = None
            self._current_pipeline_outputs = None
            self._current_workflow_inputs = None
            self._current_workflow_outputs = None
            self._pipeline_info = None
            self._apply_inputs()

        return diff

    def add_steps(
        self,
        *pipeline_steps: Union[PipelineStep, Mapping[str, Any]],
        replace_existing: bool = False,
        clear_existing: bool = False,
    ):

        if clear_existing:
            self.clear_steps()

        duplicates = []
        for step in pipeline_steps:
            if isinstance(step, PipelineStep):
                step_id = step.step_id
            else:
                step_id = step["step_id"]
            if step_id in self._steps.keys() and not replace_existing:
                duplicates.append(step_id)

        if duplicates:
            raise Exception(
                f"Can't add steps, step id(s) already taken: {', '.join(duplicates)}."
            )

        for step in pipeline_steps:
            if isinstance(step, PipelineStep):
                input_connections = {}
                for input_field, links in step.input_links.items():
                    if len(links) != 1:
                        raise NotImplementedError()
                    input_connections[input_field] = links[0].alias
                data: Mapping[str, Any] = {
                    "operation": step.manifest_src.module_type,
                    "step_id": step.step_id,
                    "module_config": step.manifest_src.module_config,
                    "input_connections": input_connections,
                    "doc": step.doc,
                    "replace_existing": replace_existing,
                }
            else:
                data = step

            self.add_step(**data)

        self._invalidate_pipeline()

    def clear_steps(self, *step_ids: str):

        if not step_ids:
            self._steps.clear()
        else:
            for step_id in step_ids:
                self._steps.pop(step_id, None)

        self._invalidate_pipeline()

    def set_input_alias(self, input_field: str, alias: str):

        if "." in input_field:
            tokens = input_field.split(".")
            if len(tokens) != 2:
                raise Exception(
                    f"Invalid input field specification '{input_field}': can only contain a single (or no) '.' character."
                )
            input_field = generate_pipeline_endpoint_name(tokens[0], tokens[1])

        self._workflow_input_aliases[input_field] = alias
        self._current_workflow_inputs = None
        self._current_workflow_inputs_schema = None
        self.workflow_metadata.input_aliases[input_field] = alias
        self._metadata_is_synced = False

    def set_output_alias(self, output_field: str, alias: str):

        if "." in output_field:
            tokens = output_field.split(".")
            if len(tokens) != 2:
                raise Exception(
                    f"Invalid output field specification '{output_field}': can only contain a single (or no) '.' character."
                )
            output_field = generate_pipeline_endpoint_name(tokens[0], tokens[1])

        self._workflow_output_aliases[output_field] = alias
        self._current_workflow_outputs = None
        self._current_workflow_outputs_schema = None
        self.workflow_metadata.output_aliases[output_field] = alias
        self._metadata_is_synced = False

    # def remove_step(self, step_id: str):
    #
    #     if step_id not in self._steps.keys():
    #         raise Exception(f"Can't remove step, no step with id '{step_id}'.")
    #
    #     del_step = self._steps[step_id]
    #     for step_id, step in self._steps.items():
    #         for input_field, links in step.input_links.items():
    #             for link in links:
    #                 if link.step_id == del_step.step_id:
    #                     links.remove(link)
    #
    #     self._invalidate_pipeline()

    def add_step(
        self,
        operation: str,
        step_id: Union[str, None] = None,
        module_config: Union[None, Mapping[str, Any]] = None,
        input_connections: Union[None, Mapping[str, str]] = None,
        doc: Union[str, DocumentationMetadataModel, None] = None,
        replace_existing: bool = False,
    ) -> PipelineStep:
        """
        Add a step to the workflows current pipeline structure.

        If no 'step_id' is provided, a unque one will automatically be generated based on the 'module_type' argument.

        Arguments:
        ---------
            operation: the module or operation name
            step_id: the id of the new step
            module_config: (optional) configuration for the kiara module this step uses
            input_connections: a map with this steps input field name(s) as keys and output field links (format: <step_id>.<output_field_name>) as value(s).
            replace_existing: if set to 'True', this replaces a step with the same id that already exists, otherwise an exception will be thrown
        """
        if step_id is None:
            step_id = find_free_id(
                slugify(operation, delim="_"), current_ids=self._steps.keys()
            )

        if "." in step_id:
            raise Exception(f"Invalid step id '{step_id}': id can't contain '.'.")

        if step_id in self._steps.keys() and not replace_existing:
            raise Exception(
                f"Can't add step with id '{step_id}': step already exists and 'replace_existing' not set."
            )
        elif step_id in self._steps.keys():
            raise NotImplementedError()

        manifest = self._kiara.create_manifest(
            module_or_operation=operation, config=module_config
        )
        module = self._kiara.module_registry.create_module(manifest=manifest)
        manifest_src = Manifest(
            module_type=manifest.module_type, module_config=manifest.module_config
        )
        step = PipelineStep(
            step_id=step_id,
            module_type=module.module_type_name,
            module_config=module.config.model_dump(),
            module_details=KiaraModuleInstance.from_module(module=module),
            doc=doc,
            manifest_src=manifest_src,
        )
        step._module = module
        self._steps[step_id] = step

        if input_connections:
            for k, v in input_connections.items():
                self.connect_to_inputs(v, f"{step_id}.{k}")

        self._invalidate_pipeline()

        return step

    def connect_fields(self, *fields: Union[Tuple[str, str], str]):

        pairs = []
        current_pair = None
        for field in fields:
            if isinstance(field, str):
                tokens = field.split(".")
                if not len(tokens) == 2:
                    raise Exception(
                        f"Can't connect field '{field}', field name must be in format: <step_id>.<field_name>."
                    )
                if not current_pair:
                    current_pair = [tokens]
                else:
                    if not len(current_pair) == 1:
                        raise Exception(
                            f"Can't connect fields, invalid input(s): {fields}"
                        )
                    current_pair.append(tokens)
                    pairs.append(current_pair)
                    current_pair = None
            else:
                if not len(field) == 2:
                    raise Exception(
                        f"Can't connect fields, field tuples must have length 2: {field}"
                    )
                if current_pair:
                    raise Exception(
                        f"Can't connect fields, dangling single field: {current_pair}"
                    )
                pair = []
                for f in field:
                    tokens = f.split(".")
                    if not len(tokens) == 2:
                        raise Exception(
                            f"Can't connect field '{f}', field name must be in format: <step_id>.<field_name>."
                        )
                    pair.append(tokens)
                pairs.append(pair)

        for pair in pairs:
            self.connect_steps(pair[0][0], pair[0][1], pair[1][0], pair[1][1])

    def connect_steps(
        self,
        source_step: Union[PipelineStep, str],
        source_field: str,
        target_step: Union[PipelineStep, str],
        target_field: str,
    ):

        if isinstance(source_step, str):
            source_step_obj = self.get_step(source_step)
        else:
            source_step_obj = source_step
        if isinstance(target_step, str):
            target_step_obj = self.get_step(target_step)
        else:
            target_step_obj = target_step

        source_step_id = source_step_obj.step_id
        target_step_id = target_step_obj.step_id

        reversed = False

        if source_field not in source_step_obj.module.outputs_schema.keys():
            reversed = True
        if target_field not in target_step_obj.module.inputs_schema.keys():
            reversed = True

        if reversed:
            if target_field not in target_step_obj.module.outputs_schema.keys():
                raise Exception(
                    f"Can't connect steps '{source_step_id}.{source_field}' -> '{target_step_id}.{target_field}': invalid field name(s)."
                )
            if source_field not in source_step_obj.module.inputs_schema.keys():
                raise Exception(
                    f"Can't connect steps '{source_step_id}.{source_field}' -> '{target_step_id}.{target_field}': invalid field name(s)."
                )
        else:
            if target_field not in target_step_obj.module.inputs_schema.keys():
                raise Exception(
                    f"Can't connect steps '{source_step_id}.{source_field}' -> '{target_step_id}.{target_field}': invalid field name(s)."
                )
            if source_field not in source_step_obj.module.outputs_schema.keys():
                raise Exception(
                    f"Can't connect steps '{source_step_id}.{source_field}' -> '{target_step_id}.{target_field}': invalid field name(s)."
                )

        # we rely on the value of input links to always be a dict here
        if not reversed:
            source_addr = StepValueAddress(
                step_id=source_step_id, value_name=source_field
            )
            target_step_obj.input_links.setdefault(target_field, []).append(source_addr)  # type: ignore
        else:
            source_addr = StepValueAddress(
                step_id=target_step_id, value_name=target_field
            )
            source_step_obj.input_links.setdefault(source_field, []).append(source_addr)  # type: ignore

        self._invalidate_pipeline()

    def connect_to_inputs(self, source_field: str, *input_fields: str):

        source_tokens = source_field.split(".")
        if len(source_tokens) != 2:
            raise Exception(
                f"Can't add input link(s): invalid format for provided source '{source_field}', must be string with a single '.' to delimit step-id and output field name."
            )

        source_step = self.get_step(source_tokens[0])
        if source_step is None:
            raise Exception(
                f"Can't add input link(s)': no source step with id '{source_tokens[0]}' exists."
            )

        if source_tokens[1] not in source_step.module.outputs_schema.keys():
            av_fields = ", ".join(source_step.module.outputs_schema.keys())
            raise Exception(
                f"Can't add input link(s): source step with id '{source_step.step_id}' does not have output field '{source_tokens[1]}'. Available field names: {av_fields}."
            )

        source_addr = StepValueAddress(
            step_id=source_step.step_id, value_name=source_tokens[1]
        )

        steps = []
        for input_field in input_fields:
            input_tokens = input_field.split(".")
            if len(input_tokens) != 2:
                raise Exception(
                    f"Can't add input link '{input_field}': invalid format, must be string with a single '.' to delimit step-id and field name."
                )

            step = self.get_step(input_tokens[0])
            if step is None:
                raise Exception(
                    f"Can't add input link '{input_field}': no step with id '{input_tokens[0]}' exists."
                )

            if input_tokens[1] not in step.module.inputs_schema.keys():
                av_fields = ", ".join(step.module.inputs_schema.keys())
                raise Exception(
                    f"Can't add input link '{input_field}': step with id '{input_tokens[0]}' does not have input field '{input_tokens[1]}'. Available field names: {av_fields}."
                )
            steps.append((step, input_tokens[1]))

        for s in steps:
            step, field_name = s
            # we rely on the value of input links to always be a dict here
            step.input_links.setdefault(field_name, []).append(source_addr)  # type: ignore

        self._invalidate_pipeline()

    def get_step(self, step_id: str) -> PipelineStep:

        step = self._steps.get(step_id, None)
        if step is None:
            if self._steps:
                msg = f"Available step ids: {', '.join(self._steps.keys())}"
            else:
                msg = "Workflow does not have any steps (yet)."
            raise Exception(f"No step with id '{step_id}' registered. {msg}")
        return step

    def load_state(
        self, workflow_state_id: Union[str, None] = None
    ) -> Union[None, WorkflowState]:
        """
        Load a past state.

        If no state id is specified, the latest one that was saved will be used.

        Returns:
        -------
            'None' if no state was loaded, otherwise the relevant 'WorkflowState' instance
        """
        if workflow_state_id is None:
            if not self._workflow_metadata.workflow_history:
                return None
            else:
                workflow_state_id = self._workflow_metadata.last_state_id

        if workflow_state_id is None:
            raise Exception(
                f"Can't load current state for workflow '{self.workflow_id}': no state available."
            )

        state = self._state_cache.get(workflow_state_id, None)
        if state is not None:
            return state

        state = self._kiara.workflow_registry.get_workflow_state(
            workflow=self.workflow_id, workflow_state_id=workflow_state_id
        )
        assert workflow_state_id == state.instance_id

        self._state_cache[workflow_state_id] = state

        self._all_inputs.clear()
        self._current_pipeline_inputs = None
        self.clear_steps()
        self._invalidate_pipeline()

        self.add_steps(*state.steps)
        # self._workflow_input_aliases = dict(state.input_aliases)
        # self._workflow_output_aliases = dict(state.output_aliases)

        self.set_inputs(**state.inputs)
        assert {k: v for k, v in self._current_pipeline_inputs.items() if v not in [NONE_VALUE_ID, NOT_SET_VALUE_ID]} == {k: v for k, v in state.inputs.items() if v not in [NONE_VALUE_ID, NOT_SET_VALUE_ID]}  # type: ignore
        self._current_pipeline_outputs = (
            state.pipeline_info.pipeline_state.pipeline_outputs
        )
        self._pipeline_info = state.pipeline_info
        self._current_state = state
        self._current_info = None

        return state

    @property
    def all_state_ids(self) -> List[str]:

        hashes = set(self._workflow_metadata.workflow_history.values())
        return sorted(hashes)

    @property
    def all_states(self) -> Mapping[str, WorkflowState]:
        """Return a list of all states this workflow had in the past, indexed by the hash of each state."""
        missing = []
        for state_id in self.workflow_metadata.workflow_history.values():
            if state_id not in self._state_cache.keys():
                missing.append(state_id)

        if missing:
            # TODO: only request missing ones?
            all_states = self._kiara.workflow_registry.get_all_states_for_workflow(
                workflow=self.workflow_id
            )
            self._state_cache.update(all_states)

        return self._state_cache

    @property
    def info(self) -> WorkflowInfo:

        if self._current_info is not None:
            return self._current_info

        self._current_info = WorkflowInfo.create_from_workflow(workflow=self)
        return self._current_info

    @property
    def pipeline_info(self) -> PipelineInfo:

        if self._pipeline_info is not None:
            return self._pipeline_info

        self._pipeline_info = PipelineInfo.create_from_pipeline(
            kiara=self._kiara, pipeline=self.pipeline
        )
        return self._pipeline_info

    def save(self, *aliases: str):

        self._pending_aliases.update(aliases)

        self._workflow_metadata = self._kiara.workflow_registry.register_workflow(
            workflow_metadata=self.workflow_metadata,
            workflow_aliases=self._pending_aliases,
        )
        self._pending_aliases.clear()
        self._metadata_is_stored = True
        self._metadata_is_synced = True

    def snapshot(self, save: bool = False) -> WorkflowState:

        state = self.current_state

        if state.instance_id not in self._state_cache.keys():
            self._state_cache[state.instance_id] = state

        now = get_current_time_incl_timezone()

        for field_name, value in self.current_pipeline_outputs.items():
            if value in [NOT_SET_VALUE_ID, NONE_VALUE_ID]:
                continue

            self._state_output_cache.setdefault(state.instance_id, set()).add(value)
            self._state_jobrecord_cache.setdefault(state.instance_id, set()).add(
                self._job_id_cache[value]
            )

        self.workflow_metadata.workflow_history[now] = state.instance_id
        self._metadata_is_synced = False

        if save:
            self.register_snapshot(snapshot=state.instance_id)
        return state

    def register_snapshot(self, snapshot: Union[datetime, str]):

        timestamps: List[datetime]
        if isinstance(snapshot, str):
            if snapshot not in self._state_cache.keys():
                raise Exception(
                    f"Can't register snapshot with hash '{snapshot}': no state with this hash available."
                )
            state: WorkflowState = self._state_cache[snapshot]
            timestamps = [
                _timestamp
                for _timestamp, _hash in self.workflow_metadata.workflow_history.items()
                if _hash == snapshot
            ]
        elif isinstance(snapshot, datetime):
            if snapshot not in self.workflow_metadata.workflow_history.keys():
                raise Exception(
                    f"Can't register snapshot with timestamp '{snapshot}': no state with this timestamp available."
                )
            state = self._state_cache[self.workflow_metadata.workflow_history[snapshot]]
            timestamps = [snapshot]
        else:
            raise Exception(
                f"Can't register snapshot '{snapshot}': invalid type '{type(snapshot)}'."
            )

        # input values are stored in the add_workflow_state method on the backend

        if state.instance_id in self._state_output_cache.keys():
            for value_id in self._state_output_cache[state.instance_id]:
                self._kiara.data_registry.store_value(value=value_id)

        if state.instance_id in self._state_jobrecord_cache.keys():
            for job_id in self._state_jobrecord_cache[state.instance_id]:
                try:
                    self._kiara.job_registry.store_job_record(job_id=job_id)
                except Exception as e:
                    log_exception(e)

        if not self._metadata_is_stored:
            self._sync_workflow_metadata()
            self._metadata_is_synced = True

        if not self._metadata_is_synced:
            self._kiara.workflow_registry.update_workflow_metadata(
                self.workflow_metadata
            )

        for timestamp in timestamps:
            self._workflow_metadata = self._kiara.workflow_registry.add_workflow_state(
                workflow=self._workflow_metadata.workflow_id,
                workflow_state=state,
                timestamp=timestamp,
            )
            self._metadata_is_synced = True

        return state

    def create_renderable(self, **config: Any):

        if not self._steps:
            return "Invalid workflow: no steps set yet."

        return self.info.create_renderable(**config)
