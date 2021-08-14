# -*- coding: utf-8 -*-

"""Main module."""
import logging
import os
import typing
import zmq
from pathlib import Path
from threading import Thread
from zmq import Context
from zmq.devices import ThreadDevice

from kiara.config import KiaraConfig
from kiara.data import Value, ValueSet
from kiara.data.registry import DataRegistry
from kiara.data.store import LocalDataStore
from kiara.data.types import ValueType
from kiara.data.types.type_mgmt import TypeMgmt
from kiara.interfaces import get_console
from kiara.metadata.mgmt import MetadataMgmt
from kiara.metadata.module_models import KiaraModuleTypeMetadata
from kiara.module_config import ModuleConfig
from kiara.module_mgmt import ModuleManager
from kiara.module_mgmt.merged import MergedModuleManager
from kiara.operations import Operation, OperationMgmt
from kiara.pipeline.config import PipelineConfig
from kiara.pipeline.controller import PipelineController
from kiara.pipeline.pipeline import Pipeline
from kiara.processing import Job
from kiara.processing.processor import ModuleProcessor
from kiara.utils import (
    create_valid_identifier,
    get_auto_workflow_alias,
    get_data_from_file,
    is_debug,
)
from kiara.workflow.kiara_workflow import KiaraWorkflow

if typing.TYPE_CHECKING:
    from kiara.module import KiaraModule
    from kiara.operations.pretty_print import PrettyPrintOperationType

log = logging.getLogger("kiara")


def explain(item: typing.Any):

    if isinstance(item, type):
        from kiara.module import KiaraModule

        if issubclass(item, KiaraModule):
            item = KiaraModuleTypeMetadata.from_module_class(module_cls=item)

    elif isinstance(item, Value):
        item.get_metadata()

    console = get_console()
    console.print(item)


class Kiara(object):
    _instance = None

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = Kiara()
        return cls._instance

    def __init__(self, config: typing.Optional[KiaraConfig] = None):

        if not config:
            config = KiaraConfig()

        self._config: KiaraConfig = config

        self._zmq_context: Context = Context.instance()

        self._operation_mgmt = OperationMgmt(kiara=self)
        self._metadata_mgmt = MetadataMgmt(kiara=self)
        self._data_store = LocalDataStore(kiara=self, base_path=config.data_store)

        # self.start_zmq_device()
        # self.start_log_thread()

        self._default_processor: ModuleProcessor = ModuleProcessor.from_config(
            config.default_processor
        )

        self._type_mgmt_obj: TypeMgmt = TypeMgmt(self)

        self._data_registry: DataRegistry = DataRegistry(self)

        self._module_mgr: MergedModuleManager = MergedModuleManager(
            config.module_managers
        )

    @property
    def config(self) -> KiaraConfig:
        return self._config

    @property
    def default_processor(self) -> "ModuleProcessor":
        return self._default_processor

    def start_zmq_device(self):

        pd = ThreadDevice(zmq.QUEUE, zmq.SUB, zmq.PUB)
        pd.bind_in("inproc://kiara_in")
        pd.bind_out("inproc://kiara_out")
        pd.setsockopt_in(zmq.SUBSCRIBE, b"")
        pd.setsockopt_in(zmq.IDENTITY, b"SUB")
        pd.setsockopt_out(zmq.IDENTITY, b"PUB")
        pd.start()

    def start_log_thread(self):
        def log_messages():
            socket = self._zmq_context.socket(zmq.SUB)
            socket.setsockopt_string(zmq.SUBSCRIBE, "")
            socket.connect("inproc://kiara_out")

            debug = is_debug()

            while True:
                message = socket.recv()
                topic, details = message.decode().split(" ", maxsplit=1)
                try:
                    job = Job.parse_raw(details)
                    if debug:
                        print(f"{topic}: {job.pipeline_name}.{job.step_id}")
                    else:
                        log.debug(f"{topic}: {job.pipeline_name}.{job.step_id}")

                except Exception as e:
                    if debug:
                        import traceback

                        traceback.print_exception()
                    else:
                        log.debug(e)

        t = Thread(target=log_messages, daemon=True)
        t.start()

    def explain(self, item: typing.Any):
        explain(item)

    @property
    def type_mgmt(self) -> TypeMgmt:

        return self._type_mgmt_obj

    @property
    def data_store(self) -> LocalDataStore:
        return self._data_store

    @property
    def module_mgmt(self) -> MergedModuleManager:
        return self._module_mgr

    @property
    def metadata_mgmt(self) -> MetadataMgmt:
        return self._metadata_mgmt

    @property
    def value_types(self) -> typing.Mapping[str, typing.Type[ValueType]]:
        return self.type_mgmt.value_types

    @property
    def value_type_names(self) -> typing.List[str]:
        return self.type_mgmt.value_type_names

    def determine_type(self, data: typing.Any) -> typing.Optional[ValueType]:

        raise NotImplementedError()

        # return self.type_mgmt.determine_type(data)

    def get_value_type_cls(self, type_name: str) -> typing.Type[ValueType]:

        return self.type_mgmt.get_value_type_cls(type_name=type_name)

    def add_module_manager(self, module_manager: ModuleManager):

        self._module_mgr.add_module_manager(module_manager)
        self._type_mgmt_obj.invalidate_types()

    @property
    def data_registry(self) -> DataRegistry:
        return self._data_registry

    @property
    def operation_mgmt(self) -> OperationMgmt:
        return self._operation_mgmt

    def get_module_class(self, module_type: str) -> typing.Type["KiaraModule"]:
        return self._module_mgr.get_module_class(module_type=module_type)

    # def get_module_info(self, module_type: str) -> "ModuleInfo":
    #
    #     if module_type not in self.available_module_types:
    #         raise ValueError(f"Module type '{module_type}' not available.")
    #
    #     if module_type in self.available_pipeline_module_types:
    #         from kiara.pipeline.module import PipelineModuleInfo
    #
    #         info = PipelineModuleInfo(module_type=module_type, _kiara=self)  # type: ignore
    #         return info
    #     else:
    #         from kiara.module import ModuleInfo
    #
    #         info = ModuleInfo.from_module_cls(module_cls=module_type)
    #         return info

    @property
    def available_module_types(self) -> typing.List[str]:
        """Return the names of all available modules"""
        return self._module_mgr.available_module_types

    @property
    def available_non_pipeline_module_types(self) -> typing.List[str]:
        """Return the names of all available pipeline-type modules."""

        return self._module_mgr.available_non_pipeline_module_types

    @property
    def available_pipeline_module_types(self) -> typing.List[str]:
        """Return the names of all available pipeline-type modules."""

        return self._module_mgr.available_pipeline_module_types

    def is_pipeline_module(self, module_type: str):

        return self._module_mgr.is_pipeline_module(module_type=module_type)

    def register_pipeline_description(
        self,
        data: typing.Union[Path, str, typing.Mapping[str, typing.Any]],
        module_type_name: typing.Optional[str] = None,
        namespace: typing.Optional[str] = None,
        raise_exception: bool = False,
    ) -> typing.Optional[str]:

        return self._module_mgr.register_pipeline_description(
            data=data,
            module_type_name=module_type_name,
            namespace=namespace,
            raise_exception=raise_exception,
        )

    def create_module(
        self,
        module_type: str,
        module_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        id: typing.Optional[str] = None,
        parent_id: typing.Optional[str] = None,
    ) -> "KiaraModule":
        """Create a module instance.

        The 'module_type' argument can be a module type id, or an operation id. In case of the latter, the 'real' module_type
        and module_config will be resolved via the operation management.

        Arguments:
            module_type: the module type- or operation-id
            module_config: the module instance configuration (must be empty in case of the module_type being an operation id
            id: the id of this module, only relevant if the module is part of a pipeline, in which case it must be unique within the pipeline
            parent_id: a reference to the pipeline that contains this module (if applicable)

        Returns:
            The instantiated module object.
        """

        if module_type == "pipeline":
            from kiara import PipelineModule

            module_cls = PipelineModule
            module = module_cls(
                id=id, parent_id=parent_id, module_config=module_config, kiara=self
            )
            return module

        elif (
            module_type not in self.available_module_types
            and module_type in self.operation_mgmt.operation_ids
        ):
            if module_config:
                raise NotImplementedError()
            op = self.operation_mgmt.get_operation(module_type)
            assert op is not None
            module_type = op.module_type
            module_config = op.module_config

        return self._module_mgr.create_module(
            kiara=self,
            id=id,
            module_type=module_type,
            module_config=module_config,
            parent_id=parent_id,
        )

    def get_module_doc(
        self,
        module_type: str,
        module_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    ):

        m = self.create_module(module_type=module_type, module_config=module_config)
        return m.module_instance_doc

    def get_operation(self, operation_id: str) -> Operation:

        op = self.operation_mgmt.profiles.get(operation_id, None)
        if op is None:
            raise Exception(f"No operation with id '{operation_id}' available.")

        return op

    def run(
        self,
        module_type: typing.Union[str, Operation],
        module_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        inputs: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        output_name: typing.Optional[str] = None,
        resolve_result: bool = False,
    ) -> typing.Union[ValueSet, Value, typing.Any]:

        if isinstance(module_type, str):
            if module_type in self.available_module_types:
                module = self.create_module(
                    module_type=module_type, module_config=module_config
                )
            elif module_type in self.operation_mgmt.profiles.keys():
                if module_config:
                    raise NotImplementedError()
                op = self.operation_mgmt.profiles[module_type]
                module = op.module
            else:
                raise Exception(
                    f"Can't run operation: invalid module type '{module_type}'"
                )
        elif isinstance(module_type, Operation):
            if module_config:
                raise NotImplementedError()
            module = module_type.module

        return self.run_module(
            module=module,
            inputs=inputs,
            output_name=output_name,
            resolve_result=resolve_result,
        )

    def run_module(
        self,
        module: "KiaraModule",
        inputs: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        output_name: typing.Optional[str] = None,
        resolve_result: bool = False,
    ):

        if inputs is None:
            inputs = {}
        result = module.run(**inputs)
        if output_name is not None:
            v = result.get_value_obj(output_name)
            if resolve_result:
                return v.get_value_data()
            else:
                return v
        else:
            if resolve_result:
                return result.get_all_value_data()
            else:
                return result

    def create_pipeline(
        self,
        config: typing.Union[ModuleConfig, typing.Mapping[str, typing.Any], str],
        controller: typing.Optional[PipelineController] = None,
    ) -> Pipeline:

        if isinstance(config, typing.Mapping):
            pipeline_config: PipelineConfig = PipelineConfig(**config)

        elif isinstance(config, str):
            if config == "pipeline":
                raise Exception(
                    "Can't create pipeline from 'pipeline' module type without further configuration."
                )

            if config in self.available_module_types:
                config_data = {
                    "steps": [
                        {
                            "module_type": config,
                            "step_id": create_valid_identifier(config),
                        }
                    ]
                }
                pipeline_config = PipelineConfig(**config_data)
            elif os.path.isfile(os.path.expanduser(config)):
                path = os.path.expanduser(config)
                pipeline_config_data = get_data_from_file(path)
                pipeline_config = PipelineConfig(**pipeline_config_data)
            else:
                raise Exception(
                    f"Can't create pipeline config from string '{config}'. Value must be path to a file, or one of: {', '.join(self.available_module_types)}"
                )
        elif isinstance(config, PipelineConfig):
            pipeline_config = config
        else:
            raise TypeError(
                f"Invalid type '{type(config)}' for pipeline configuration."
            )

        pipeline = pipeline_config.create_pipeline(controller=controller, kiara=self)
        return pipeline

    def create_workflow_from_operation_config(
        self,
        config: "ModuleConfig",
        workflow_id: typing.Optional[str] = None,
        controller: typing.Optional[PipelineController] = None,
    ):

        if not workflow_id:
            workflow_id = get_auto_workflow_alias(
                config.module_type, use_incremental_ids=True
            )

        workflow = KiaraWorkflow(
            workflow_id=workflow_id,
            config=config,
            controller=controller,
            kiara=self,
        )
        return workflow

    def create_workflow(
        self,
        config: typing.Union[ModuleConfig, typing.Mapping[str, typing.Any], str],
        workflow_id: typing.Optional[str] = None,
        module_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        controller: typing.Optional[PipelineController] = None,
    ) -> KiaraWorkflow:

        _config = ModuleConfig.create_module_config(
            config=config, module_config=module_config, kiara=self
        )
        return self.create_workflow_from_operation_config(
            config=_config, workflow_id=workflow_id, controller=controller
        )

    def pretty_print(
        self,
        value: Value,
        target_type: str = "renderables",
        print_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    ) -> typing.Any:

        pretty_print_ops: PrettyPrintOperationType = self.operation_mgmt.get_operations("pretty_print")  # type: ignore

        return pretty_print_ops.pretty_print(
            value=value, target_type=target_type, print_config=print_config
        )
