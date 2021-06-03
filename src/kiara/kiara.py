# -*- coding: utf-8 -*-

"""Main module."""
import logging
import os
import typing
import zmq
from threading import Thread
from zmq import Context
from zmq.devices import ThreadDevice

from kiara.config import KiaraConfig
from kiara.data import Value, ValueSet
from kiara.data.persistence import DataStore
from kiara.data.registry import DataRegistry
from kiara.data.types import ValueType
from kiara.data.types.type_mgmt import TypeMgmt
from kiara.interfaces import get_console
from kiara.module_config import KiaraWorkflowConfig, PipelineModuleConfig
from kiara.module_mgmt import ModuleManager
from kiara.module_mgmt.merged import MergedModuleManager
from kiara.pipeline.controller import PipelineController
from kiara.pipeline.pipeline import Pipeline
from kiara.processing import Job, ModuleProcessor
from kiara.profiles import ModuleProfileMgmt
from kiara.utils import get_auto_workflow_alias, get_data_from_file, is_debug
from kiara.workflow import KiaraWorkflow

if typing.TYPE_CHECKING:
    from kiara.module import KiaraModule, ModuleInfo, ModulesList

log = logging.getLogger("kiara")


def explain(item: typing.Any, kiara: typing.Optional["Kiara"] = None):

    if kiara is None:
        kiara = Kiara.instance()

    if isinstance(item, type):
        from kiara.module import KiaraModule, ModuleInfo

        if issubclass(item, KiaraModule):
            item = ModuleInfo(module_type=item._module_type_id, _kiara=kiara)  # type: ignore
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

        self._profile_mgmt = ModuleProfileMgmt(kiara=self)

        self._data_store = DataStore(kiara=self)

        self.start_zmq_device()
        self.start_log_thread()

        self._default_processor: ModuleProcessor = ModuleProcessor.from_config(
            config.default_processor
        )

        self._type_mgmt_obj: TypeMgmt = TypeMgmt(self)

        self._data_registry: DataRegistry = DataRegistry(self)
        self._module_mgr: MergedModuleManager = MergedModuleManager(
            config.module_managers
        )

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

        explain(item, kiara=self)

    @property
    def type_mgmt(self) -> TypeMgmt:

        return self._type_mgmt_obj

    @property
    def data_store(self) -> DataStore:
        return self._data_store

    @property
    def value_types(self) -> typing.Mapping[str, typing.Type[ValueType]]:
        return self.type_mgmt.value_types

    @property
    def value_type_names(self) -> typing.List[str]:
        return self.type_mgmt.value_type_names

    def determine_type(self, data: typing.Any) -> typing.Optional[ValueType]:

        return self.type_mgmt.determine_type(data)

    def get_metadata_keys_for_type(self, value_type: str) -> typing.Iterable[str]:

        all_profiles_for_type = self._profile_mgmt.extract_metadata_profiles.get(
            value_type, None
        )
        if not all_profiles_for_type:
            return []
        else:
            return all_profiles_for_type.keys()

    def get_value_metadata(
        self, value: Value, *metadata_keys: str, also_return_schema: bool = False
    ):

        value_type = value.value_schema.type
        # TODO: validate type exists

        all_profiles_for_type = self._profile_mgmt.extract_metadata_profiles.get(
            value_type, None
        )
        if all_profiles_for_type is None:
            all_profiles_for_type = {}

        if not metadata_keys:
            _metadata_keys = set(all_profiles_for_type.keys())
            for key in value.metadata.keys():
                _metadata_keys.add(key)
        elif isinstance(metadata_keys, str):
            _metadata_keys = set(metadata_keys)
        else:
            _metadata_keys = set(metadata_keys)

        result = {}
        missing = []

        for md_key in _metadata_keys:
            if md_key not in value.metadata.keys():
                missing.append(md_key)
            else:
                result[md_key] = value.metadata[md_key]

        for mk in missing:
            if not all_profiles_for_type or mk not in all_profiles_for_type:
                raise Exception(
                    f"Can't extract metadata profile '{mk}' for type '{value_type}': metadata profile does not exist (for this type, anyway)."
                )
            profile = all_profiles_for_type[mk]
            module = profile.create_module(kiara=self)
            metadata_result = module.run(value=value)
            result[mk] = metadata_result.get_all_value_data()

        if also_return_schema:
            return result
        else:
            return {k: v["metadata"] for k, v in result.items()}

    def get_value_type_cls(self, type_name: str) -> typing.Type[ValueType]:

        return self.type_mgmt.get_value_type_cls(type_name=type_name)

    def save_value(self, value: Value) -> str:
        return self._data_store.save_value(value)

    def transform_data(
        self,
        data: typing.Any,
        target_type: str,
        source_type: typing.Optional[str] = None,
        config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        register_result: bool = False,
    ) -> Value:

        if register_result:
            raise NotImplementedError()

        if not source_type:
            if isinstance(data, Value):
                source_type = data.type_name
            else:
                _source_type = self.type_mgmt.determine_type(data)
                if not _source_type:
                    raise Exception(
                        f"Can't transform data to '{target_type}': can not determine source type."
                    )
                source_type = _source_type.type_name()

        module = self._profile_mgmt.get_type_conversion_module(
            source_type=source_type, target_type=target_type  # type: ignore
        )
        from kiara.modules.type_conversion import TypeConversionModule

        if isinstance(module, TypeConversionModule):

            result = module.run(source_value=data, config=config)
            return result.get_value_obj("target_value")

        else:
            raise NotImplementedError()

    def get_convert_target_types(self, source_type: str) -> typing.Iterable[str]:

        return self._profile_mgmt.type_convert_profiles.get(source_type, [])

    def add_module_manager(self, module_manager: ModuleManager):

        self._module_mgr.add_module_manager(module_manager)
        self._type_mgmt_obj.invalidate_types()

    @property
    def data_registry(self) -> DataRegistry:
        return self._data_registry

    def get_module_class(self, module_type: str) -> typing.Type["KiaraModule"]:
        return self._module_mgr.get_module_class(module_type=module_type)

    def get_module_info(self, module_type: str) -> "ModuleInfo":

        if module_type not in self.available_module_types:
            raise ValueError(f"Module type '{module_type}' not available.")

        if module_type in self.available_pipeline_module_types:
            from kiara.pipeline.module import PipelineModuleInfo

            info = PipelineModuleInfo(module_type=module_type, _kiara=self)  # type: ignore
            return info
        else:
            from kiara.module import ModuleInfo

            info = ModuleInfo(module_type=module_type)  # type: ignore
            return info

    @property
    def available_module_types(self) -> typing.List[str]:
        """Return the names of all available modules"""
        return self._module_mgr.available_module_types

    @property
    def modules_list(self) -> "ModulesList":
        """Return an object that contains a list of all available modules, and their details."""

        return self.create_modules_list()

    def create_modules_list(
        self, list_non_pipeline_modules: bool = True, list_pipeline_modules: bool = True
    ) -> "ModulesList":

        if list_non_pipeline_modules and list_pipeline_modules:
            module_names = self.available_module_types
        elif list_non_pipeline_modules:
            module_names = self.available_non_pipeline_module_types
        elif list_pipeline_modules:
            module_names = self.available_pipeline_module_types
        else:
            module_names = []

        try:
            module_names.remove("pipeline")
        except ValueError:
            pass

        from kiara.module import ModulesList

        return ModulesList(kiara=self, modules=module_names)

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

    def create_module(
        self,
        id: str,
        module_type: str,
        module_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        parent_id: typing.Optional[str] = None,
    ) -> "KiaraModule":

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

        m = self.create_module(
            id="_", module_type=module_type, module_config=module_config
        )
        return m.doc()

    def run(
        self,
        module_type: str,
        module_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        inputs: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        output_name: typing.Optional[str] = None,
    ) -> typing.Union[ValueSet, Value]:

        module = self.create_module(
            "_", module_type=module_type, module_config=module_config
        )
        if inputs is None:
            inputs = {}
        result = module.run(**inputs)
        if output_name is not None:
            return result.get_value_obj(output_name)
        else:
            return result

    def create_pipeline(
        self,
        config: typing.Union[KiaraWorkflowConfig, typing.Mapping[str, typing.Any], str],
        controller: typing.Optional[PipelineController] = None,
    ) -> Pipeline:

        if isinstance(config, typing.Mapping):
            pipeline_config: PipelineModuleConfig = PipelineModuleConfig(**config)

        elif isinstance(config, str):
            if config == "pipeline":
                raise Exception(
                    "Can't create pipeline from 'pipeline' module type without further configuration."
                )

            if config in self.available_module_types:
                config_data = {"steps": [{"module_type": config, "step_id": config}]}
                pipeline_config = PipelineModuleConfig(**config_data)
            elif os.path.isfile(os.path.expanduser(config)):
                path = os.path.expanduser(config)
                pipeline_config_data = get_data_from_file(path)
                pipeline_config = PipelineModuleConfig(**pipeline_config_data)
            else:
                raise Exception(
                    f"Can't create pipeline config from string '{config}'. Value must be path to a file, or one of: {', '.join(self.available_module_types)}"
                )
        elif isinstance(config, PipelineModuleConfig):
            pipeline_config = config
        else:
            raise TypeError(
                f"Invalid type '{type(config)}' for pipeline configuration."
            )

        pipeline = pipeline_config.create_pipeline(controller=controller, kiara=self)
        return pipeline

    def create_workflow(
        self,
        config: typing.Union[KiaraWorkflowConfig, typing.Mapping[str, typing.Any], str],
        workflow_id: typing.Optional[str] = None,
        module_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        controller: typing.Optional[PipelineController] = None,
    ) -> KiaraWorkflow:

        if isinstance(config, typing.Mapping):
            workflow_config: KiaraWorkflowConfig = KiaraWorkflowConfig(**config)

            if module_config:
                raise NotImplementedError()

        elif isinstance(config, str):
            if config == "pipeline":
                raise Exception(
                    "Can't create workflow from 'pipeline' module type without further configuration."
                )

            if config in self.available_module_types:
                if module_config:
                    workflow_config = KiaraWorkflowConfig(
                        module_type=config, module_config=module_config
                    )
                else:
                    workflow_config = KiaraWorkflowConfig(module_type=config)

            elif os.path.isfile(os.path.expanduser(config)):
                path = os.path.expanduser(config)
                workflow_config_data = get_data_from_file(path)

                if module_config:
                    raise NotImplementedError()

                workflow_config = KiaraWorkflowConfig(
                    module_config=workflow_config_data, module_type="pipeline"
                )
            else:
                raise Exception(
                    f"Can't create workflow config from string '{config}'. Value must be path to a file, or one of: {', '.join(self.available_module_types)}"
                )
        elif isinstance(config, KiaraWorkflowConfig):
            workflow_config = config
            if module_config:
                raise NotImplementedError()
        else:
            raise TypeError(
                f"Invalid type '{type(config)}' for workflow configuration."
            )

        if not workflow_id:
            workflow_id = get_auto_workflow_alias(
                workflow_config.module_type, use_incremental_ids=True
            )

        workflow = KiaraWorkflow(
            workflow_id=workflow_id,
            config=workflow_config,
            controller=controller,
            kiara=self,
        )
        return workflow
