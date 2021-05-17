# -*- coding: utf-8 -*-

"""Main module."""

import logging
import os
import typing
from pathlib import Path

from kiara.config import KiaraWorkflowConfig, PipelineModuleConfig
from kiara.data.registry import DataRegistry
from kiara.data.types import ValueType
from kiara.interfaces import get_console
from kiara.mgmt import ModuleManager, PipelineModuleManager, PythonModuleManager
from kiara.pipeline.controller import PipelineController
from kiara.pipeline.pipeline import Pipeline
from kiara.utils import get_auto_workflow_alias, get_data_from_file
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

    console = get_console()
    console.print(item)


class Kiara(object):
    _instance = None

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = Kiara()
        return cls._instance

    def __init__(
        self, module_managers: typing.Optional[typing.Iterable[ModuleManager]] = None
    ):

        self._default_python_mgr = PythonModuleManager()
        self._default_pipeline_mgr = PipelineModuleManager(folders=None)
        self._custom_pipelines_mgr = PipelineModuleManager(folders=[])

        _mms = [
            self._default_python_mgr,
            self._default_pipeline_mgr,
            self._custom_pipelines_mgr,
        ]
        if module_managers:
            _mms.extend(module_managers)

        self._module_mgrs: typing.List[ModuleManager] = [
            self._default_python_mgr,
            self._default_pipeline_mgr,
        ]
        self._modules: typing.Dict[str, ModuleManager] = {}
        self._value_types: typing.Optional[
            typing.Dict[str, typing.Type[ValueType]]
        ] = None

        self._data_registry: DataRegistry = DataRegistry(self)

        for mm in _mms:
            self.add_module_manager(mm)

    def explain(self, item: typing.Any):

        explain(item, kiara=self)

    @property
    def value_types(self) -> typing.Mapping[str, typing.Type[ValueType]]:

        if self._value_types is not None:
            return self._value_types

        all_value_type_classes = ValueType.__subclasses__()
        value_type_dict: typing.Dict[str, typing.Type[ValueType]] = {}
        for cls in all_value_type_classes:
            type_name = cls.type_name()
            if type_name in value_type_dict.keys():
                raise Exception(
                    f"Can't initiate types: duplicate type name '{type_name}'"
                )
            value_type_dict[type_name] = cls

        self._value_types = value_type_dict
        return self._value_types

    @property
    def value_type_names(self) -> typing.List[str]:
        return list(self.value_types.keys())

    def get_value_type_cls(self, type_name: str) -> typing.Type[ValueType]:

        t = self.value_types.get(type_name, None)
        if t is None:
            raise Exception(
                f"No value type '{type_name}', available types: {', '.join(self.value_types.keys())}"
            )
        return t

    def add_module_manager(self, module_manager: ModuleManager):

        for module_type in module_manager.get_module_types():
            if module_type in self._modules.keys():
                log.warning(
                    f"Duplicate module name '{module_type}'. Ignoring all but the first."
                )
                continue
            self._modules[module_type] = module_manager

        self._module_mgrs.append(module_manager)
        self._value_types = None

    def add_pipeline_folder(self, folder: typing.Union[Path, str]) -> typing.List[str]:

        if isinstance(folder, str):
            folder = Path(os.path.expanduser(folder))

        if not folder.is_dir():
            raise Exception(
                f"Can't add pipeline folder '{folder.as_posix()}': not a directory"
            )

        added = self._custom_pipelines_mgr.add_pipelines_path(folder)
        result = []
        for a in added:
            if a in self._modules.keys():
                log.warning(f"Duplicate module name '{a}'. Ignoring all but the first.")
                continue
            self._modules[a] = self._custom_pipelines_mgr
            result.append(a)

        return result

    def register_pipeline_description(
        self,
        data: typing.Union[Path, str, typing.Mapping[str, typing.Any]],
        module_type_name: typing.Optional[str] = None,
        raise_exception: bool = False,
    ) -> typing.Optional[str]:

        name = self._custom_pipelines_mgr.register_pipeline(
            data, module_type_name=module_type_name
        )
        if name in self._modules.keys():
            if raise_exception:
                raise Exception(f"Duplicate module name: {name}")
            log.warning(f"Duplicate module name '{name}'. Ignoring all but the first.")
            return None
        else:
            self._modules[name] = self._custom_pipelines_mgr
            return name

    @property
    def data_registry(self) -> DataRegistry:
        return self._data_registry

    def get_module_class(self, module_type: str) -> typing.Type["KiaraModule"]:

        if module_type == "pipeline":
            from kiara import PipelineModule

            return PipelineModule

        mm = self._modules.get(module_type, None)
        if mm is None:
            raise Exception(f"No module '{module_type}' available.")

        cls = mm.get_module_class(module_type)
        if hasattr(cls, "_module_type_id") and cls._module_type_id != "pipeline" and cls._module_type_id != module_type:  # type: ignore
            raise Exception(
                f"Can't create module class '{cls}', it already has a _module_type_id attribute and it's different to the module name '{module_type}': {cls._module_type_id}"  # type: ignore
            )

        setattr(cls, "_module_type_id", module_type)
        return cls

    def get_module_info(self, module_type: str) -> "ModuleInfo":

        if module_type not in self.available_module_types:
            raise ValueError(f"Module type '{module_type}' not available.")

        if module_type in self.available_pipeline_module_types:
            from kiara.pipeline.module import PipelineModuleInfo

            info = PipelineModuleInfo(module_type=module_type, _kiara=self)  # type: ignore
            return info
        else:
            from kiara.module import ModuleInfo

            info = ModuleInfo(module_type=module_type, _kiara=self)  # type: ignore
            return info

    @property
    def available_module_types(self) -> typing.List[str]:
        """Return the names of all available modules"""
        return sorted(set(self._modules.keys()))

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

        return [
            module_type
            for module_type in self.available_module_types
            if module_type != "pipeline"
            and not self.get_module_class(module_type).is_pipeline()
        ]

    @property
    def available_pipeline_module_types(self) -> typing.List[str]:
        """Return the names of all available pipeline-type modules."""

        return [
            module_type
            for module_type in self.available_module_types
            if module_type != "pipeline"
            and self.get_module_class(module_type).is_pipeline()
        ]

    def is_pipeline_module(self, module_type: str):

        cls = self.get_module_class(module_type=module_type)
        return cls.is_pipeline()

    def create_module(
        self,
        id: str,
        module_type: str,
        module_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        parent_id: typing.Optional[str] = None,
    ) -> "KiaraModule":

        mm = self._modules.get(module_type, None)
        if mm is None:
            raise Exception(
                f"No module '{module_type}' registered. Available modules: {', '.join(self.available_module_types)}"
            )

        _ = self.get_module_class(
            module_type
        )  # just to make sure the _module_type_id attribute is added

        return mm.create_module(
            id=id,
            parent_id=parent_id,
            module_type=module_type,
            module_config=module_config,
            kiara=self,
        )

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
            # raise TypeError(f"Invalid type '{type(workflow_config)}' for workflow configuration: {workflow_config}")
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
            # raise TypeError(f"Invalid type '{type(workflow_config)}' for workflow configuration: {workflow_config}")
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
