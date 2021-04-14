# -*- coding: utf-8 -*-

"""Main module."""

import logging
import typing

from kiara.data.registry import DataRegistry
from kiara.mgmt import ModuleManager, PipelineModuleManager, PythonModuleManager

if typing.TYPE_CHECKING:
    from kiara.module import KiaraModule

log = logging.getLogger("kiara")


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
        self._default_pipeline_mgr = PipelineModuleManager()
        module_managers = [self._default_python_mgr, self._default_pipeline_mgr]

        self._module_mgrs: typing.List[ModuleManager] = []
        self._modules: typing.Dict[str, ModuleManager] = {}

        self._data_registry: DataRegistry = DataRegistry()

        for mm in module_managers:
            self.add_module_manager(mm)

    def add_module_manager(self, module_manager: ModuleManager):

        for module_type in module_manager.get_module_types():
            if module_type in self._modules.keys():
                log.warning(
                    f"Duplicate module name '{module_type}'. Ignoring all but the first."
                )
            self._modules[module_type] = module_manager

        self._module_mgrs.append(module_manager)

    @property
    def data_registry(self) -> DataRegistry:
        return self._data_registry

    def get_module_class(self, module_type: str) -> typing.Type["KiaraModule"]:

        mm = self._modules.get(module_type, None)
        if mm is None:
            raise Exception(f"No module '{module_type}' available.")

        cls = mm.get_module_class(module_type)
        if hasattr(cls, "_module_type_id") and cls._module_type_id != module_type:  # type: ignore
            raise Exception(
                f"Can't create module class '{cls}', it already has a _module_type_id attribute and it's different to the module name '{module_type}'."
            )
        setattr(cls, "_module_type_id", module_type)
        return cls

    @property
    def available_module_types(self) -> typing.List[str]:
        """Return the names of all available modules"""
        return sorted(set(self._modules.keys()))

    @property
    def available_non_pipeline_module_types(self) -> typing.List[str]:
        """Return the names of all available pipeline-type modules."""

        return [
            module_type
            for module_type in self.available_module_types
            if not self.get_module_class(module_type).is_pipeline()
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
        module_config: typing.Mapping[str, typing.Any],
        parent_id: typing.Optional[str] = None,
    ) -> "KiaraModule":

        mm = self._modules.get(module_type, None)
        if mm is None:
            raise Exception(f"No module '{module_type}' available.")

        _ = self.get_module_class(
            module_type
        )  # just to make sure the _module_type_id attribute is added

        return mm.create_module(
            id=id,
            parent_id=parent_id,
            module_type=module_type,
            module_config=module_config,
        )
