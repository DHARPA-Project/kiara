# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Base module for code that handles the import and management of [KiaraModule][kiara.module.KiaraModule] sub-classes."""

import abc
import structlog
import typing

if typing.TYPE_CHECKING:
    from kiara import KiaraModule

    # from kiara.modules.mgmt.pipelines import PipelineModuleManagerConfig
    from kiara.modules.mgmt.python_classes import PythonModuleManagerConfig


logget = structlog.getLogger()


class ModuleManager(abc.ABC):

    @abc.abstractmethod
    def get_module_type_names(self) -> typing.Iterable[str]:
        pass

    @abc.abstractmethod
    def get_module_class(self, module_type: str) -> typing.Type["KiaraModule"]:
        pass

    @property
    def module_types(self) -> typing.Mapping[str, typing.Type["KiaraModule"]]:
        return {tn: self.get_module_class(tn) for tn in self.get_module_type_names()}

    @property
    def module_type_names(self) -> typing.List[str]:
        return sorted(self.module_types.keys())

    @abc.abstractmethod
    def find_modules_for_package(
        self,
        package_name: str,
        include_core_modules: bool = True,
        include_pipelines: bool = True,
    ) -> typing.Dict[str, typing.Type["KiaraModule"]]:
        pass


class DefaultModuleManager(ModuleManager):

    def __init__(
        self
    ):

        from kiara.utils.class_loading import find_all_kiara_modules
        module_classes = find_all_kiara_modules()

        self._module_classes: typing.Mapping[str, typing.Type[KiaraModule]] = {}

        for k, v in module_classes.items():
            self._module_classes[k] = v

    @property
    def module_types(self) -> typing.Mapping[str, typing.Type["KiaraModule"]]:
        return self._module_classes

    def get_module_class(self, module_type: str) -> typing.Type["KiaraModule"]:

        cls = self._module_classes.get(module_type, None)
        if cls is None:
            raise ValueError(f"No module of type '{module_type}' available.")
        return cls

    def get_module_type_names(self) -> typing.Iterable[str]:
        return self._module_classes.keys()

    def find_modules_for_package(
        self,
        package_name: str,
        include_core_modules: bool = True,
        include_pipelines: bool = True,
    ) -> typing.Dict[str, typing.Type["KiaraModule"]]:

        result = {}
        for module_type in self.module_type_names:

            if module_type == "pipeline":
                continue
            module_cls = self.get_module_class(module_type)

            module_package = module_cls.get_type_metadata().context.labels.get(
                "package", None
            )
            if module_package != package_name:
                continue
            if module_cls.is_pipeline():
                if include_pipelines:
                    result[module_type] = module_cls
            else:
                if include_core_modules:
                    result[module_type] = module_cls

        return result
