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
    @classmethod
    def from_config(
        cls,
        config: typing.Union[
            typing.Mapping[str, typing.Any],
            "PipelineModuleManagerConfig",
            "PythonModuleManagerConfig",
        ],
    ) -> "ModuleManager":

        raise NotImplementedError()

        from kiara.modules.mgmt.pipelines import (  # PipelineModuleManager,
            PipelineModuleManagerConfig,
        )
        from kiara.modules.mgmt.python_classes import (
            PythonModuleManager,
            PythonModuleManagerConfig,
        )

        if isinstance(config, typing.Mapping):
            mm_type = config.get("module_manager_type", None)
            if not mm_type:
                raise ValueError(f"No module manager type provided in config: {config}")
            if mm_type == "python":
                config = PythonModuleManagerConfig(**config)
            # elif mm_type == "pipeline":
            #     config = PipelineModuleManagerConfig(**config)
            else:
                raise ValueError(f"Invalid module manager type: {mm_type}")

        if config.module_manager_type == "python":
            mm: ModuleManager = PythonModuleManager(
                **config.dict(exclude={"module_manager_type"})
            )
        else:
            raise NotImplementedError()
        # elif config.module_manager_type == "pipeline":
        #     mm = PipelineModuleManager(**config.dict(exclude={"module_manager_type"}))

        return mm

    @abc.abstractmethod
    def get_module_type_names(self) -> typing.Iterable[str]:
        pass

    @abc.abstractmethod
    def get_module_class(self, module_type: str) -> typing.Type["KiaraModule"]:
        pass

    @property
    def module_types(self) -> typing.Mapping[str, typing.Type["KiaraModule"]]:
        return {tn: self.get_module_class(tn) for tn in self.get_module_type_names()}

    # def create_module_config(
    #     self, module_type: str, module_config: typing.Mapping[str, typing.Any]
    # ) -> "KiaraModuleConfig":
    #
    #     cls = self.get_module_class(module_type)
    #     config = cls._config_cls(**module_config)
    #
    #     return config
    #
    # def create_module_instance(
    #     self,
    #     module_type: str,
    #     module_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    # ) -> "ModuleConfig":
    #
    #     if module_type not in self.get_module_type_names():
    #         raise Exception(f"Can't create instance of module type '{module_type}': type not available. Available data_types: {', '.join(self.get_module_type_names())}.")
    #
    #     if module_config is None:
    #         mi = ModuleConfig(module_type=module_type)
    #     else:
    #         mi = ModuleConfig(module_type=module_type, module_config=module_config)
    #
    #     return mi
