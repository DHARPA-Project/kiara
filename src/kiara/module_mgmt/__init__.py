# -*- coding: utf-8 -*-
"""Base module for code that handles the import and management of [KiaraModule][kiara.module.KiaraModule] sub-classes."""

import abc
import logging
import typing

if typing.TYPE_CHECKING:
    from kiara import Kiara, KiaraModule
    from kiara.module_config import KiaraModuleConfig
    from kiara.module_mgmt.pipelines import PipelineModuleManagerConfig
    from kiara.module_mgmt.python_classes import (
        PythonModuleManager,
        PythonModuleManagerConfig,
    )


log = logging.getLogger("kiara")


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

        from kiara.module_mgmt.pipelines import (
            PipelineModuleManager,
            PipelineModuleManagerConfig,
        )
        from kiara.module_mgmt.python_classes import (
            PythonModuleManager,
            PythonModuleManagerConfig,
        )

        if isinstance(config, typing.Mapping):
            mm_type = config.get("module_manager_type", None)
            if not mm_type:
                raise ValueError(f"No module manager type provided in config: {config}")
            if mm_type == "python":
                config = PythonModuleManagerConfig(**config)
            elif mm_type == "pipeline":
                config = PipelineModuleManagerConfig(**config)
            else:
                raise ValueError(f"Invalid module manager type: {mm_type}")

        if config.module_manager_type == "python":
            mm: ModuleManager = PythonModuleManager(
                **config.dict(exclude={"module_manager_type"})
            )
        elif config.module_manager_type == "pipeline":
            mm = PipelineModuleManager(**config.dict(exclude={"module_manager_type"}))

        return mm

    @abc.abstractmethod
    def get_module_types(self) -> typing.Iterable[str]:
        pass

    @abc.abstractmethod
    def get_module_class(self, module_type: str) -> typing.Type["KiaraModule"]:
        pass

    def create_module_config(
        self, module_type: str, module_config: typing.Mapping[str, typing.Any]
    ) -> "KiaraModuleConfig":

        cls = self.get_module_class(module_type)
        config = cls._config_cls(**module_config)

        return config

    def create_module(
        self,
        kiara: "Kiara",
        id: typing.Optional[str],
        module_type: str,
        module_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        parent_id: typing.Optional[str] = None,
    ) -> "KiaraModule":

        module_cls = self.get_module_class(module_type)

        module = module_cls(
            id=id, parent_id=parent_id, module_config=module_config, kiara=kiara
        )
        return module


class WorkflowManager(object):
    def __init__(self, module_manager: "PythonModuleManager"):

        self._module_mgr: "PythonModuleManager" = module_manager

    def create_workflow(
        self,
        workflow_id: str,
        config: typing.Union[str, typing.Mapping[str, typing.Any]],
    ):

        if isinstance(config, typing.Mapping):
            raise NotImplementedError()
