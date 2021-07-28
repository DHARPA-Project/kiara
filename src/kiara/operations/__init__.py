# -*- coding: utf-8 -*-
import typing

from kiara.module_config import ModuleInstanceConfig

if typing.TYPE_CHECKING:
    from kiara import Kiara, KiaraModule
    from kiara.pipeline.config import PipelineModuleConfig


class ClassAttributes(object):
    def __init__(self, attrs: typing.Mapping[str, typing.Type]):
        self._attrs: typing.Iterable[str] = attrs


class OperationConfig(ModuleInstanceConfig):
    @classmethod
    def create(
        cls,
        config: typing.Union["ModuleInstanceConfig", typing.Mapping, str],
        module_config: typing.Union[
            None, typing.Mapping[str, typing.Any], "PipelineModuleConfig"
        ] = None,
        kiara: typing.Optional["Kiara"] = None,
    ) -> "OperationConfig":

        config = ModuleInstanceConfig.create(
            config=config, module_config=module_config, kiara=kiara
        )
        return config  # type: ignore


class Operation(object):
    def create(
        self,
        config: typing.Union[OperationConfig, typing.Mapping, str],
        module_config: typing.Union[
            None, typing.Mapping[str, typing.Any], "PipelineModuleConfig"
        ] = None,
        kiara: typing.Optional["Kiara"] = None,
    ):

        if kiara is None:
            from kiara import Kiara

            kiara = Kiara.instance()

        operation_config = OperationConfig.create(
            config=config, module_config=module_config, kiara=kiara
        )

        return Operation(config=operation_config, kiara=kiara)

    def __init__(self, config: OperationConfig, kiara: typing.Optional["Kiara"] = None):

        if kiara is None:
            from kiara import Kiara

            kiara = Kiara.instance()

        self._kiara: Kiara = kiara
        self._config: OperationConfig = config
        self._module: typing.Optional["KiaraModule"] = None

    def module(self) -> "KiaraModule":
        if self._module is None:
            self._module = self._config.create_module(self._kiara)
        return self._module


class OperationMgmt(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._no_conf_modules: typing.Optional[typing.Dict[str, str]] = None

    @property
    def available_operations(self) -> typing.Mapping[str, str]:
        if self._no_conf_modules is None:
            _modules = {}
            for module_type in self._kiara.available_module_types:
                mod_cls = self._kiara.get_module_class(module_type)
                mod_conf = mod_cls._config_cls
                if not mod_conf.requires_config():
                    _modules[module_type] = module_type
            self._no_conf_modules = _modules
        return self._no_conf_modules
