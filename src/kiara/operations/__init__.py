# -*- coding: utf-8 -*-
import typing
from pydantic import PrivateAttr

from kiara.module_config import ModuleInstanceConfig

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara
    from kiara.module import KiaraModule
    from kiara.pipeline.config import PipelineModuleConfig


class ClassAttributes(object):
    def __init__(self, attrs: typing.Mapping[str, typing.Type]):
        self._attrs: typing.Iterable[str] = attrs


class OperationConfig(ModuleInstanceConfig):
    @classmethod
    def create_operation_config(
        cls,
        kiara: "Kiara",
        config: typing.Union["ModuleInstanceConfig", typing.Mapping, str],
        module_config: typing.Union[
            None, typing.Mapping[str, typing.Any], "PipelineModuleConfig"
        ] = None,
    ) -> "OperationConfig":

        _config = ModuleInstanceConfig.create(
            config=config, module_config=module_config, kiara=kiara
        )
        op_config = cls(**_config.dict())
        op_config._kiara = kiara
        return op_config

    _kiara: typing.Optional["Kiara"] = PrivateAttr(default=None)
    _module: typing.Optional["KiaraModule"]

    @property
    def kiara(self) -> "Kiara":
        if self._kiara is None:
            raise Exception("Kiara context not set for operation.")
        return self._kiara

    @property
    def module(self) -> "KiaraModule":
        if self._module is None:
            self._module = self.create_module(self.kiara)
        return self._module

    @property
    def module_cls(self) -> typing.Type["KiaraModule"]:
        return self.kiara.get_module_class(self.module_type)


class Operations(object):
    def __init__(self, kiara: typing.Optional["Kiara"] = None):

        if kiara is None:
            from kiara import Kiara

            kiara = Kiara.instance()

        self._kiara: "Kiara" = kiara
        self._operations: typing.Dict[str, OperationConfig] = {}

    def is_matching_operation(self, op_config: OperationConfig) -> bool:
        raise NotImplementedError()

    def add_operation(self, module_type_id, op_config) -> bool:

        if not self.is_matching_operation(op_config):
            return False

        self._operations[module_type_id] = op_config
        return True

    @property
    def operation_configs(self) -> typing.Mapping[str, OperationConfig]:
        return self._operations


class AllOperations(Operations):
    def is_matching_operation(self, op_config: OperationConfig) -> bool:
        return True


class OperationMgmt(object):
    def __init__(
        self,
        kiara: "Kiara",
        operation_type_classes: typing.Optional[
            typing.Mapping[str, typing.Type[Operations]]
        ] = None,
    ):

        self._kiara: "Kiara" = kiara

        if operation_type_classes is None:
            from kiara.utils.class_loading import find_all_operation_types

            self._operation_type_classes: typing.Dict[
                str, typing.Type["Operations"]
            ] = find_all_operation_types()
        else:
            self._operation_type_classes = dict(operation_type_classes)
        self._operation_types: typing.Optional[typing.Dict[str, Operations]] = None

        self._profiles: typing.Optional[typing.Dict[str, OperationConfig]] = None
        self._operations: typing.Optional[typing.Dict[str, typing.List[str]]] = None

    @property
    def profiles(self) -> typing.Mapping[str, OperationConfig]:

        if self._profiles is not None:
            return self._profiles

        _profiles = {}

        for module_id in self._kiara.available_module_types:

            mod_cls = self._kiara.get_module_class(module_id)
            mod_conf = mod_cls._config_cls
            if not mod_conf.requires_config():
                _profiles[module_id] = OperationConfig.create_operation_config(
                    config={"module_type": module_id}, kiara=self._kiara
                )

            profiles = mod_cls.retrieve_module_profiles(kiara=self._kiara)
            if profiles:
                for profile_name, config in profiles.items():
                    if not isinstance(config, OperationConfig):
                        config = OperationConfig.create_operation_config(
                            config=config, kiara=self._kiara
                        )
                    if "." not in profile_name:
                        profile_id = f"{module_id}.{profile_name}"
                    else:
                        profile_id = profile_name
                    if profile_id in _profiles.keys():
                        raise Exception(f"Duplicate operation id: {profile_id}")
                    _profiles[profile_id] = config

        self._profiles = {k: _profiles[k] for k in sorted(_profiles.keys())}
        _operations: typing.Dict[str, typing.List[str]] = {}

        for profile_name, op_config in self._profiles.items():
            for op_type_name, op_type in self.operation_types.items():
                if op_type.add_operation(profile_name, op_config):
                    _operations.setdefault(op_type_name, []).append(profile_name)

        self._operations = _operations

        return self._profiles

    @property
    def operation_types(self) -> typing.Mapping[str, Operations]:

        if self._operation_types is not None:
            return self._operation_types

        # TODO: support op type config
        _operation_types = {}
        for op_name, op_cls in self._operation_type_classes.items():
            _operation_types[op_name] = op_cls()

        self._operation_types = _operation_types
        self.profiles
        return self._operation_types

    def get_operations(self, operation_type: str) -> Operations:

        if operation_type not in self.operation_types.keys():
            raise Exception(f"No operation type '{operation_type}' registered.")
        return self.operation_types[operation_type]

    def get_types_for_id(self, operation_id: str) -> typing.Set[str]:

        result = set()
        for ops_ty, ops in self.operation_types.items():
            if operation_id in ops.operation_configs.keys():
                result.add(ops_ty)
        return result

    def run(self):
        pass
