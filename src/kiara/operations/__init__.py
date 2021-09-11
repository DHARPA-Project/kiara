# -*- coding: utf-8 -*-
import json
import typing
from pydantic import Field, PrivateAttr
from rich import box
from rich.console import RenderableType, RenderGroup
from rich.syntax import Syntax
from rich.table import Table

from kiara.data.values import ValueSchema
from kiara.metadata.operation_models import OperationsMetadata
from kiara.module_config import ModuleConfig
from kiara.utils import create_table_from_field_schemas

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara
    from kiara.module import KiaraModule


class ClassAttributes(object):
    def __init__(self, attrs: typing.Mapping[str, typing.Type]):
        self._attrs: typing.Iterable[str] = attrs


class Operation(ModuleConfig):
    @classmethod
    def create_operation_config(
        cls,
        kiara: "Kiara",
        operation_id: str,
        config: typing.Union["ModuleConfig", typing.Mapping, str],
        module_config: typing.Union[None, typing.Mapping[str, typing.Any]] = None,
    ) -> "Operation":

        _config = ModuleConfig.create_module_config(
            config=config, module_config=module_config, kiara=kiara
        )
        _config_dict = _config.dict()
        _config_dict["id"] = operation_id
        op_config = cls(**_config_dict)
        op_config._kiara = kiara
        return op_config

    _kiara: typing.Optional["Kiara"] = PrivateAttr(default=None)
    _module: typing.Optional["KiaraModule"]

    id: str = Field(description="The operation id.")

    @property
    def kiara(self) -> "Kiara":
        if self._kiara is None:
            raise Exception("Kiara context not set for operation.")
        return self._kiara

    @property
    def module(self) -> "KiaraModule":
        if self._module is None:
            self._module = self.create_module(kiara=self.kiara)
        return self._module

    @property
    def input_schemas(self) -> typing.Mapping[str, ValueSchema]:
        return self.module.input_schemas

    @property
    def output_schemas(self) -> typing.Mapping[str, ValueSchema]:
        return self.module.output_schemas

    @property
    def module_cls(self) -> typing.Type["KiaraModule"]:
        return self.kiara.get_module_class(self.module_type)

    def create_renderable(self, **config: typing.Any) -> RenderableType:
        """Create a printable overview of this operations details.

        Available config options:
          - 'include_full_doc' (default: True): whether to include the full documentation, or just a description
          - 'include_src' (default: False): whether to include the module source code
        """

        include_full_doc = config.get("include_full_doc", True)

        table = Table(box=box.SIMPLE, show_header=False, show_lines=True)
        table.add_column("Property", style="i")
        table.add_column("Value")

        if self.doc:
            if include_full_doc:
                table.add_row("Documentation", self.doc.full_doc)
            else:
                table.add_row("Description", self.doc.description)

        module_type_md = self.module.get_type_metadata()

        table.add_row("Module type", self.module_type)
        conf = Syntax(
            json.dumps(self.module_config, indent=2), "json", background_color="default"
        )
        table.add_row("Module config", conf)

        constants = self.module_config.get("constants")
        inputs_table = create_table_from_field_schemas(
            _add_required=True,
            _add_default=True,
            _show_header=True,
            _constants=constants,
            **self.module.input_schemas,
        )
        table.add_row("Inputs", inputs_table)
        outputs_table = create_table_from_field_schemas(
            _add_required=False,
            _add_default=False,
            _show_header=True,
            _constants=None,
            **self.module.output_schemas,
        )
        table.add_row("Outputs", outputs_table)

        m_md_o = module_type_md.origin.create_renderable()
        m_md_c = module_type_md.context.create_renderable()
        m_md = RenderGroup(m_md_o, m_md_c)
        table.add_row("Module metadata", m_md)

        if config.get("include_src", False):
            table.add_row("Source code", module_type_md.process_src)

        return table


class OperationType(object):
    @classmethod
    def get_type_metadata(cls) -> OperationsMetadata:

        return OperationsMetadata.from_operations_class(cls)

    def __init__(self, kiara: typing.Optional["Kiara"] = None):

        if kiara is None:
            from kiara import Kiara

            kiara = Kiara.instance()

        self._kiara: "Kiara" = kiara
        self._operations: typing.Dict[str, Operation] = {}

    def is_matching_operation(self, op_config: Operation) -> bool:
        raise NotImplementedError()

    def add_operation(self, module_type_id, op_config) -> bool:

        if not self.is_matching_operation(op_config):
            return False

        self._operations[module_type_id] = op_config
        return True

    @property
    def operation_configs(self) -> typing.Mapping[str, Operation]:
        return self._operations


class AllOperationType(OperationType):
    def is_matching_operation(self, op_config: Operation) -> bool:
        return True


class OperationMgmt(object):
    def __init__(
        self,
        kiara: "Kiara",
        operation_type_classes: typing.Optional[
            typing.Mapping[str, typing.Type[OperationType]]
        ] = None,
    ):

        self._kiara: "Kiara" = kiara

        self._operation_type_classes: typing.Optional[
            typing.Dict[str, typing.Type["OperationType"]]
        ] = None

        if operation_type_classes is not None:
            self._operation_type_classes = dict(operation_type_classes)

        self._operation_types: typing.Optional[typing.Dict[str, OperationType]] = None

        self._profiles: typing.Optional[typing.Dict[str, Operation]] = None
        self._operations: typing.Optional[typing.Dict[str, typing.List[str]]] = None

    @property
    def operation_type_classes(
        self,
    ) -> typing.Mapping[str, typing.Type["OperationType"]]:

        if self._operation_type_classes is not None:
            return self._operation_type_classes

        from kiara.utils.class_loading import find_all_operation_types

        self._operation_type_classes = find_all_operation_types()
        return self._operation_type_classes

    @property
    def operation_ids(self) -> typing.List[str]:
        return list(self.profiles.keys())

    @property
    def profiles(self) -> typing.Mapping[str, Operation]:

        if self._profiles is not None:
            return self._profiles

        _profiles = {}

        for module_id in self._kiara.available_module_types:

            mod_cls = self._kiara.get_module_class(module_id)
            mod_conf = mod_cls._config_cls
            if not mod_conf.requires_config():
                _profiles[module_id] = Operation.create_operation_config(
                    operation_id=module_id,
                    config={
                        "module_type": module_id,
                        "doc": mod_cls.get_type_metadata().documentation,
                    },
                    kiara=self._kiara,
                )

            profiles = mod_cls.retrieve_module_profiles(kiara=self._kiara)
            if profiles:
                for profile_name, config in profiles.items():
                    if "." not in profile_name:
                        profile_id = f"{module_id}.{profile_name}"
                    else:
                        profile_id = profile_name

                    if profile_id in _profiles.keys():
                        raise Exception(
                            f"Duplicate operation id '{profile_id}': {_profiles[profile_id].dict()} -- {config}"
                        )

                    if not isinstance(config, Operation):
                        config = Operation.create_operation_config(
                            operation_id=profile_id, config=config, kiara=self._kiara
                        )
                    _profiles[profile_id] = config

        self._profiles = {k: _profiles[k] for k in sorted(_profiles.keys())}
        _operations: typing.Dict[str, typing.List[str]] = {}

        for profile_name, op_config in self._profiles.items():
            for op_type_name, op_type in self.operation_types.items():
                if op_type.add_operation(profile_name, op_config):
                    _operations.setdefault(op_type_name, []).append(profile_name)

        self._operations = _operations

        return self._profiles

    def get_operation(self, operation_id: str) -> typing.Optional[Operation]:

        return self.profiles.get(operation_id, None)

    @property
    def operation_types(self) -> typing.Mapping[str, OperationType]:

        if self._operation_types is not None:
            return self._operation_types

        # TODO: support op type config
        _operation_types = {}
        for op_name, op_cls in self.operation_type_classes.items():
            _operation_types[op_name] = op_cls(kiara=self._kiara)

        self._operation_types = _operation_types
        # make sure the profiles are loaded
        self.profiles  # noqa
        return self._operation_types

    def get_operations(self, operation_type: str) -> OperationType:

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
