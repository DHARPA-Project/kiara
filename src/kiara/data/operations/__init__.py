# -*- coding: utf-8 -*-
import abc
import logging
import typing
from pydantic import BaseModel, Extra, Field, PrivateAttr, ValidationError

from kiara.data import Value
from kiara.data.values import ValueSchema, ValueSet, ValueSetImpl
from kiara.utils import is_debug
from kiara.utils.class_loading import find_all_operation_types
from kiara.utils.modules import create_schemas

if typing.TYPE_CHECKING:
    from kiara import Kiara
    from kiara.module import KiaraModule


log = logging.getLogger("kiara")


class ModuleProfileConfig(BaseModel):
    class Config:
        extra = Extra.forbid
        validate_all = True

    _module: typing.Optional["KiaraModule"] = PrivateAttr(default=None)
    module_type: str = Field(description="The module type.")
    module_config: typing.Dict[str, typing.Any] = Field(
        default_factory=dict, description="The configuration for the module."
    )

    def create_module(self, kiara: "Kiara"):

        if self._module is None:
            self._module = kiara.create_module(
                id=f"extract_metadata_{self.module_type}",
                module_type=self.module_type,
                module_config=self.module_config,
            )
        return self._module


class OperationType(ModuleProfileConfig, abc.ABC):
    """A class to represent a sort of an 'interface' for a group of kiara modules that do the same thing to a dataset, independent of the dataset type."""

    @classmethod
    def retrieve_operation_configs(
        cls, kiara: "Kiara"
    ) -> typing.Mapping[
        str,
        typing.Mapping[
            str,
            typing.Mapping[
                str,
                typing.Mapping[
                    str, typing.Union[typing.Mapping[str, typing.Any], "OperationType"]
                ],
            ],
        ],
    ]:

        return {}

    @classmethod
    def retrieve_operations(cls, kiara: "Kiara"):
        """Return an (optional) collection of operations that the implementing class created.

        Internally, this calls the [retrieve_operation_configs][kiara.data.operations.OperationType.retrieve_operation_configs] method of the sub-class (if implemented).
        """

        profiles: typing.Mapping[
            str,
            typing.Mapping[
                str,
                typing.Mapping[
                    str, typing.Union[OperationType, typing.Mapping[str, typing.Any]]
                ],
            ],
        ] = cls.retrieve_operation_configs(kiara=kiara)
        operations: typing.Dict[
            str, typing.Dict[str, typing.Dict[str, typing.Any]]
        ] = {}

        for value_type, details in profiles.items():

            for operation_name, operation_ids_and_config in details.items():
                for operation_id, data in operation_ids_and_config.items():
                    if operation_name in operations.setdefault(
                        value_type, {}
                    ).setdefault(operation_name, {}):
                        raise Exception(
                            f"Duplicate name for value type '{value_type}' and operation '{operation_name}': {operation_id}"
                        )

                    if isinstance(data, OperationType):
                        if not isinstance(data, cls):
                            raise Exception(
                                f"Invalid operation object type '{type(data)}' for operation type '{operation_name}': should be {cls}"
                            )
                        config_obj = data
                    elif isinstance(data, typing.Mapping):
                        try:
                            config_obj = cls(input_type=value_type, **data)
                        except ValidationError as ve:
                            raise Exception(
                                f"Can't create operation '{operation_name}.{operation_id}' for value type '{value_type}': {ve}"
                            )
                    else:
                        raise TypeError(f"Invalid operation object type: {type(data)}")

                    config_obj._kiara = kiara
                    operations[value_type][operation_name][operation_id] = config_obj

        return operations

    @classmethod
    def create_input_schema(
        cls,
    ) -> typing.Optional[
        typing.Mapping[str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]]
    ]:
        """Abstract method to implement by child classes, returns a description of the input schema of this module."""

        return None

    @classmethod
    def create_output_schema(
        cls,
    ) -> typing.Optional[
        typing.Mapping[str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]]
    ]:
        """Abstract method to implement by child classes, returns a description of the output schema of this module."""

        return None

    _kiara: typing.Optional["Kiara"] = PrivateAttr(default=None)
    _input_schemas: typing.Mapping[str, ValueSchema] = PrivateAttr(default=None)
    _output_schemas: typing.Mapping[str, ValueSchema] = PrivateAttr(default=None)

    profile_doc: typing.Optional[str] = Field(
        description="Description of the profile.", default=None
    )
    input_type: str = Field(
        description="The value type this profile takes as main input."
    )
    input_name: typing.Optional[str] = Field(
        description="The name of the input for the value to save.", default=None
    )
    input_map: typing.Optional[typing.Dict[str, str]] = Field(
        description="A mapping of final input field (for the operation input) to input field name of the underlying module (minus the main 'input_name'. Defaults to unmodified input field names."
    )
    output_map: typing.Optional[typing.Dict[str, str]] = Field(
        description="A mapping of final output field name to result output field names. Defaults to unmodified output field names",
        default=None,
    )

    @property
    def module_obj(self) -> "KiaraModule":

        if self._module is None:
            assert self._kiara is not None
            self.create_module(self._kiara)
        return self._module  # type: ignore

    @property
    def doc(self) -> str:

        if self.profile_doc is not None:
            return self.profile_doc

        return self.module_obj.module_instance_doc

    @property
    def final_input_name(self) -> str:
        """The actual input name for the main input value, will be calculated if not provided."""

        if self.input_name:
            return self.input_name

        m_input_schemas = self.module_obj.input_schemas

        if len(m_input_schemas) != 1:
            raise Exception(
                f"Can't create inputs for profile. Input name not specified, and module '{self.module_type}' has multiple inputs: {', '.join(m_input_schemas.keys())}"
            )

        input_name = next(iter(m_input_schemas.keys()))
        return input_name

    def create_inputs(
        self,
        value: Value,
        other_inputs: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    ) -> typing.Dict[str, typing.Any]:

        if self.input_name is not None:
            m_input_schemas = self.module_obj.input_schemas
            if self.input_name not in m_input_schemas.keys():
                raise Exception(
                    f"Can't create inputs for profile, input name '{self.input_name}' does not exist for module '{self.module_type}'. Available inputs: {', '.join(m_input_schemas.keys())}"
                )
            input_name = self.input_name
        else:
            input_name = self.final_input_name

        if other_inputs:
            all_inputs = dict(other_inputs)
        else:
            all_inputs = {}

        if self.input_map:
            _temp_dir = {}
            for k, v in all_inputs.items():
                if k in self.input_map.keys():
                    _temp_dir[self.input_map[k]] = v
                else:
                    _temp_dir[k] = v
            all_inputs = _temp_dir

        if input_name in all_inputs.keys():
            raise Exception(
                f"Can't create inputs for profile. Main input name '{input_name}' specified twice."
            )

        all_inputs[input_name] = value
        return all_inputs

    def _validate_operation_interface(self):

        pass

    def _validate_input_schemas(self):

        if not self.input_map:
            input_schema = {
                k: v
                for k, v in self.module_obj.input_schemas.items()
                if k != self.final_input_name
            }

        else:
            input_schema = {}
            for k, v in self.input_map.items():
                if k == self.input_name:
                    raise Exception(
                        f"Misconfigured input name for operation type '{self.__class__.__name__}', input map can't contain main input field ({self.input_name}): {self.input_map}"
                    )

                input_schema[k] = self.module_obj.input_schemas[v]

        if not input_schema.keys() == self.input_schemas.keys():
            raise Exception("Invalid input schema for ")

    #
    # @property
    # def output_schemas(self) -> typing.Mapping[str, ValueSchema]:
    #
    #     if not self.output_map:
    #         return self.module_obj.output_schemas
    #
    #     result = {}
    #     for k, v in self.output_map.items():
    #         result[k] = self.module_obj.output_schemas[v]
    #     return result


class DataOperationMgmt(object):
    def __init__(
        self,
        kiara: "Kiara",
        operation_type_classes: typing.Optional[
            typing.Mapping[str, typing.Type[OperationType]]
        ] = None,
    ):

        self._kiara: "Kiara" = kiara
        if operation_type_classes is None:
            operation_type_classes = find_all_operation_types()

        self._operation_type_classes: typing.Dict[str, typing.Type[OperationType]] = {}
        self._operation_input_schemas: typing.Dict[
            str, typing.Mapping[str, ValueSchema]
        ] = {}
        self._operation_output_schemas: typing.Dict[
            str, typing.Mapping[str, ValueSchema]
        ] = {}

        for k, v in operation_type_classes.items():
            try:
                self.register_type_cls(alias=k, cls=v)
            except Exception as e:
                if is_debug():
                    log.warning(f"Can't add operation type '{k}': {e}")
                else:
                    log.debug(f"Can't add operation type '{k}': {e}")

        self._operations: typing.Optional[
            typing.Dict[str, typing.Dict[str, typing.Dict[str, OperationType]]]
        ] = None

    def register_type_cls(self, alias: str, cls: typing.Type[OperationType]):

        if alias in self._operation_type_classes.keys():
            raise Exception(f"Operation type already registered: {alias}")

        _input_schemas = cls.create_input_schema()
        _output_schema = cls.create_output_schema()

        self._operation_type_classes[alias] = cls

        if _input_schemas:
            input_schemas = create_schemas(
                schema_config=_input_schemas, kiara=self._kiara
            )
            self._operation_input_schemas[alias] = input_schemas

        if _output_schema:
            output_schemas = create_schemas(
                schema_config=_output_schema, kiara=self._kiara
            )
            self._operation_output_schemas[alias] = output_schemas

    def get_operation_type_cls(self, operation_name: str):

        op_cls = self._operation_type_classes.get(operation_name, None)
        if op_cls is None:
            raise Exception(
                f"No operation type with name '{operation_name}' registered."
            )

        return op_cls

    @property
    def operations(
        self,
    ) -> typing.Mapping[str, typing.Mapping[str, typing.Mapping[str, OperationType]]]:
        """Return all available operations, per value type.

        The result dict has the value type as key, and then another dict with the name of the operation type as key and the [OperationType][kiara.profiles.OperationType] object as value.
        """

        if self._operations is not None:
            return self._operations

        self._operations = {}

        for operation_name, operation_cls in self._operation_type_classes.items():
            all_ops = operation_cls.retrieve_operations(kiara=self._kiara)
            for value_type, details in all_ops.items():
                for operation_name, obj in details.items():
                    if (
                        operation_name
                        in self._operations.setdefault(value_type, {}).keys()
                    ):
                        raise Exception(
                            f"Duplicate operation name for type '{value_type}': {operation_name}"
                        )
                    self._operations[value_type][operation_name] = obj

        for value_type, value_type_cls in self._kiara.value_types.items():

            if hasattr(value_type_cls, "get_operations"):

                value_type_ops = value_type_cls.get_operations()  # type: ignore
                for operation_name, id_and_config in value_type_ops.items():

                    if operation_name not in self._operation_type_classes.keys():
                        if is_debug():
                            log.warning(
                                f"Ignoring {len(id_and_config)} operation config(s) for type '{value_type}': no operation type '{operation_name}' registered"
                            )
                        else:
                            log.debug(
                                f"Ignoring {len(id_and_config)} operation config(s) for type '{value_type}': no operation type '{operation_name}' registered"
                            )
                        continue

                    for operation_id, config in id_and_config.items():
                        if (
                            operation_id
                            in self._operations.setdefault(value_type, {})
                            .setdefault(operation_name, {})
                            .keys()
                        ):
                            # TODO: figure out how to handle this
                            raise Exception(
                                f"Duplicate operation: {value_type} - {operation_name} - {operation_id}"
                            )

                        op_type_cls = self._operation_type_classes.get(operation_name)

                        if not op_type_cls:
                            raise Exception(f"Invalid operation name: {operation_name}")

                        if isinstance(config, OperationType):
                            if not isinstance(config, op_type_cls):
                                raise Exception(
                                    f"Invalid operation object type '{type(config)}' for operation type '{operation_name}': should be {op_type_cls}"
                                )
                            obj = config
                        elif isinstance(config, typing.Mapping):
                            try:
                                obj = op_type_cls(input_type=value_type, **config)
                            except ValidationError as ve:
                                raise Exception(
                                    f"Can't create operation '{operation_name}.{operation_id}' for value type '{value_type}': {ve}"
                                )
                        else:
                            raise TypeError(
                                f"Invalid operation object type: {type(config)}"
                            )
                        self._operations[value_type][operation_name][operation_id] = obj

        for module_name in self._kiara.available_module_types:
            module_cls = self._kiara.get_module_class(module_name)
            if hasattr(module_cls, "get_operations"):
                operations = module_cls.get_operations()  # type: ignore
                for value_type, operation_details in operations.items():
                    for operation_name, id_and_config in operation_details.items():
                        for operation_id, config in id_and_config.items():
                            if (
                                operation_id
                                in self._operations.setdefault(value_type, {})
                                .setdefault(operation_name, {})
                                .keys()
                            ):
                                # TODO: figure out how to handle this
                                raise Exception(
                                    f"Duplicate operation: {value_type} - {operation_name} - {operation_id}"
                                )

                            op_type_cls = self._operation_type_classes.get(
                                operation_name
                            )
                            if not op_type_cls:
                                raise Exception(
                                    f"Invalid operation name: {operation_name}"
                                )

                            if isinstance(config, OperationType):
                                if not isinstance(config, op_type_cls):
                                    raise Exception(
                                        f"Invalid operation object type '{type(config)}' for operation type '{operation_name}': should be {op_type_cls}"
                                    )
                                obj = config
                            elif isinstance(config, typing.Mapping):
                                try:
                                    obj = op_type_cls(input_type=value_type, **config)
                                except ValidationError as ve:
                                    raise Exception(
                                        f"Can't create operation '{operation_name}.{operation_id}' for value type '{value_type}': {ve}"
                                    )
                            else:
                                raise TypeError(
                                    f"Invalid operation object type: {type(config)}"
                                )
                            self._operations[value_type][operation_name][
                                operation_id
                            ] = obj

        invalid = [x for x in self._operations.keys() if "." in x]
        if invalid:
            raise Exception(
                f"Invalid value type name(s), type names can't contain '.': {', '.join(invalid)}"
            )

        return self._operations

    def get_operation(
        self,
        value_type: str,
        operation_name: str,
        operation_id: typing.Optional[str],
        raise_exception: bool = False,
    ) -> typing.Optional[OperationType]:

        if operation_id is None:
            available_operations: typing.Optional[
                typing.Mapping[str, typing.Any]
            ] = self.operations.get(value_type, {}).get(operation_name, None)
            if not available_operations and raise_exception:
                raise Exception(
                    f"No '{operation_name}' operations available for value type '{value_type}'"
                )
            elif not available_operations:
                return None

            if len(available_operations) == 1:
                operation_id = next(iter(available_operations.keys()))
            else:
                if raise_exception:
                    raise Exception(
                        f"Multiple '{operation_name}' operations available for value type '{value_type}', specify one of: {', '.join(available_operations.keys())}"
                    )
                else:
                    log.warning(
                        f"Multiple '{operation_name}' operations available for value type '{value_type}', specify one of: {', '.join(available_operations.keys())}"
                    )
                    return None

        oc = (
            self.operations.get(value_type, {})
            .get(operation_name, {})
            .get(operation_id, None)
        )

        if oc is None:
            if raise_exception:
                av = self.operations.get(value_type, {}).get(operation_name, {}).keys()
                msg = ""
                if av:
                    msg = f" Available ids: {', '.join(av)}"
                raise Exception(
                    f"No operation '{operation_name}' with id '{operation_id}' available for value type '{value_type}'.{msg}"
                )
            else:
                return None
        return oc

    def run(
        self,
        operation_name: str,
        operation_id: str,
        value: Value,
        other_inputs: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        value_type: typing.Optional[str] = None,
    ) -> ValueSet:

        if value_type is None:
            value_type = value.type_name

        op_config = self.get_operation(
            value_type=value_type,
            operation_name=operation_name,
            operation_id=operation_id,
            raise_exception=False,
        )

        if not op_config:
            av = self.operations.get(value_type, {}).get(operation_name, {}).keys()
            msg = ""
            if av:
                msg = f" Available ids: {', '.join(av)}"
            raise Exception(
                f"No operation '{operation_name}' with id '{operation_id}' available for value type '{value_type}'.{msg}"
            )

        op_module = op_config.create_module(self._kiara)
        constants = op_module.config.constants
        inputs = dict(constants)
        for k, v in op_module.config.defaults:
            if k in constants.keys():
                raise Exception(
                    f"Invalid default value '{k}', constant defined for this name."
                )
            inputs[k] = v

        calculated_inputs = op_config.create_inputs(
            value=value, other_inputs=other_inputs
        )
        for k, v in calculated_inputs.items():
            inputs[k] = v

        # TODO: do this via a processor
        result = op_module.run(**inputs)

        if not op_config.output_map:
            return result

        mapped_results: typing.Dict[str, Value] = {}
        for k, v in op_config.output_map.items():
            mapped_results[k] = result[v]

        return ValueSetImpl(items=mapped_results, read_only=True)
