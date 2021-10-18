# -*- coding: utf-8 -*-
import abc
import logging
import typing
from pydantic import Field

from kiara import Kiara, KiaraModule
from kiara.data import ValueSet
from kiara.data.values import Value, ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.metadata.data import LoadConfig
from kiara.module_config import ModuleTypeConfigSchema
from kiara.operations import Operation, OperationType
from kiara.utils import log_message

log = logging.getLogger("kiara")


class StoreValueModuleConfig(ModuleTypeConfigSchema):

    value_type: str = Field(description="The type of the value to save.")


class StoreValueTypeModule(KiaraModule):
    """Store a specific value type.

    This is used internally.
    """

    _config_cls = StoreValueModuleConfig

    @classmethod
    def get_supported_value_types(cls) -> typing.Set[str]:
        _types = cls.retrieve_supported_types()
        if isinstance(_types, str):
            _types = [_types]

        return set(_types)

    @classmethod
    @abc.abstractmethod
    def retrieve_supported_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        pass

    @classmethod
    def retrieve_module_profiles(
        cls, kiara: Kiara
    ) -> typing.Mapping[str, typing.Union[typing.Mapping[str, typing.Any], Operation]]:

        all_metadata_profiles: typing.Dict[
            str, typing.Dict[str, typing.Dict[str, typing.Any]]
        ] = {}

        for sup_type in cls.get_supported_value_types():

            if sup_type not in kiara.type_mgmt.value_type_names:
                log_message(
                    f"Ignoring save operation for type '{sup_type}': type not available"
                )
                continue

            op_config = {
                "module_type": cls._module_type_id,  # type: ignore
                "module_config": {"value_type": sup_type},
                "doc": f"Store a value of type '{sup_type}'.",
            }
            all_metadata_profiles[f"{sup_type}.save"] = op_config

        return all_metadata_profiles

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        field_name = self.get_config_value("value_type")
        if field_name == "any":
            field_name = "value_item"

        inputs: typing.Mapping[str, typing.Any] = {
            "value_id": {
                "type": "string",
                "doc": "The id to use when saving the value.",
            },
            field_name: {
                "type": self.get_config_value("value_type"),
                "doc": f"A value of type '{self.get_config_value('value_type')}'.",
            },
            "base_path": {
                "type": "string",
                "doc": "The base path to save the value to.",
            },
        }

        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        field_name = self.get_config_value("value_type")
        if field_name == "any":
            field_name = "value_item"

        outputs: typing.Mapping[str, typing.Any] = {
            field_name: {
                "type": self.get_config_value("value_type"),
                "doc": "The original or cloned (if applicable) value that was saved.",
            },
            "load_config": {
                "type": "load_config",
                "doc": "The configuration to use with kiara to load the saved value.",
            },
        }

        return outputs

    @abc.abstractmethod
    def store_value(
        self, value: Value, base_path: str
    ) -> typing.Union[
        typing.Tuple[typing.Dict[str, typing.Any], typing.Any],
        typing.Dict[str, typing.Any],
    ]:
        """Save the value, and return the load config needed to load it again."""

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        value_id: str = inputs.get_value_data("value_id")
        if not value_id:
            raise KiaraProcessingException("No value id provided.")

        field_name = self.get_config_value("value_type")
        if field_name == "any":
            field_name = "value_item"

        value_obj: Value = inputs.get_value_obj(field_name)
        base_path: str = inputs.get_value_data("base_path")

        result = self.store_value(value=value_obj, base_path=base_path)
        if isinstance(result, typing.Mapping):
            load_config = result
            result_value = value_obj
        elif isinstance(result, tuple):
            load_config = result[0]
            if result[1]:
                result_value = result[1]
            else:
                result_value = value_obj
        else:
            raise KiaraProcessingException(
                f"Invalid result type for 'store_value' method in class '{self.__class__.__name__}'. This is a bug."
            )

        load_config["value_id"] = value_id

        lc = LoadConfig(**load_config)

        if lc.base_path_input_name and lc.base_path_input_name not in lc.inputs.keys():
            raise KiaraProcessingException(
                f"Invalid load config: base path '{lc.base_path_input_name}' not part of inputs."
            )

        outputs.set_values(
            metadata=None, lineage=None, **{"load_config": lc, field_name: result_value}
        )


class StoreOperationType(OperationType):
    """Store a value into a local data store.

    This is a special operation type, that is used internally by the [LocalDataStore](http://dharpa.org/kiara/latest/api_reference/kiara.data.registry.store/#kiara.data.registry.store.LocalDataStore] data registry implementation.

    For each value type that should be supported by the persistent *kiara* data store, there must be an implementation of the [StoreValueTypeModule](http://dharpa.org/kiara/latest/api_reference/kiara.operations.store_value/#kiara.operations.store_value.StoreValueTypeModule) class, which handles the
    actual persisting on disk. In most cases, end users won't need to interact with this type of operation.
    """

    def is_matching_operation(self, op_config: Operation) -> bool:

        return issubclass(op_config.module_cls, StoreValueTypeModule)

    def get_store_operation_for_type(self, value_type: str) -> Operation:

        result = []

        for op_config in self.operations.values():
            if op_config.module_config["value_type"] == value_type:
                result.append(op_config)

        if not result:
            raise Exception(
                f"No 'store_value' operation for type '{value_type}' registered."
            )
        elif len(result) != 1:
            raise Exception(
                f"Multiple 'store_value' operations for type '{value_type}' registered."
            )

        return result[0]
