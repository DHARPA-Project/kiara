# -*- coding: utf-8 -*-
import abc
import logging
import typing
from pydantic import Field

from kiara import Kiara, KiaraModule
from kiara.data.values import Value, ValueSchema
from kiara.data.values.value_set import ValueSet
from kiara.module_config import ModuleTypeConfigSchema
from kiara.operations import Operation, OperationType
from kiara.utils import log_message

log = logging.getLogger("kiara")


class DataImportModuleConfig(ModuleTypeConfigSchema):

    value_type: str = Field(description="The type of the value to be imported.")
    source_name: str = Field(
        description="A description of the source data (e.g. 'path', 'url', ...)."
    )
    source_type: str = Field(description="The type of the source to import from.")


class DataImportModule(KiaraModule):
    """Import data, and save it to the data store."""

    _config_cls = DataImportModuleConfig

    @classmethod
    @abc.abstractmethod
    def retrieve_supported_value_type(cls) -> str:
        pass

    @classmethod
    def retrieve_module_profiles(
        cls, kiara: Kiara
    ) -> typing.Mapping[str, typing.Union[typing.Mapping[str, typing.Any], Operation]]:

        all_metadata_profiles: typing.Dict[
            str, typing.Dict[str, typing.Dict[str, typing.Any]]
        ] = {}

        sup_type = cls.retrieve_supported_value_type()
        if sup_type not in kiara.type_mgmt.value_type_names:
            log_message(
                f"Ignoring save operation for type '{sup_type}': type not available"
            )
            return {}

        for attr in dir(cls):
            if not attr.startswith("import_from_"):
                continue

            tokens = attr[12:].rsplit("__", maxsplit=1)
            if len(tokens) != 2:
                log_message(
                    f"Can't determine source name and type from string in module {cls._module_type_id}, ignoring method: {attr}"  # type: ignore
                )

            source_name, source_type = tokens

            op_config = {
                "module_type": cls._module_type_id,  # type: ignore
                "module_config": {
                    "value_type": sup_type,
                    "source_name": source_name,
                    "source_type": source_type,
                },
                "doc": f"Import data of type '{sup_type}' from a {source_name} {source_type} and save it to the kiara data store.",
            }
            all_metadata_profiles[
                f"{sup_type}.import_from.{source_name}.{source_type}"
            ] = op_config

        return all_metadata_profiles

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Mapping[str, typing.Any] = {
            "source": {"type": "string", "doc": "The source."},
            "aliases": {
                "type": "list",
                "doc": "A list of aliases to use when storing the value.",
                "optional": True,
            },
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs: typing.Mapping[str, typing.Any] = {
            "value_item": {
                "type": self.get_config_value("value_type"),
                "doc": f"The imported {self.get_config_value('value_type')} value.",
            },
            # "value_metadata": {
            #     "type": "value_metadata",
            #     "doc": "The metadata of the value that was created when storing to the data store.",
            # },
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        source: str = inputs.get_value_data("source")
        value_type: str = self.get_config_value("value_type")
        source_name: str = self.get_config_value("source_name")
        source_type: str = self.get_config_value("source_type")
        aliases = inputs.get_value_data("aliases")
        if aliases is None:
            aliases = []

        func_name = f"import_from_{source_name}__{source_type}"
        if not hasattr(self, func_name):
            raise Exception(
                f"Can't import value of type '{value_type}': missing function '{func_name}'. This is most likely a bug."
            )

        func = getattr(self, func_name)
        # TODO: check signature?

        result = func(source, base_aliases=aliases)
        schema = ValueSchema(type=value_type, doc=f"Imported {value_type} value.")

        value_lineage = None
        # value_lineage = ValueLineage.from_module_and_inputs(module=self, output_name="value_item", inputs=inputs)
        # value_lineage = ValueLineage(module_type=self._module_type_id, module_config=self.config.dict(), result_name="value_item", inputs={ k: v.id for k, v in inputs.items()})

        value: Value = self._kiara.data_registry.register_data(
            value_data=result, value_schema=schema, value_lineage=value_lineage
        )
        # value: Value = Value(
        #     value_schema=schema, value_data=result, kiara=self._kiara  # type: ignore
        # )
        value_saved = self._kiara.data_store.register_data(value_data=value)
        self._kiara.data_store.link_aliases(value_saved, *aliases)

        outputs.set_values(value_item=value)


class DataImportOperationType(OperationType):
    """Save a value into a data store."""

    def is_matching_operation(self, op_config: Operation) -> bool:

        return issubclass(op_config.module_cls, DataImportModule)

    def get_import_operations_for_type(
        self, value_type: str
    ) -> typing.Dict[str, typing.Dict[str, Operation]]:
        """Return all available import operataions for a value type.

        The result dictionary uses the source type as first level key, a source name/description as 2nd level key,
        and the Operation object as value.
        """

        result: typing.Dict[str, typing.Dict[str, Operation]] = {}

        for op_config in self.operation_configs.values():
            if op_config.module_config["value_type"] != value_type:
                continue

            source_type = op_config.module_config["source_type"]
            source_name = op_config.module_config["source_name"]

            result.setdefault(source_type, {})[source_name] = op_config

        return result
