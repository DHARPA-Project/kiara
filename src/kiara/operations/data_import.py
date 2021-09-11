# -*- coding: utf-8 -*-
import abc
import logging
import typing
from pydantic import Field

from kiara import Kiara, KiaraModule
from kiara.data.values import Value, ValueLineage, ValueSchema
from kiara.data.values.value_set import ValueSet
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import ModuleTypeConfigSchema
from kiara.operations import Operation, OperationType
from kiara.utils import log_message

log = logging.getLogger("kiara")


class DataImportModuleConfig(ModuleTypeConfigSchema):

    # value_type: str = Field(description="The type of the value to be imported.")
    source_profile: str = Field(
        description="The name of the source profile. Used to distinguish different input categories for the same input type."
    )
    source_type: str = Field(description="The type of the source to import from.")
    allow_save_input: bool = Field(
        description="Allow the user to choose whether to save the imported item or not.",
        default=True,
    )
    save_default: bool = Field(
        description="The default of the 'save' input if not specified by the user.",
        default=False,
    )
    allow_aliases_input: typing.Optional[bool] = Field(
        description="Allow the user to choose aliases for the saved value.",
        default=None,
    )
    aliases_default: typing.List[str] = Field(
        description="Default value for aliases.", default_factory=list
    )


class DataImportModule(KiaraModule):

    _config_cls = DataImportModuleConfig

    @classmethod
    @abc.abstractmethod
    def get_target_value_type(cls) -> str:
        pass

    @classmethod
    def retrieve_module_profiles(
        cls, kiara: Kiara
    ) -> typing.Mapping[str, typing.Union[typing.Mapping[str, typing.Any], Operation]]:

        all_metadata_profiles: typing.Dict[
            str, typing.Dict[str, typing.Dict[str, typing.Any]]
        ] = {}

        sup_type = cls.get_target_value_type()
        if sup_type not in kiara.type_mgmt.value_type_names:
            log_message(
                f"Ignoring data import operation for type '{sup_type}': type not available"
            )
            return {}

        for attr in dir(cls):
            if not attr.startswith("import_from__"):
                continue

            tokens = attr[13:].rsplit("__", maxsplit=1)
            if len(tokens) != 2:
                log_message(
                    f"Can't determine source name and type from string in module {cls._module_type_id}, ignoring method: {attr}"  # type: ignore
                )

            source_profile, source_type = tokens

            op_config = {
                "module_type": cls._module_type_id,  # type: ignore
                "module_config": {
                    "source_profile": source_profile,
                    "source_type": source_type,
                },
                "doc": f"Import data of type '{sup_type}' from a {source_profile} {source_type} and save it to the kiara data store.",
            }
            all_metadata_profiles[
                f"{sup_type}.import_from.{source_profile}.{source_type}"
            ] = op_config

        return all_metadata_profiles

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Dict[str, typing.Any] = {
            "source": {
                "type": self.get_config_value("source_type"),
                "doc": f"A {self.get_config_value('source_profile')} '{self.get_config_value('source_type')}' value.",
            },
        }

        allow_save = self.get_config_value("allow_save_input")
        save_default = self.get_config_value("save_default")
        if allow_save:
            inputs["save"] = {
                "type": "boolean",
                "doc": "Whether to save the imported value, or not.",
                "default": save_default,
            }

        allow_aliases: typing.Optional[bool] = self.get_config_value(
            "allow_aliases_input"
        )
        if allow_aliases is None:
            allow_aliases = allow_save

        if allow_aliases and not allow_save and not save_default:
            raise Exception(
                "Invalid module configuration: allowing aliases input does not make sense if save is disabled."
            )

        if allow_aliases:
            default_aliases = self.get_config_value("aliases_default")
            inputs["aliases"] = {
                "type": "list",
                "doc": "A list of aliases to use when storing the value (only applicable if 'save' is set).",
                "default": default_aliases,
            }

        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs: typing.Mapping[str, typing.Any] = {
            "value_item": {
                "type": self.get_target_value_type(),
                "doc": f"The imported {self.get_target_value_type()} value.",
            },
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        source: str = inputs.get_value_data("source")

        source_profile: str = self.get_config_value("source_profile")
        source_type: str = self.get_config_value("source_type")

        func_name = f"import_from__{source_profile}__{source_type}"
        if not hasattr(self, func_name):
            raise Exception(
                f"Can't import '{source_type}' value: missing function '{func_name}' in class '{self.__class__.__name__}'. Please check this modules documentation or source code to determine which source types and profiles are supported."
            )

        allow_save = self.get_config_value("allow_save_input")
        if allow_save:
            save = inputs.get_value_data("save")
        else:
            save = self.get_config_value("save_default")

        allow_aliases: typing.Optional[bool] = self.get_config_value(
            "allow_aliases_input"
        )
        if allow_aliases is None:
            allow_aliases = allow_save

        if allow_aliases:
            aliases = inputs.get_value_data("aliases")
        else:
            aliases = self.get_config_value("aliases_default")

        if aliases and not save:
            raise KiaraProcessingException(
                f"Can't import file from '{source}': 'aliases' specified, but not 'save', this does not make sense."
            )

        if aliases is None:
            aliases = []

        func = getattr(self, func_name)
        # TODO: check signature?

        result = func(source)
        schema = ValueSchema(
            type=self.get_target_value_type(), doc="Imported file value."
        )

        value_lineage = ValueLineage.from_module_and_inputs(
            module=self, output_name="value_item", inputs=inputs
        )
        value: Value = self._kiara.data_registry.register_data(
            value_data=result, value_schema=schema, value_lineage=value_lineage
        )

        if save:
            value_saved = self._kiara.data_store.register_data(value_data=value)
            self._kiara.data_store.link_aliases(value_saved, *aliases)

        outputs.set_values(value_item=value)


class FileImportModule(DataImportModule):
    """Import a file, optionally saving it to the data store."""

    @classmethod
    def get_target_value_type(cls) -> str:
        return "file"


class FileImportOperationType(OperationType):
    """Save a value into a data store."""

    def is_matching_operation(self, op_config: Operation) -> bool:

        return issubclass(op_config.module_cls, FileImportModule)

    def get_import_operations(self) -> typing.Dict[str, typing.Dict[str, Operation]]:
        """Return all available import operataions for a value type.

        The result dictionary uses the source type as first level key, a source name/description as 2nd level key,
        and the Operation object as value.
        """

        result: typing.Dict[str, typing.Dict[str, Operation]] = {}

        for op_config in self.operation_configs.values():

            source_type = op_config.module_config["source_type"]
            source_profile = op_config.module_config["source_profile"]

            result.setdefault(source_type, {})[source_profile] = op_config

        return result


class FileBundleImportModule(DataImportModule):
    """Import a file, optionally saving it to the data store."""

    @classmethod
    def get_target_value_type(cls) -> str:
        return "file_bundle"


class FileBundleImportOperationType(OperationType):
    """Save a value into a data store."""

    def is_matching_operation(self, op_config: Operation) -> bool:

        return issubclass(op_config.module_cls, FileBundleImportModule)

    def get_import_operations(self) -> typing.Dict[str, typing.Dict[str, Operation]]:
        """Return all available import operataions for a value type.

        The result dictionary uses the source type as first level key, a source name/description as 2nd level key,
        and the Operation object as value.
        """

        result: typing.Dict[str, typing.Dict[str, Operation]] = {}

        for op_config in self.operation_configs.values():

            source_type = op_config.module_config["source_type"]
            source_profile = op_config.module_config["source_profile"]

            result.setdefault(source_type, {})[source_profile] = op_config

        return result
