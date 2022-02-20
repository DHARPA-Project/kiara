# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import logging
import typing
from pydantic import Field

from kiara import Kiara, KiaraModule
from kiara.data.values import ValueSchema
from kiara.data.values.value_set import ValueSet
from kiara.module_config import ModuleTypeConfigSchema
from kiara.operations import Operation, OperationType
from kiara.utils import log_message

log = logging.getLogger("kiara")


class DataExportModuleConfig(ModuleTypeConfigSchema):

    target_profile: str = Field(
        description="The name of the target profile. Used to distinguish different target formats for the same data type."
    )
    source_type: str = Field(
        description="The type of the source data that is going to be exported."
    )


class DataExportModule(KiaraModule):

    _config_cls = DataExportModuleConfig

    @classmethod
    @abc.abstractmethod
    def get_source_value_type(cls) -> str:
        pass

    @classmethod
    def retrieve_module_profiles(
        cls, kiara: Kiara
    ) -> typing.Mapping[str, typing.Union[typing.Mapping[str, typing.Any], Operation]]:

        all_metadata_profiles: typing.Dict[
            str, typing.Dict[str, typing.Dict[str, typing.Any]]
        ] = {}

        sup_type = cls.get_source_value_type()
        if sup_type not in kiara.type_mgmt.value_type_names:
            log_message(
                f"Ignoring data export operation for type '{sup_type}': type not available"
            )
            return {}

        for attr in dir(cls):
            if not attr.startswith("export_as__"):
                continue

            target_profile = attr[11:]

            op_config = {
                "module_type": cls._module_type_id,  # type: ignore
                "module_config": {
                    "source_type": sup_type,
                    "target_profile": target_profile,
                },
                "doc": f"Export data of type '{sup_type}' as: {target_profile}.",
            }
            all_metadata_profiles[f"export.{sup_type}.as.{target_profile}"] = op_config

        return all_metadata_profiles

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        source_type = self.get_config_value("source_type")
        inputs: typing.Dict[str, typing.Any] = {
            source_type: {
                "type": source_type,
                "doc": f"A value of type '{source_type}'.",
            },
        }

        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs = {
            "export_details": {
                "type": "dict",
                "doc": "Details about the exported files/folders.",
            }
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        target_profile: str = self.get_config_value("target_profile")
        source_type: str = self.get_config_value("source_type")

        source = inputs.get_value_data(source_type)

        func_name = f"export_as__{target_profile}"
        if not hasattr(self, func_name):
            raise Exception(
                f"Can't export '{source_type}' value: missing function '{func_name}' in class '{self.__class__.__name__}'. Please check this modules documentation or source code to determine which source types and profiles are supported."
            )

        func = getattr(self, func_name)
        # TODO: check signature?

        result = func(source)
        # schema = ValueSchema(type=self.get_target_value_type(), doc="Imported dataset.")

        # value_lineage = ValueLineage.from_module_and_inputs(
        #     module=self, output_name=output_key, inputs=inputs
        # )
        # value: Value = self._kiara.data_registry.register_data(
        #     value_data=result, value_schema=schema, lineage=None
        # )

        outputs.set_value("export_details", result)


class FileExportModule(DataExportModule):
    """Import a file, optionally saving it to the data store."""

    @classmethod
    def get_target_value_type(cls) -> str:
        return "file"


class FileBundleImportModule(DataExportModule):
    """Import a file, optionally saving it to the data store."""

    @classmethod
    def get_target_value_type(cls) -> str:
        return "file_bundle"


class ExportDataOperationType(OperationType):
    """Export data from *kiara*.

    Operations of this type use internally handled datasets, and export it to the local file system.

    Export operations are created by implementing a class that inherits from [DataExportModule](http://dharpa.org/kiara/latest/api_reference/kiara.operations.data_export/#kiara.operations.data_export.DataExportModule), *kiara* will
    register it under an operation id following this template:

    ```
    <SOURCE_DATA_TYPE>.export_as.<EXPORT_PROFILE>
    ```

    The meaning of the templated fields is:

    - `EXPORTED_DATA_TYPE`: the data type of the value to export
    - `EXPORT_PROFILE`: a short, free-form description of the format the data will be exported as
    """

    def is_matching_operation(self, op_config: Operation) -> bool:

        match = issubclass(op_config.module_cls, DataExportModule)
        return match

    def get_export_operations_per_source_type(
        self,
    ) -> typing.Dict[str, typing.Dict[str, typing.Dict[str, Operation]]]:
        """Return all available import operations per value type.

        The result dictionary uses the source type as first level key, a source name/description as 2nd level key,
        and the Operation object as value.
        """

        result: typing.Dict[str, typing.Dict[str, typing.Dict[str, Operation]]] = {}

        for op_config in self.operations.values():

            target_type: str = op_config.module_cls.get_source_value_type()  # type: ignore

            source_type = op_config.module_config["source_type"]
            target_profile = op_config.module_config["target_profile"]

            result.setdefault(target_type, {}).setdefault(source_type, {})[
                target_profile
            ] = op_config

        return result

    def get_export_operations_for_source_type(
        self, value_type: str
    ) -> typing.Dict[str, typing.Dict[str, Operation]]:
        """Return all available import operations that produce data of the specified type."""

        return self.get_export_operations_per_source_type().get(value_type, {})
