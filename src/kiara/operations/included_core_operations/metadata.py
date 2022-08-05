# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from pydantic import Field
from typing import Iterable, Mapping, Type, Union

from kiara.models.module.operation import (
    BaseOperationDetails,
    Operation,
    OperationConfig,
)
from kiara.models.values.value_metadata import ValueMetadata
from kiara.modules import KiaraModule
from kiara.operations import OperationType
from kiara.registries.models import ModelRegistry


class ExtractMetadataDetails(BaseOperationDetails):
    """A model that contains information needed to describe an 'extract_metadata' operation."""

    data_type: str = Field(
        description="The data type this metadata operation can be used with."
    )
    metadata_key: str = Field(description="The metadata key.")
    input_field_name: str = Field(description="The input field name.")
    result_field_name: str = Field(description="The result field name.")

    # def retrieve_inputs_schema(self) -> ValueSetSchema:
    #     return {
    #         "value": {
    #             "type": self.data_type,
    #             "doc": f"The {self.data_type} value to extract metadata from.",
    #         }
    #     }
    #
    # def retrieve_outputs_schema(self) -> ValueSetSchema:
    #
    #     return {"value_metadata": {"type": "value_metadata", "doc": "The metadata."}}
    #
    # def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
    #     return {self.input_field_name: inputs["value"]}
    #
    # def create_operation_outputs(self, outputs: ValueMap) -> ValueMap:
    #
    #     return outputs


class ExtractMetadataOperationType(OperationType[ExtractMetadataDetails]):
    """An operation that extracts metadata of a specific type from value data.

    For a module profile to be picked up by this operation type, it needs to have:
    - exactly one input field
    - that input field must have the same name as its value type, or be 'value'
    - exactly one output field, whose field name is called 'value_metadata', and where the value has the type 'internal_model'
    """

    _operation_type_name = "extract_metadata"

    def retrieve_included_operation_configs(
        self,
    ) -> Iterable[Union[Mapping, OperationConfig]]:

        model_registry = ModelRegistry.instance()
        all_models = model_registry.get_models_of_type(ValueMetadata)

        result = []
        for model_id, model_cls_info in all_models.item_infos.items():
            model_cls: Type[ValueMetadata] = model_cls_info.python_class.get_class()  # type: ignore
            metadata_key = model_cls._metadata_key  # type: ignore
            data_types = model_cls.retrieve_supported_data_types()
            if isinstance(data_types, str):
                data_types = [data_types]
            for data_type in data_types:

                config = {
                    "module_type": "value.extract_metadata",
                    "module_config": {
                        "data_type": data_type,
                        "kiara_model_id": model_cls._kiara_model_id,  # type: ignore
                    },
                    "doc": f"Extract '{metadata_key}' metadata for value type '{data_type}'.",
                }
                result.append(config)

        return result

    def check_matching_operation(
        self, module: "KiaraModule"
    ) -> Union[ExtractMetadataDetails, None]:

        if len(module.outputs_schema) != 1:
            return None
        if (
            "value_metadata" not in module.outputs_schema
            or module.outputs_schema["value_metadata"].type != "internal_model"
        ):
            return None
        if len(module.inputs_schema) != 1:
            return None

        input_field_name = next(iter(module.inputs_schema.keys()))
        input_schema = module.inputs_schema.get(input_field_name)
        assert input_schema is not None
        if input_field_name != input_schema.type and input_field_name != "value":
            return None

        data_type_name = module.inputs_schema["value"].type
        model_id: str = module.get_config_value("kiara_model_id")

        registry = ModelRegistry.instance()
        metadata_model_cls = registry.get_model_cls(
            kiara_model_id=model_id, required_subclass=ValueMetadata
        )

        metadata_key = metadata_model_cls._metadata_key  # type: ignore

        if data_type_name == "any":
            op_id = f"extract.{metadata_key}.metadata"
        else:
            op_id = f"extract.{metadata_key}.metadata.from.{data_type_name}"

        details = ExtractMetadataDetails.create_operation_details(
            module_inputs_schema=module.inputs_schema,
            module_outputs_schema=module.outputs_schema,
            operation_id=op_id,
            data_type=data_type_name,
            metadata_key=metadata_key,
            input_field_name=input_field_name,
            result_field_name="value_metadata",
            is_internal_operation=True,
        )

        return details

    def get_operations_for_data_type(self, data_type: str) -> Mapping[str, Operation]:
        """Return all available metadata extract operations for the provided type (and it's parent types).

        Arguments:
            data_type: the value type

        Returns:
            a mapping with the metadata type as key, and the operation as value
        """

        lineage = set(
            self._kiara.type_registry.get_type_lineage(data_type_name=data_type)
        )

        result = {}

        for op_id, op in self.operations.items():
            op_details = self.retrieve_operation_details(op)
            included = op_details.data_type in lineage
            if not included:
                continue
            metadata_key = op_details.metadata_key
            if metadata_key in result:
                raise Exception(
                    f"Duplicate metadata operations for type '{metadata_key}'."
                )

            result[metadata_key] = op

        return result
