# -*- coding: utf-8 -*-
from pydantic import Field
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Optional, Set, Union

from kiara.models.module.operation import (
    BaseOperationDetails,
    Operation,
    OperationConfig,
)
from kiara.models.python_class import PythonClass
from kiara.models.values.value import ValueSet
from kiara.modules import KiaraModule, ValueSetSchema
from kiara.modules.operations import OperationType
from kiara.utils.class_loading import find_all_value_metadata_models

if TYPE_CHECKING:
    pass


class ExtractMetadataDetails(BaseOperationDetails):
    """A model that contains information needed to describe an 'extract_metadata' operation."""

    @classmethod
    def retrieve_inputs_schema(cls) -> ValueSetSchema:
        return {"value": {"type": "any", "doc": "The value to extract metadata from."}}

    @classmethod
    def retrieve_outputs_schema(cls) -> ValueSetSchema:

        return {"value_metadata": {"type": "value_metadata", "doc": "The metadata."}}

    data_types: Set[str] = Field(
        description="A set of value types this metadata operation can be used with."
    )
    metadata_key: str = Field(description="The metadata key.")
    input_field_name: str = Field(description="The input field name.")
    result_field_name: str = Field(description="The result field name.")

    def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
        return {self.input_field_name: inputs["value"]}

    def create_operation_outputs(self, outputs: ValueSet) -> ValueSet:

        return outputs


class ExtractMetadataOperationType(OperationType[ExtractMetadataDetails]):
    """An operation that extracts metadata of a specific type from value data.

    For a module profile to be picked up by this operation type, it needs to have:
    - exactly one input field
    - that input field must have the same name as its value type, or be 'value'
    - exactly one output field, whose field name is called 'value_metadata', and where the value has the type 'internal_model'
    """

    def retrieve_included_operation_configs(
        self,
    ) -> Iterable[Union[Mapping, OperationConfig]]:

        all_models = find_all_value_metadata_models()

        result = []
        for metadata_key, model_cls in all_models.items():
            data_types = model_cls.retrieve_supported_data_types()
            if isinstance(data_types, str):
                data_types = [data_types]
            for data_type in data_types:

                config = {
                    "module_type": "value.extract_metadata",
                    "module_config": {
                        "data_type": data_type,
                        "metadata_model": PythonClass.from_class(model_cls),
                    },
                    "doc": f"Extract '{metadata_key}' for value type '{data_type}'.",
                }
                result.append(config)

        return result

    def check_matching_operation(
        self, module: "KiaraModule"
    ) -> Optional[ExtractMetadataDetails]:

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
        if input_field_name != input_schema.type and input_field_name != "value":
            return None

        data_type_name = module.inputs_schema["value"].type
        # metadata_key=module.get_config_value("metadata_key")
        metadata_model: PythonClass = module.get_config_value("metadata_model")
        metadata_key = metadata_model.get_class()._metadata_key  # type: ignore
        all_types = self._kiara.type_registry.get_sub_types(data_type_name)

        if data_type_name == "any":
            op_id = f"extract.{metadata_key}.metadata"
        else:
            op_id = f"extract.{metadata_key}.metadata.from.{data_type_name}"

        details = ExtractMetadataDetails.create_operation_details(
            operation_id=op_id,
            data_types=all_types,
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
            common_types = op_details.data_types.intersection(lineage)
            if not common_types:
                continue
            metadata_key = op_details.metadata_key
            if metadata_key in result:
                raise Exception(
                    f"Duplicate metadata operations for type '{metadata_key}'."
                )

            result[metadata_key] = op

        return result