from typing import Iterable, Union, Mapping, Optional, Any, TYPE_CHECKING, Dict, Type, List, Set

from pydantic import Field

from kiara.models.module.operation import OperationDetails, OperationConfig, Operation
from kiara.models.python_class import PythonClass
from kiara.models.values.value import Value, ValueSet
from kiara.models.values.value_metadata import ValueMetadata

from kiara.modules.operations import OperationType
from kiara.utils.class_loading import find_all_metadata_models

from kiara.modules import KiaraModule, ValueSetSchema


class ExtractMetadataDetails(OperationDetails):

    @classmethod
    def retrieve_inputs_schema(cls) -> ValueSetSchema:
        return {
            "value": {
                "type": "any",
                "doc": "The value to extract metadata from."
            }
        }

    @classmethod
    def retrieve_outputs_schema(cls) -> ValueSetSchema:

        return {
            "metadata": {
                "type": "value_metadata",
                "doc": "The metadata."
            }
        }

    value_types: Set[str] = Field(description="A set of value types this metadata operation can be used with.")
    metadata_key: str = Field(description="The metadata key.")
    metadata_schema: str = Field(description="The (json) schema that describes the metadata dictionary.")

    def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
        return inputs

    def create_operation_outputs(self, outputs: ValueSet) -> ValueSet:
        return outputs

class ExtractMetadataOperationType(OperationType[ExtractMetadataDetails]):
    """An operation that extracts metadata of a specific type from value data.

    For a module profile to be picked up by this operation type, it needs to have:
    - exactly one input field, that input field
    - that input field must have the same name as its value type
    - exactly one output field, where the value has the type 'ValueMetadata'
    """

    def retrieve_included_operation_configs(self) -> Iterable[Union[Mapping, OperationConfig]]:

        all_models = find_all_metadata_models()

        result = []
        for metadata_key, model_cls in all_models.items():
            value_types = model_cls.retrieve_supported_value_types()
            if isinstance(value_types, str):
                value_types = [value_types]
            for value_type in value_types:
                config = {
                    "module_type": "value.extract_metadata",
                    "module_config": {
                        "metadata_key": metadata_key,
                        "value_type": value_type,
                        "metadata_model": PythonClass.from_class(model_cls)
                    },
                    "doc": f"Extract '{metadata_key}' for value type '{value_type}'."
                }
                result.append(config)

        return result


    def check_matching_operation(self, module: "KiaraModule") -> Optional[ExtractMetadataDetails]:

        if len(module.outputs_schema) != 1:
            return None
        if "value_metadata" not in module.outputs_schema or module.outputs_schema["value_metadata"].type != "value_metadata":
            return None
        if len(module.inputs_schema) != 1:
            return None
        if "value" not in module.inputs_schema.keys():
            return None

        value_type = module.inputs_schema["value"].type
        metadata_key=module.get_config_value("metadata_key")
        all_types = self._kiara.type_mgmt.get_sub_types(value_type)

        if value_type == "any":
            op_id = f"extract.{metadata_key}.metadata"
        else:
            op_id = f"extract.{metadata_key}.metadata.from.{value_type}"
        model_cls: ValueMetadata = module.get_config_value("metadata_model").get_class()
        details = ExtractMetadataDetails.create_operation_details(operation_id=op_id, value_types=all_types, metadata_key=metadata_key, metadata_schema=model_cls.schema_json())

        return details


