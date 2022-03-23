from typing import Iterable, Union, Mapping, Optional, Any, TYPE_CHECKING, Dict, Type

from pydantic import Field, BaseModel

from kiara.defaults import SERIALIZED_DATA_TYPE_NAME, LOAD_CONFIG_DATA_TYPE_NAME
from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.module.operation import BaseOperationDetails, OperationConfig, Operation

from kiara.models.values.value import Value, ValueSet
from kiara.models.values.value_schema import ValueSchema
from kiara.modules.included_core_modules.persistence import PersistValueModule
from kiara.modules.operations import OperationType
from kiara.utils import log_message
from kiara.data_types.included_core_types.persistence import LoadConfigValueType

from kiara.modules import KiaraModule, ValueSetSchema


class PersistValueDetails(BaseOperationDetails):

    @classmethod
    def retrieve_inputs_schema(cls) -> ValueSetSchema:

        return {
            "value": {
                "type": "any",
                "doc": "The value to persist."
            },
            "target": {
                "type": "string",
                "doc": "The target path or url."
            },
            "base_name": {
                "type": "string",
                "doc": "A string to use as base token when persisting (might or might not be used)."
            }
        }

    @classmethod
    def retrieve_outputs_schema(cls) -> ValueSetSchema:

        return {
            "load_config": {
                "type": "load_config",
                "doc": "The saved value details."
            }
        }


    value_input_field: str = Field(description="The (input) field name containing the value to be persisted.")
    value_input_type: str = Field(description="The type of the value to be persisted.")
    load_config_output_field: str = Field(description="The (output) field name containing the details of the persisted value.")
    target_input_field: str = Field(description="The (input) field name containing the target value.")
    base_name_input_field: Optional[str] = Field(description="The (input) field name containing the base_name value.", default=None)
    persistence_target: str = Field(description="The name of the persistence target.")
    persistence_format: str = Field(description="The persistence format.")\


    def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:

        return {
            self.value_input_field: inputs["value"],
            self.target_input_field: inputs["target"],
            self.base_name_input_field: inputs["base_name"]
        }

    def create_operation_outputs(self, outputs: ValueSet) -> Mapping[str, Value]:

        return {
            "load_config": outputs.get_value_obj(self.load_config_output_field)
        }


class PersistValueOperationType(OperationType[PersistValueDetails]):
    """An operation that takes a value, and saves it to disk and returns details about how to re-assemble the value (via a [kiara.data_types.included_core_types.persistence.LoadConfigValueType] object).

    For a module profile to be picked up by this operation type, it needs to:
    - exactly one output field of type `load_config`
    - either one of (in this order):
      - exactly one input field
      - one input field where the field name equals the type name
      - an input field called 'value'
    """

    def retrieve_included_operation_configs(self) -> Iterable[Union[Mapping, OperationConfig]]:
        result = []
        for name, module_cls in self._kiara.module_types.items():

            if not issubclass(module_cls, PersistValueModule):
                continue

            for st in module_cls.retrieve_supported_source_types():
                func_name = f"data_type__{st}"
                attr = getattr(module_cls, func_name)
                doc = DocumentationMetadataModel.from_function(attr)
                mc = {
                    "source_type": st
                }
                oc = OperationConfig(module_type=name, module_config=mc, doc=doc)
                result.append(oc)

        return result

    def check_matching_operation(self, module: "KiaraModule") -> Optional[PersistValueDetails]:

        details = self.extract_details(module)

        if details is None:
            return None
        else:
            return details

    def extract_details(self, module: "KiaraModule") -> Optional[PersistValueDetails]:

        if len(module.inputs_schema) < 2:
            return None

        match = None
        for field_name, schema in module.outputs_schema.items():
            if schema.type != LOAD_CONFIG_DATA_TYPE_NAME:
                continue
            else:
                if match != None:
                    log_message("ignore.operation", reason=f"More than one field of type '{LOAD_CONFIG_DATA_TYPE_NAME}'", module_type=module.module_type_name)
                    continue
                else:
                    match = field_name

        if not match:
            return None

        input_field_match = None
        target_field_match = None
        base_name_field_match = None

        for field_name, schema in module.inputs_schema.items():
            if field_name == schema.type:
                if input_field_match is not None:
                    # we can't deal (yet) with multiple fields
                    log_message("operation.ignore", module=module.module_type_name, reason=f"more than one input fields of type '{schema.type}'")
                    input_field_match = None
                    break
                else:
                    input_field_match = field_name
            elif field_name == "target":
                target_field_match = "target"
            elif field_name == "base_name":
                base_name_field_match = "base_name"

        if input_field_match is not None:
            input_field = input_field_match
        else:
            return None

        if target_field_match is None:
            return None

        input_field_type = module.inputs_schema[input_field].type
        value_schema: ValueSchema = module.outputs_schema[match]
        load_config_type: LoadConfigValueType = self._kiara.type_mgmt.retrieve_data_type(
            data_type_name=value_schema.type, data_type_config=value_schema.type_config)  # type: ignore

        persistence_target = load_config_type.type_config.persistence_target
        persistence_format = load_config_type.type_config.persistence_format

        if input_field_type == "any":
            operation_id = f"save.to.{persistence_target}.as.{persistence_format}"
        else:
            operation_id = f"save.{input_field_type}.to.{persistence_target}.as.{persistence_format}"

        details = {
            "operation_id": operation_id,
            "value_input_field": input_field,
            "value_input_type": input_field_type,
            "target_input_field": target_field_match,
            "base_name_input_field": base_name_field_match,
            "load_config_output_field": match,
            "persistence_target": persistence_target,
            "persistence_format": persistence_format,
            "is_internal_operation": True
        }

        result = PersistValueDetails.create_operation_details(**details)
        return result

    def get_operation_for_data_type(self, type_name: str) -> Operation:

        lineage = self._kiara.type_mgmt.get_type_lineage(type_name)

        persist_op: Optional[Operation] = None
        for data_type in lineage:
            match = []
            for op in self.operations.values():
                details = self.retrieve_operation_details(op)
                if details.value_input_type == data_type:
                    match.append(op)

            if match:
                if len(match) > 1:
                    raise Exception(f"Multiple serialization operations found for value type '{type_name}'. This is not supported (yet).")
                persist_op = match[0]

        if persist_op is None:
            raise Exception(f"Can't find persist operation for type '{type_name}'.")

        return persist_op


