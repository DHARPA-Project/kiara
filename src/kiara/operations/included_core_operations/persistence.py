# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from pydantic import Field
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Optional, Union

from kiara.data_types.included_core_types.persistence import LoadConfigValueType
from kiara.defaults import LOAD_CONFIG_DATA_TYPE_NAME
from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.module.operation import (
    BaseOperationDetails,
    ManifestOperationConfig,
    Operation,
    OperationConfig,
)
from kiara.models.values.value import Value, ValueMap
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import KiaraModule, ValueSetSchema
from kiara.modules.included_core_modules.persistence import PersistValueModule
from kiara.operations import OperationType
from kiara.utils import log_message

if TYPE_CHECKING:
    pass


class PersistValueDetails(BaseOperationDetails):

    value_input_field: str = Field(
        description="The (input) field name containing the value to be persisted."
    )
    value_input_type: str = Field(description="The type of the value to be persisted.")
    load_config_output_field: str = Field(
        description="The (output) field name containing the details of the persisted value."
    )
    persistence_target: str = Field(description="The name of the persistence target.")
    persistence_format: str = Field(description="The persistence format.")

    def retrieve_inputs_schema(self) -> ValueSetSchema:

        return {
            "value": {"type": "any", "doc": "The value to persist."},
            "persitence_config": {
                "type": "any",
                "doc": "(Optional) configuration for the persistance process.",
            },
        }

    def retrieve_outputs_schema(self) -> ValueSetSchema:

        return {
            "load_config": {"type": "load_config", "doc": "The saved value details."},
            "bytes_structure": {
                "type": "any",
                "doc": "A structure of serialized bytes.",
            },
        }

    def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:

        return {
            self.value_input_field: inputs["value"],
            "persistence_config": inputs.get("persistence_config", {}),
        }

    def create_operation_outputs(self, outputs: ValueMap) -> Mapping[str, Value]:

        return outputs


class PersistValueOperationType(OperationType[PersistValueDetails]):
    """An operation that takes a value, and saves it to disk and returns details about how to re-assemble the value (via a [kiara.data_types.included_core_types.persistence.LoadConfigValueType] object).

    For a module profile to be picked up by this operation type, it needs to:
    - exactly one output field of type `load_config`
    - either one of (in this order):
      - exactly one input field
      - one input field where the field name equals the type name
      - an input field called 'value'
    """

    _operation_type_name = "persist_value"

    def retrieve_included_operation_configs(
        self,
    ) -> Iterable[Union[Mapping, OperationConfig]]:
        result = []
        for name, module_cls in self._kiara.module_type_classes.items():

            if not issubclass(module_cls, PersistValueModule):
                continue

            for st in module_cls.retrieve_supported_source_types():
                func_name = f"data_type__{st}"
                attr = getattr(module_cls, func_name)
                doc = DocumentationMetadataModel.from_function(attr)
                mc = {"source_type": st}
                oc = ManifestOperationConfig(
                    module_type=name, module_config=mc, doc=doc
                )
                result.append(oc)

        return result

    def check_matching_operation(
        self, module: "KiaraModule"
    ) -> Optional[PersistValueDetails]:

        details = self.extract_details(module)

        if details is None:
            return None
        else:
            return details

    def extract_details(self, module: "KiaraModule") -> Optional[PersistValueDetails]:

        if len(module.inputs_schema) != 1:
            return None

        # if "save" not in module.module_type_name:
        #     return None
        match = None

        for field_name, schema in module.outputs_schema.items():
            if schema.type != LOAD_CONFIG_DATA_TYPE_NAME:
                continue
            else:
                if match is not None:
                    log_message(
                        "ignore.operation",
                        reason=f"More than one field of type '{LOAD_CONFIG_DATA_TYPE_NAME}'",
                        module_type=module.module_type_name,
                    )
                    continue
                else:
                    match = field_name

        if not match:
            return None

        input_field_match = None

        for field_name, schema in module.inputs_schema.items():
            if field_name == schema.type:
                if input_field_match is not None:
                    # we can't deal (yet) with multiple fields
                    log_message(
                        "operation.ignore",
                        module=module.module_type_name,
                        reason=f"more than one input fields of type '{schema.type}'",
                    )
                    input_field_match = None
                    break
                else:
                    input_field_match = field_name

        if input_field_match is not None:
            input_field = input_field_match
        else:
            return None

        input_field_type = module.inputs_schema[input_field].type
        value_schema: ValueSchema = module.outputs_schema[match]
        load_config_type: LoadConfigValueType = self._kiara.type_registry.retrieve_data_type(  # type: ignore  # type: ignore
            data_type_name=value_schema.type,
            data_type_config=value_schema.type_config,
        )  # type: ignore

        persistence_target = load_config_type.type_config.persistence_target  # type: ignore
        persistence_format = load_config_type.type_config.persistence_format  # type: ignore

        if input_field_type == "any":
            operation_id = f"save.to.{persistence_target}.as.{persistence_format}"
        else:
            operation_id = f"save.{input_field_type}.to.{persistence_target}.as.{persistence_format}"

        details = {
            "operation_id": operation_id,
            "value_input_field": input_field,
            "value_input_type": input_field_type,
            "load_config_output_field": match,
            "persistence_target": persistence_target,
            "persistence_format": persistence_format,
            "is_internal_operation": True,
        }

        result = PersistValueDetails.create_operation_details(**details)
        return result

    def get_operation_for_data_type(self, type_name: str) -> Operation:

        lineage = self._kiara.type_registry.get_type_lineage(type_name)
        persist_op: Optional[Operation] = None
        for data_type in lineage:
            match = []
            for op in self.operations.values():
                details = self.retrieve_operation_details(op)
                if details.value_input_type == data_type:
                    match.append(op)

            if match:
                if len(match) > 1:
                    raise Exception(
                        f"Multiple serialization operations found for value type '{type_name}'. This is not supported (yet)."
                    )
                persist_op = match[0]
                break

        if persist_op is None:
            raise Exception(f"Can't find persist operation for type '{type_name}'.")

        return persist_op
