# -*- coding: utf-8 -*-

# Copyright (c) 2021, University of Luxembourg / DHARPA project
# Copyright (c) 2021, Markus Binsteiner
#
# Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from pydantic import Field
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Optional, Union

from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.module.operation import (
    BaseOperationDetails,
    ManifestOperationConfig,
    Operation,
    OperationConfig,
)
from kiara.models.values.value import Value, ValueMap
from kiara.modules import ValueSetSchema
from kiara.operations import OperationType
from kiara.utils import log_message

if TYPE_CHECKING:
    from kiara import KiaraModule

# class SerializeDetails(BaseOperationDetails):
#
#     value_input_field: str = Field(
#         description="The (input) field name containing the value to be serialized."
#     )
#     value_input_type: str = Field(description="The type of the value to be serialized.")
#     serialized_value_output_field: str = Field(
#         description="The (output) field name containing the serialzied form of the value."
#     )
#     serialization_profile: str = Field(
#         description="The name of the serialization profile."
#     )
#
#     def retrieve_inputs_schema(self) -> ValueSetSchema:
#
#         return {"value": {"type": "any", "doc": "The value to serialzie."}}
#
#     def retrieve_outputs_schema(self) -> ValueSetSchema:
#
#         return {
#             "serialized_value": {
#                 "type": "serialized_value",
#                 "doc": "The serialized value details (and data).",
#             }
#         }
#
#     def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
#
#         result = {
#             self.value_input_type: inputs["value"]
#         }
#         return result
#
#     def create_operation_outputs(self, outputs: ValueMap) -> Mapping[str, Value]:
#         return outputs
#
#
#
# class SerializeOperationType(OperationType[SerializeDetails]):
#     """An operation that takes a value, and serializes it into the format suitable to the [`serialized_value`][kiara.data_types.included_core_types.SeriailzedValue] value type.
#
#     For a module profile to be picked up by this operation type, it needs to have:
#     - exactly one output field of type `serialized_value`
#     - either one of (in this order):
#       - exactly one input field
#       - one input field where the field name equals the type name
#       - an input field called 'value'
#     """
#
#     _operation_type_name = "serialize"
#
#     def retrieve_included_operation_configs(
#         self,
#     ) -> Iterable[Union[Mapping, OperationConfig]]:
#         result = []
#         for name, module_cls in self._kiara.module_type_classes.items():
#             if not hasattr(module_cls, "retrieve_supported_source_types"):
#                 continue
#             for st in module_cls.retrieve_supported_source_types():
#                 func_name = f"from__{st}"
#                 attr = getattr(module_cls, func_name)
#                 doc = DocumentationMetadataModel.from_function(attr)
#                 mc = {"value_type": st}
#                 oc = ManifestOperationConfig(
#                     module_type=name, module_config=mc, doc=doc
#                 )
#                 result.append(oc)
#
#         return result
#
#     def check_matching_operation(
#         self, module: "KiaraModule"
#     ) -> Optional[SerializeDetails]:
#
#         details = self.extract_details(module)
#
#         if details is None:
#             return None
#         else:
#             return details
#
#     def extract_details(self, module: "KiaraModule") -> Optional[SerializeDetails]:
#
#         match = None
#         for field_name, schema in module.outputs_schema.items():
#             if schema.type != SERIALIZED_DATA_TYPE_NAME:
#                 continue
#             else:
#                 if match is not None:
#                     log_message(
#                         "ignore.operation",
#                         reason=f"More than one field of type '{SERIALIZED_DATA_TYPE_NAME}'",
#                         module_type=module.module_type_name,
#                     )
#                     continue
#                 else:
#                     match = field_name
#
#         if not match:
#             return None
#
#         if len(module.inputs_schema) == 1:
#             input_field: Optional[str] = next(iter(module.inputs_schema.keys()))
#         else:
#             input_field_match = None
#             for field_name, schema in module.inputs_schema.items():
#                 if field_name == schema.type:
#                     if input_field_match is not None:
#                         input_field_match = None
#                         break
#                     else:
#                         input_field_match = field_name
#             if input_field_match is not None:
#                 input_field = input_field_match
#             elif "value" in module.inputs_schema.keys():
#                 input_field = "value"
#             else:
#                 input_field = None
#
#         if input_field is None:
#             return None
#
#         input_field_type = module.inputs_schema[input_field].type
#         value_schema: ValueSchema = module.outputs_schema[match]
#         serialized_value_type: SerializedValueType = self._kiara.type_registry.retrieve_data_type(  # type: ignore
#             data_type_name=value_schema.type,
#             data_type_config=value_schema.type_config,
#         )  # type: ignore
#
#         if input_field_type == "any":
#             operation_id = f"serialize.as.{serialized_value_type.serialization_profile}"
#         else:
#             operation_id = (
#                 f"serialize.{input_field_type}.as.{serialized_value_type.serialization_profile}"
#             )
#
#         details: Dict[str, Any] = {
#             "operation_id": operation_id,
#             "value_input_field": input_field,
#             "value_input_type": input_field_type,
#             "serialized_value_output_field": match,
#             "serialization_profile": serialized_value_type.serialization_profile,
#             "is_internal_operation": True,
#         }
#
#         result = SerializeDetails.construct(**details)
#         return result
#
#     def find_serialzation_operation_for_type(self, type_name: str) -> Operation:
#
#         lineage = self._kiara.type_registry.get_type_lineage(type_name)
#         serialize_op: Optional[Operation] = None
#         for data_type in lineage:
#             match = []
#             op = None
#             for op in self.operations.values():
#                 details = self.retrieve_operation_details(op)
#                 if details.value_input_type == data_type:
#                     match.append(op)
#
#             if match:
#                 if len(match) > 1:
#                     assert op is not None
#                     raise Exception(
#                         f"Multiple serialization operations found for type of '{op.operation_id}'. This is not supported (yet)."
#                     )
#                 serialize_op = match[0]
#
#         if serialize_op is None:
#             raise Exception(
#                 f"Can't find serialization operation for type '{type_name}'."
#             )
#
#         return serialize_op
#
#     # def find_operation(self, **op_args: Any) -> Operation:
#     #     op_conf = SerializeValueInputs(**op_args)
#     #     input_value_type = op_conf.value.data_type_name
#     #     op = self.find_serialzation_operation_for_type(input_value_type)
#     #     return op
#
#     # def apply(self, inputs: SerializeValueInputs) -> SerializeValueOutputs:
#     #
#     #     input_value_type = inputs.value.data_type_name
#     #
#     #     op = self.find_serialzation_operation_for_type(input_value_type)
#     #     op_details = self.retrieve_operation_details(op)
#     #     op_inputs = {
#     #         op_details.value_input_field: inputs.value
#     #     }
#     #
#     #     result = self._kiara.execute(manifest=op, inputs=op_inputs)
#     #     op_details = self.retrieve_operation_details(op)
#     #     result_data = {"serialized_value": result.get_value_obj(op_details.serialized_value_output_field)}
#     #     return SerializeValueOutputs.construct(**result_data)
#
class DeSerializeDetails(BaseOperationDetails):

    value_type: str = Field(
        "The name of the input field for the serialized version of the value."
    )
    value_input_field: str = Field(
        "The name of the input field for the serialized version of the value."
    )
    object_output_field: str = Field(
        description="The (output) field name containing the deserialized python class."
    )
    serialization_profile: str = Field(
        description="The name for the serialization profile used on the source value."
    )
    target_profile: str = Field(description="The target profile name.")
    # target_class: PythonClass = Field(
    #     description="The python class of the result object."
    # )

    def retrieve_inputs_schema(self) -> ValueSetSchema:

        return {"value": {"type": self.value_type, "doc": "The value to de-serialize."}}

    def retrieve_outputs_schema(self) -> ValueSetSchema:

        return {
            "python_object": {
                "type": "python_object",
                "doc": "The de-serialized python object instance.",
            }
        }

    def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:

        result = {self.value_input_field: inputs["value"]}
        return result

    def create_operation_outputs(self, outputs: ValueMap) -> Mapping[str, Value]:

        return outputs


class DeSerializeOperationType(OperationType[DeSerializeDetails]):
    """An operation that takes a value, and serializes it into the format suitable to the [`serialized_value`][kiara.data_types.included_core_types.SeriailzedValue] value type.

    For a module profile to be picked up by this operation type, it needs to have:
    - exactly one output field of type `serialized_value`
    - either one of (in this order):
      - exactly one input field
      - one input field where the field name equals the type name
      - an input field called 'value'
    """

    _operation_type_name = "deserialize"

    def retrieve_included_operation_configs(
        self,
    ) -> Iterable[Union[Mapping, OperationConfig]]:
        result = []
        for name, module_cls in self._kiara.module_type_classes.items():

            if not hasattr(module_cls, "retrieve_source_value_type"):
                continue
            if not hasattr(module_cls, "retrieve_supported_target_profiles"):
                continue
            if not hasattr(module_cls, "retrieve_supported_serialization_profile"):
                continue

            value_type = module_cls.retrieve_source_value_type()  # type: ignore
            serialization_profile = module_cls.retrieve_supported_serialization_profile()  # type: ignore
            for _profile_name, cls in module_cls.retrieve_supported_target_profiles().items():  # type: ignore
                func_name = f"to__{_profile_name}"
                attr = getattr(module_cls, func_name)
                doc = DocumentationMetadataModel.from_function(attr)
                mc = {
                    "value_type": value_type,
                    "target_profile": _profile_name,
                    "serialization_profile": serialization_profile
                    # "target_class": PythonClass.from_class(cls),
                }
                oc = ManifestOperationConfig(
                    module_type=name, module_config=mc, doc=doc
                )
                result.append(oc)

        return result

    def check_matching_operation(
        self, module: "KiaraModule"
    ) -> Optional[DeSerializeDetails]:

        details = self.extract_details(module)

        if details is None:
            return None
        else:
            return details

    def extract_details(self, module: "KiaraModule") -> Optional[DeSerializeDetails]:

        result_field_name = None
        for field_name, schema in module.outputs_schema.items():
            if schema.type != "python_object":
                continue
            else:
                if result_field_name is not None:
                    log_message(
                        "ignore.operation",
                        reason=f"found more than one potential result value field: {result_field_name} -- {field_name}'",
                        module_type=module.module_type_name,
                    )
                    continue
                else:
                    result_field_name = field_name

        if not result_field_name:
            return None

        input_field_name = None
        for field_name, schema in module.inputs_schema.items():
            if field_name != schema.type:
                continue
            if input_field_name is not None:
                log_message(
                    "ignore.operation",
                    reason=f"found more than one potential result value field: {result_field_name} -- {field_name}'",
                    module_type=module.module_type_name,
                )
                continue
            else:
                input_field_name = field_name

        if not input_field_name:
            return None

        try:
            value_type = module.config.get("value_type")
            target_profile = module.config.get("target_profile")
            serialization_profile = module.config.get("serialization_profile")
            # target_class = module.config.get("target_class")
        except Exception as e:
            log_message(
                "ignore.operation",
                reason=str(e),
                module_type=module.module_type_name,
            )
            return None

        if value_type not in self._kiara.type_registry.data_type_names:
            log_message(
                "ignore.operation",
                reason=f"Invalid value type: {value_type}",
                module_type=module.module_type_name,
            )
            return None

        if input_field_name == "any":
            operation_id = f"deserialize.value"
        else:
            operation_id = f"deserialize.{input_field_name}.as.{target_profile}"

        details: Dict[str, Any] = {
            "operation_id": operation_id,
            "value_type": input_field_name,
            "value_input_field": input_field_name,
            "object_output_field": result_field_name,
            "target_profile": target_profile,
            "serialization_profile": serialization_profile,
            # "target_class": target_class,
            "is_internal_operation": True,
        }

        result = DeSerializeDetails.construct(**details)
        return result

    def find_deserialization_operations_for_type(
        self, type_name: str
    ) -> List[Operation]:

        lineage = self._kiara.type_registry.get_type_lineage(type_name)
        result = []
        for data_type in lineage:
            match = []
            for op in self.operations.values():
                details = self.retrieve_operation_details(op)
                if details.value_type == data_type:
                    match.append(op)

            result.extend(match)

        return result

    def find_deserialzation_operation_for_type_and_profile(
        self, type_name: str, serialization_profile: str
    ) -> List[Operation]:

        lineage = self._kiara.type_registry.get_type_lineage(type_name)
        serialize_ops: List[Operation] = []
        for data_type in lineage:
            match = []
            op = None
            for op in self.operations.values():
                details = self.retrieve_operation_details(op)
                if (
                    details.value_type == data_type
                    and details.serialization_profile == serialization_profile
                ):
                    match.append(op)

            if match:
                if len(match) > 1:
                    assert op is not None
                    raise Exception(
                        f"Multiple deserialization operations found for data type '{type_name}' and serialization profile '{serialization_profile}'. This is not supported (yet)."
                    )
                serialize_ops.append(match[0])

        return serialize_ops
