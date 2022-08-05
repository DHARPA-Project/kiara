# -*- coding: utf-8 -*-

# Copyright (c) 2021, University of Luxembourg / DHARPA project
# Copyright (c) 2021, Markus Binsteiner
#
# Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from pydantic import Field
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Union

from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.module.operation import (
    BaseOperationDetails,
    ManifestOperationConfig,
    Operation,
    OperationConfig,
)
from kiara.operations import OperationType
from kiara.utils import log_message

if TYPE_CHECKING:
    from kiara.modules import KiaraModule


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

    # def retrieve_inputs_schema(self) -> ValueSetSchema:
    #
    #     return {"value": {"type": self.value_type, "doc": "The value to de-serialize."}}
    #
    # def retrieve_outputs_schema(self) -> ValueSetSchema:
    #
    #     return {
    #         "python_object": {
    #             "type": "python_object",
    #             "doc": "The de-serialized python object instance.",
    #         }
    #     }
    #
    # def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
    #
    #     result = {self.value_input_field: inputs["value"]}
    #     return result
    #
    # def create_operation_outputs(self, outputs: ValueMap) -> Mapping[str, Value]:
    #
    #     return outputs


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

            if not hasattr(module_cls, "retrieve_serialized_value_type"):
                continue
            if not hasattr(module_cls, "retrieve_supported_target_profiles"):
                continue
            if not hasattr(module_cls, "retrieve_supported_serialization_profile"):
                continue

            try:
                value_type = module_cls.retrieve_serialized_value_type()  # type: ignore
            except TypeError:
                raise Exception(
                    f"Can't retrieve source value type for deserialization module '{module_cls.__name__}'. This is most likely a bug, maybe you are missing a '@classmethod' annotation on the 'retrieve_source_value_type' method?"
                )
            try:
                serialization_profile = module_cls.retrieve_supported_serialization_profile()  # type: ignore
            except TypeError:
                raise Exception(
                    f"Can't retrieve supported serialization profiles for deserialization module '{module_cls.__name__}'. This is most likely a bug, maybe you are missing a '@classmethod' annotation on the 'retrieve_supported_serialization_profile' method?"
                )

            try:
                target_profiles = module_cls.retrieve_supported_target_profiles()  # type: ignore
            except TypeError:
                raise Exception(
                    f"Can't retrieve supported target profile for deserialization module '{module_cls.__name__}'. This is most likely a bug, maybe you are missing a '@classmethod' annotation on the 'retrieve_supported_target_profile' method?"
                )

            for _profile_name, cls in target_profiles.items():
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
    ) -> Union[DeSerializeDetails, None]:

        details = self.extract_details(module)

        if details is None:
            return None
        else:
            return details

    def extract_details(self, module: "KiaraModule") -> Union[DeSerializeDetails, None]:

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
            operation_id = "deserialize.value"
        else:
            operation_id = f"deserialize.{input_field_name}.as.{target_profile}"

        details: Dict[str, Any] = {
            "module_inputs_schema": module.inputs_schema,
            "module_outputs_schema": module.outputs_schema,
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
