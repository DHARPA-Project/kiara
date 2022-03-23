# -*- coding: utf-8 -*-
import abc
from typing import (
    TYPE_CHECKING,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Type,
    Union, Any,
)

from pydantic import Field, PrivateAttr

from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.module.operation import OperationDetails, OperationSchema, OperationConfig
from kiara.models.values.value import ValueSet, Value
from kiara.models.values.value_schema import ValueSchema


from kiara.modules import KiaraModule
from kiara.modules.operations import OperationType


class CustomModuleOperationDetails(OperationDetails):

    @classmethod
    def create_from_module(cls, module: KiaraModule):

        return CustomModuleOperationDetails(operation_id=module.module_type_name, module_inputs_schema=module.inputs_schema, module_outputs_schema=module.outputs_schema)

    module_inputs_schema: Dict[str, ValueSchema] = Field(description="The input schemas of the module.")
    module_outputs_schema: Dict[str, ValueSchema] = Field(description="The output schemas of the module.")
    _op_schema: OperationSchema = PrivateAttr(default=None)

    def get_operation_schema(self) -> OperationSchema:

        if self._op_schema is not None:
            return self._op_schema

        self._op_schema = OperationSchema(alias=self.operation_id, inputs_schema=self.module_inputs_schema, outputs_schema=self.module_outputs_schema)
        return self._op_schema

    def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
        return inputs

    def create_operation_outputs(self, outputs: ValueSet) -> Mapping[str, Value]:
        return outputs


class CustomModuleOperationType(OperationType[CustomModuleOperationDetails]):

    def retrieve_included_operation_configs(self) -> Iterable[Union[Mapping, OperationConfig]]:

        result = []
        for name, module_cls in self._kiara.module_types.items():
            mod_conf = module_cls._config_cls
            if mod_conf.requires_config():
                continue
            doc = DocumentationMetadataModel.from_class_doc(module_cls)
            oc = OperationConfig(module_type=name, doc=doc)
            result.append(oc)
        return result

    def check_matching_operation(self, module: "KiaraModule") -> Optional[str]:
        mod_conf = module.__class__._config_cls

        if not mod_conf.requires_config():
            is_internal = module.characteristics.is_internal
            op_details = CustomModuleOperationDetails.create_operation_details(operation_id=module.module_type_name, module_inputs_schema=module.inputs_schema, module_outputs_schema=module.outputs_schema, is_internal_operation=is_internal)
            return op_details
        else:
            return None


