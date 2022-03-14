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

from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.module.operation import OperationConfig, OperationDetails
from kiara.models.values.value import ValueSet, Value
from kiara.modules import ValueSetSchema
from kiara.modules.operations import OperationType

if TYPE_CHECKING:
    from kiara import KiaraModule


class GenericOperationDetails(OperationDetails):

    @classmethod
    def retrieve_inputs_schema(cls) -> ValueSetSchema:
        return {}

    @classmethod
    def retrieve_outputs_schema(cls) -> ValueSetSchema:
        return {}

    def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
        raise NotImplementedError()

    def create_operation_outputs(self, outputs: ValueSet) -> Mapping[str, Value]:
        raise NotImplementedError()

class AllOperationType(OperationType[GenericOperationDetails]):

    def retrieve_included_operation_configs(self) -> Iterable[Union[Mapping, OperationConfig]]:
        return []
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
            op_details = GenericOperationDetails.create_operation_details(details={"operation_id": module.module_type_id})
            return op_details
        else:
            return None

