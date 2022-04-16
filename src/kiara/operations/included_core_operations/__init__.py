# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import structlog
from pydantic import Field, PrivateAttr
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Optional, Union

from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.module.operation import (
    ManifestOperationConfig,
    OperationConfig,
    OperationDetails,
    OperationSchema,
)
from kiara.models.values.value import Value, ValueMap
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import KiaraModule
from kiara.operations import OperationType

if TYPE_CHECKING:
    pass


logger = structlog.getLogger()


class CustomModuleOperationDetails(OperationDetails):
    @classmethod
    def create_from_module(cls, module: KiaraModule):

        return CustomModuleOperationDetails(
            operation_id=module.module_type_name,
            module_inputs_schema=module.inputs_schema,
            module_outputs_schema=module.outputs_schema,
        )

    module_inputs_schema: Mapping[str, ValueSchema] = Field(
        description="The input schemas of the module."
    )
    module_outputs_schema: Mapping[str, ValueSchema] = Field(
        description="The output schemas of the module."
    )
    _op_schema: OperationSchema = PrivateAttr(default=None)

    def get_operation_schema(self) -> OperationSchema:

        if self._op_schema is not None:
            return self._op_schema

        self._op_schema = OperationSchema(
            alias=self.operation_id,
            inputs_schema=self.module_inputs_schema,
            outputs_schema=self.module_outputs_schema,
        )
        return self._op_schema

    def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
        return inputs

    def create_operation_outputs(self, outputs: ValueMap) -> Mapping[str, Value]:
        return outputs


class CustomModuleOperationType(OperationType[CustomModuleOperationDetails]):

    _operation_type_name = "custom_module"

    def retrieve_included_operation_configs(
        self,
    ) -> Iterable[Union[Mapping, OperationConfig]]:

        result = []
        for name, module_cls in self._kiara.module_type_classes.items():
            mod_conf = module_cls._config_cls
            if mod_conf.requires_config():
                logger.debug(
                    "ignore.custom_operation",
                    module_type=name,
                    reason="config required",
                )
                continue
            doc = DocumentationMetadataModel.from_class_doc(module_cls)
            oc = ManifestOperationConfig(module_type=name, doc=doc)
            result.append(oc)
        return result

    def check_matching_operation(
        self, module: "KiaraModule"
    ) -> Optional[CustomModuleOperationDetails]:
        mod_conf = module.__class__._config_cls

        if not mod_conf.requires_config():
            is_internal = module.characteristics.is_internal
            op_details = CustomModuleOperationDetails.create_operation_details(
                operation_id=module.module_type_name,
                module_inputs_schema=module.inputs_schema,
                module_outputs_schema=module.outputs_schema,
                is_internal_operation=is_internal,
            )
            return op_details
        else:
            return None
