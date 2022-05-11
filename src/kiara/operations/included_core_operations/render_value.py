# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import structlog
from pydantic import Field
from typing import Any, Dict, Iterable, Mapping, Optional, Type, Union

from kiara.models.module.operation import (
    BaseOperationDetails,
    Operation,
    OperationConfig,
)
from kiara.models.render_value import RenderInstruction
from kiara.models.values.value import Value, ValueMap
from kiara.modules import KiaraModule, ValueSetSchema
from kiara.operations import OperationType
from kiara.registries.models import ModelRegistry
from kiara.utils import log_message

logger = structlog.getLogger()


class RenderValueDetails(BaseOperationDetails):
    """A model that contains information needed to describe an 'extract_metadata' operation."""

    source_data_type: str = Field(description="The data type that will be rendered.")
    rendered_type: str = Field(description="The type of the render output.")
    input_field_name: str = Field(description="The input field name.")
    rendered_field_name: str = Field(description="The result field name.")

    def retrieve_inputs_schema(self) -> ValueSetSchema:
        return {
            "value": {
                "type": self.source_data_type,
                "doc": f"The {self.source_data_type} value to extract metadata from.",
            },
            "render_instruction": {
                "type": "render_instruction",
                "doc": "Configuration how to render the value.",
                "default": {},
            },
        }

    def retrieve_outputs_schema(self) -> ValueSetSchema:

        return {
            "rendered_value": {"type": "value_metadata", "doc": "The rendered data."},
            "render_metadata": {
                "type": "render_metadata",
                "doc": "Metadata associated with this render process.",
            },
        }

    def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
        return {
            self.input_field_name: inputs["value"],
            "render_instruction": inputs["render_instruction"],
        }

    def create_operation_outputs(self, outputs: ValueMap) -> Mapping[str, Value]:
        return {
            "rendered_value": outputs[self.rendered_type],
            "render_metadata": outputs["render_metadata"],
        }


class RenderValueOperationType(OperationType[RenderValueDetails]):
    """An operation that renders a value."""

    _operation_type_name = "render_value"

    def retrieve_included_operation_configs(
        self,
    ) -> Iterable[Union[Mapping, OperationConfig]]:

        model_registry = ModelRegistry.instance()
        all_models = model_registry.get_models_of_type(RenderInstruction)

        result = []
        for model_id, model_cls_info in all_models.items():
            model_cls: Type[RenderInstruction] = model_cls_info.python_class.get_class()  # type: ignore
            source_type = model_cls.retrieve_source_type()
            supported_target_types = model_cls.retrieve_supported_target_types()

            for target in supported_target_types:

                config = {
                    "module_type": "render.value",
                    "module_config": {
                        "render_instruction_type": model_id,
                        "target_type": target,
                    },
                    "doc": f"Render a '{source_type}' value as '{target}'.",
                }
                result.append(config)

        return result

    def check_matching_operation(
        self, module: "KiaraModule"
    ) -> Optional[RenderValueDetails]:

        if len(module.inputs_schema) != 2:
            return None

        if len(module.outputs_schema) != 2:
            return None

        if (
            "render_instruction" not in module.inputs_schema.keys()
            or module.inputs_schema["render_instruction"].type != "render_instruction"
        ):
            return None

        if (
            "render_metadata" not in module.outputs_schema.keys()
            or module.outputs_schema["render_metadata"].type != "render_metadata"
        ):
            return None

        source_type = None
        for field, schema in module.inputs_schema.items():
            if field == "render_instruction":
                continue
            if field == schema.type:
                if source_type:
                    log_message(
                        "operation.ignore",
                        module=module.module_type_name,
                        reason=f"more than one potential source types: {schema.type} - {source_type}",
                    )
                    return None
                source_type = field

        if not source_type:
            return None

        target_type = None
        for field, schema in module.outputs_schema.items():
            if field == "render_metadata":
                continue
            if field == schema.type:
                if target_type:
                    log_message(
                        "operation.ignore",
                        module=module.module_type_name,
                        reason=f"more than one potential target types: {schema.type} - {target_type}",
                    )
                    return None
                target_type = field

        if not target_type:
            return None

        if source_type == "any":
            op_id = f"render.value.as.{target_type}"
        else:
            op_id = f"render.{source_type}.as.{target_type}"

        details = RenderValueDetails.create_operation_details(
            operation_id=op_id,
            source_data_type=source_type,
            rendered_type=target_type,
            input_field_name=source_type,
            rendered_field_name=target_type,
            is_internal_operation=True,
        )

        return details

    def get_render_operations_for_source_type(
        self, source_type: str
    ) -> Mapping[str, Operation]:
        """Return all render operations for the specified data type.

        Arguments:
            source_type: the data type to render

        Returns:
            a mapping with the target type as key, and the operation as value
        """

        lineage = set(
            self._kiara.type_registry.get_type_lineage(data_type_name=source_type)
        )

        result: Dict[str, Operation] = {}

        for data_type in lineage:

            for op_id, op in self.operations.items():
                op_details = self.retrieve_operation_details(op)
                match = op_details.source_data_type == data_type
                if not match:
                    continue
                target_type = op_details.rendered_type
                if target_type in result.keys():
                    continue
                result[target_type] = op

        return result

    def get_render_operation(
        self, source_type: str, target_type: str
    ) -> Optional[Operation]:

        all_ops = self.get_render_operations_for_source_type(source_type=source_type)
        return all_ops.get(target_type, None)
