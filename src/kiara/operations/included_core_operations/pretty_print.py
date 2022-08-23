# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from pydantic import Field
from typing import Dict, Iterable, Mapping, Union

from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.module.operation import (
    BaseOperationDetails,
    ManifestOperationConfig,
    Operation,
    OperationConfig,
)
from kiara.modules import KiaraModule
from kiara.modules.included_core_modules.pretty_print import PrettyPrintModule
from kiara.operations import OperationType
from kiara.utils import log_message


class PrettyPrintDetails(BaseOperationDetails):

    source_type: str = Field(description="The type of the value to be rendered.")
    target_type: str = Field(description="The type of the render result.")

    # def retrieve_inputs_schema(self) -> ValueSetSchema:
    #
    #     return {
    #         "value": {"type": "any", "doc": "The value to persist."},
    #         "render_type": {
    #             "type": "string",
    #             "doc": "The render target/type of render output.",
    #         },
    #         "render_config": {
    #             "type": "dict",
    #             "doc": "A value type specific configuration for how to render the data.",
    #             "optional": True,
    #         },
    #     }
    #
    # def retrieve_outputs_schema(self) -> ValueSetSchema:
    #
    #     return {"rendered_value": {"type": "any", "doc": "The rendered value."}}
    #
    # def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
    #
    #     return {
    #         self.source_type: inputs["value"],
    #         "render_config": inputs.get("render_config", None),
    #     }
    #
    # def create_operation_outputs(self, outputs: ValueMap) -> Mapping[str, Value]:
    #     return {"rendered_value": outputs.get_value_obj("rendered_value")}


class PrettyPrintOperationType(OperationType[PrettyPrintDetails]):
    """An operation that takes a value, and renders into a format that can be printed for output..

    For a module profile to be picked up by this operation type, it needs to have:
    - exactly one output field named "rendered_value"
    - exactly two input fields, one of them named after the type it supports, and the other called 'render_config', of type 'dict'
    """

    _operation_type_name = "pretty_print"

    def _calculate_op_id(self, source_type: str, target_type: str):

        if source_type == "any":
            operation_id = f"pretty_print.as.{target_type}"
        else:
            operation_id = f"pretty_print.{source_type}.as.{target_type}"

        return operation_id

    def retrieve_included_operation_configs(
        self,
    ) -> Iterable[Union[Mapping, OperationConfig]]:

        result = {}
        for name, module_cls in self._kiara.module_type_classes.items():

            if not issubclass(module_cls, PrettyPrintModule):
                continue

            for (
                source_type,
                target_type,
            ) in module_cls.retrieve_supported_render_combinations():
                if source_type not in self._kiara.data_type_names:
                    log_message("ignore.operation_config", operation_type="pretty_print", module_type=module_cls._module_type_name, source_type=source_type, target_type=target_type, reason=f"Source type '{source_type}' not registered.")  # type: ignore
                    continue
                if target_type not in self._kiara.data_type_names:
                    log_message(
                        "ignore.operation_config",
                        operation_type="pretty_print",
                        module_type=module_cls._module_type_name,
                        source_type=source_type,  # type: ignore
                        target_type=target_type,
                        reason=f"Target type '{target_type}' not registered.",
                    )
                    continue
                func_name = f"pretty_print__{source_type}__as__{target_type}"
                attr = getattr(module_cls, func_name)
                doc = DocumentationMetadataModel.from_function(attr)
                mc = {"source_type": source_type, "target_type": target_type}
                oc = ManifestOperationConfig(
                    module_type=name, module_config=mc, doc=doc
                )
                op_id = self._calculate_op_id(
                    source_type=source_type, target_type=target_type
                )
                result[op_id] = oc

        for data_type_name, data_type_class in self._kiara.data_type_classes.items():
            for attr in data_type_class.__dict__.keys():
                if not attr.startswith("pretty_print_as__"):
                    continue

                target_type = attr[17:]
                if target_type not in self._kiara.data_type_names:
                    log_message(
                        "operation_config.ignore",
                        operation_type="pretty_print",
                        source_type=data_type_name,
                        target_type=target_type,
                        reason=f"Target type '{target_type}' not registered.",
                    )  # type: ignore

                # TODO: inspect signature?
                doc = DocumentationMetadataModel.from_string(
                    f"Pretty print a {data_type_name} value as a {target_type}."
                )
                mc = {
                    "source_type": data_type_name,
                    "target_type": target_type,
                }
                oc = ManifestOperationConfig(
                    module_type="pretty_print.value", module_config=mc, doc=doc
                )
                result[f"_type_{data_type_name}__{target_type}"] = oc
        return result.values()

    def check_matching_operation(
        self, module: "KiaraModule"
    ) -> Union[PrettyPrintDetails, None]:

        details = self.extract_details(module)

        if details is None:
            return None
        else:
            return details

    def extract_details(self, module: "KiaraModule") -> Union[PrettyPrintDetails, None]:

        if len(module.inputs_schema) != 2 or len(module.outputs_schema) != 1:
            return None

        if "rendered_value" not in module.outputs_schema.keys():
            return None
        target_type = module.outputs_schema["rendered_value"].type

        if "value" not in module.inputs_schema.keys():
            return None
        if "render_config" not in module.inputs_schema.keys():
            return None

        input_field_match = "value"

        input_field_type = module.inputs_schema[input_field_match].type

        operation_id = self._calculate_op_id(
            source_type=input_field_type, target_type=target_type
        )

        details = {
            "module_inputs_schema": module.inputs_schema,
            "module_outputs_schema": module.outputs_schema,
            "operation_id": operation_id,
            "source_type": input_field_type,
            "target_type": target_type,
            "is_internal_operation": True,
        }

        result = PrettyPrintDetails.create_operation_details(**details)
        return result

    def get_target_types_for(self, source_type: str) -> Mapping[str, Operation]:

        # TODO: support for sub-types
        result: Dict[str, Operation] = {}
        for operation in self.operations.values():
            details = self.retrieve_operation_details(operation)

            if details.source_type == source_type:
                target_type = details.target_type
                if target_type in result.keys():
                    raise Exception(
                        f"More than one operation for pretty_print combination '{source_type}'/'{target_type}', this is not supported (for now)."
                    )
                result[target_type] = operation

        return result

    def get_operation_for_render_combination(
        self, source_type: str, target_type: str
    ) -> Operation:

        type_lineage = self._kiara.type_registry.get_type_lineage(
            data_type_name=source_type
        )

        for st in type_lineage:
            target_types = self.get_target_types_for(source_type=st)
            if not target_types:
                continue
            if target_type not in target_types.keys():
                raise Exception(
                    f"No operation that produces '{target_type}' for source type: {st}."
                )
            result = target_types[target_type]
            return result

        raise Exception(f"No pretty_print opration(s) for source type: {source_type}.")
