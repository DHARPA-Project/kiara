# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from typing import ClassVar, Dict, Iterable, Mapping, Union

import structlog
from pydantic import Field

from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.module.operation import (
    BaseOperationDetails,
    ManifestOperationConfig,
    Operation,
    OperationConfig,
)
from kiara.modules import KiaraModule
from kiara.modules.included_core_modules.render_value import RenderValueModule
from kiara.operations import OperationType
from kiara.utils import log_message

logger = structlog.getLogger()


class RenderValueDetails(BaseOperationDetails):
    """A model that contains information needed to describe an 'extract_metadata' operation."""

    source_data_type: str = Field(description="The data type that will be rendered.")
    target_data_type: str = Field(description="The rendered data type.")


class RenderValueOperationType(OperationType[RenderValueDetails]):
    """
    An operation that renders a value.

    A 'render_value' operation typically is named follwing the pattern:

    ```
     'render.<source_type>.as.<target_type>'
    ```

    It has 2 inputs:
      - 'value': the value to render
      - 'reneer_config' a target type-specific configuration dict

    And one output:
      - `render_value_result`: using internal type [RenderValueResultDataType][kiara.data_types.included_core_types.internal.render_value.RenderValueResultDataType]

    """

    _operation_type_name: ClassVar[str] = "render_value"

    def _calculate_op_id(cls, source_type: str, target_type: str):

        if source_type == "any":
            operation_id = f"render.as.{target_type}"
        else:
            operation_id = f"render.{source_type}.as.{target_type}"

        return operation_id

    def retrieve_included_operation_configs(
        self,
    ) -> Iterable[Union[Mapping, OperationConfig]]:

        result = {}
        for name, module_cls in self._kiara.module_type_classes.items():

            if not issubclass(module_cls, RenderValueModule):
                continue

            for (
                source_type,
                target_type,
            ) in module_cls.retrieve_supported_render_combinations():
                if source_type not in self._kiara.data_type_names:
                    log_message("ignore.operation_config", operation_type="render_value", module_type=module_cls._module_type_name, source_type=source_type, target_type=target_type, reason=f"Source type '{source_type}' not registered.")  # type: ignore
                    continue
                if target_type not in self._kiara.data_type_names:
                    log_message(
                        "ignore.operation_config",
                        operation_type="render_value",
                        module_type=module_cls._module_type_name,
                        source_type=source_type,  # type: ignore
                        target_type=target_type,
                        reason=f"Target type '{target_type}' not registered.",
                    )
                    continue
                func_name = f"render__{source_type}__as__{target_type}"
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
                if not attr.startswith("render_as__"):
                    continue

                target_type = attr[11:]
                if target_type not in self._kiara.data_type_names:
                    log_message(
                        "operation_config.ignore",
                        operation_type="render_value",
                        source_type=data_type_name,
                        target_type=target_type,
                        reason=f"Target type '{target_type}' not registered.",
                    )  # type: ignore

                # TODO: inspect signature?
                doc = DocumentationMetadataModel.from_string(
                    f"Render a '{data_type_name}' value as a {target_type}."
                )
                mc = {
                    "source_type": data_type_name,
                    "target_type": target_type,
                }
                oc = ManifestOperationConfig(
                    module_type="render.value", module_config=mc, doc=doc
                )

                result[f"_type_{data_type_name}_{target_type}"] = oc

        return result.values()

    def check_matching_operation(
        self, module: "KiaraModule"
    ) -> Union[RenderValueDetails, None]:

        if len(module.inputs_schema) != 2:
            return None

        if len(module.outputs_schema) != 1:
            return None

        if "value" not in module.inputs_schema.keys():
            return None

        if (
            "render_config" not in module.inputs_schema.keys()
            or module.inputs_schema["render_config"].type != "dict"
        ):
            return None

        if (
            "render_value_result" not in module.outputs_schema.keys()
            or module.outputs_schema["render_value_result"].type
            != "render_value_result"
        ):
            return None

        source_type = module.inputs_schema["value"].type
        target_type = module.get_config_value("target_type")

        if source_type == "any":
            op_id = f"render.as.{target_type}"
        else:
            op_id = f"render.{source_type}.as.{target_type}"

        details: RenderValueDetails = RenderValueDetails.create_operation_details(
            module_inputs_schema=module.inputs_schema,
            module_outputs_schema=module.outputs_schema,
            operation_id=op_id,
            source_data_type=source_type,
            target_data_type=target_type,
            is_internal_operation=True,
        )

        return details

    def get_render_operations_for_source_type(
        self, source_type: str
    ) -> Mapping[str, Operation]:
        """
        Return all render operations for the specified data type.

        Arguments:
        ---------
            source_type: the data type to render

        Returns:
        -------
            a mapping with the target type as key, and the operation as value
        """
        if source_type not in self._kiara.data_type_names:
            source_type = "any"

        lineage = self._kiara.type_registry.get_type_lineage(data_type_name=source_type)

        result: Dict[str, Operation] = {}

        for data_type in lineage:

            for op_id, op in self.operations.items():
                op_details = self.retrieve_operation_details(op)
                match = op_details.source_data_type == data_type
                if not match:
                    continue
                target_type = op_details.target_data_type
                if target_type in result.keys():
                    continue
                result[target_type] = op

        return result

    def get_render_operations_for_target_type(
        self, target_type: str
    ) -> Mapping[str, Operation]:
        """
        Return all render operations that renders to the specified data type.

        Arguments:
        ---------
            target_type: the result data type

        Returns:
        -------
            a mapping with the source type as key, and the operation as value
        """
        # TODO: consider type lineages

        if target_type not in self._kiara.data_type_names:
            raise Exception(f"Invalid target data type: {target_type}")

        result: Dict[str, Operation] = {}

        for op_id, op in self.operations.items():
            op_details = self.retrieve_operation_details(op)
            match = op_details.target_data_type == target_type
            if not match:
                continue
            source_type = op_details.source_data_type
            if source_type in result.keys():
                continue

            result[source_type] = op

        return result

    def get_render_operation(
        self, source_type: str, target_type: str
    ) -> Union[Operation, None]:

        all_ops = self.get_render_operations_for_source_type(source_type=source_type)
        return all_ops.get(target_type, None)
