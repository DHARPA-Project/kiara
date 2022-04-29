# -*- coding: utf-8 -*-
import structlog
from pydantic import Field
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Optional, Union

from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.module.operation import (
    BaseOperationDetails,
    ManifestOperationConfig,
    OperationConfig,
)
from kiara.models.values.value import Value, ValueMap
from kiara.modules import ValueSetSchema
from kiara.modules.included_core_modules.create_from import CreateFromModule
from kiara.operations import OperationType
from kiara.utils import is_debug

if TYPE_CHECKING:
    from kiara.modules import KiaraModule

logger = structlog.getLogger()


class CreateValueFromDetails(BaseOperationDetails):

    source_type: str = Field(description="The type of the value to be created.")
    target_type: str = Field(description="The result type.")

    def retrieve_inputs_schema(self) -> ValueSetSchema:

        return {
            self.source_type: {"type": self.source_type, "doc": "The source value."},
        }

    def retrieve_outputs_schema(self) -> ValueSetSchema:

        return {
            self.target_type: {"type": self.target_type, "doc": "The result value."}
        }

    def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:

        return inputs

    def create_operation_outputs(self, outputs: ValueMap) -> Mapping[str, Value]:

        return outputs


class CreateFromOperationType(OperationType[CreateValueFromDetails]):

    _operation_type_name = "create_from"

    def _calculate_op_id(self, source_type: str, target_type: str):

        if source_type == "any":
            operation_id = f"create.{target_type}"
        else:
            operation_id = f"create.{target_type}.from.{source_type}"

        return operation_id

    def retrieve_included_operation_configs(
        self,
    ) -> Iterable[Union[Mapping, OperationConfig]]:

        result = {}
        for name, module_cls in self._kiara.module_type_classes.items():
            if not hasattr(module_cls, "retrieve_supported_create_combinations"):
                continue

            try:
                supported_combinations = module_cls.retrieve_supported_create_combinations()  # type: ignore
                for sup_comb in supported_combinations:
                    source_type = sup_comb["source_type"]
                    target_type = sup_comb["target_type"]
                    func = sup_comb["func"]

                    if source_type not in self._kiara.data_type_names:
                        logger.debug(
                            "ignore.operation_config",
                            module_type=name,
                            reason=f"Source type '{source_type}' not registered.",
                        )
                        continue
                    if target_type not in self._kiara.data_type_names:
                        logger.debug(
                            "ignore.operation_config",
                            module_type=name,
                            reason=f"Target type '{target_type}' not registered.",
                        )
                        continue
                    if not hasattr(module_cls, func):
                        logger.debug(
                            "ignore.operation_config",
                            module_type=name,
                            reason=f"Specified create function '{func}' not available.",
                        )
                        continue

                    mc = {"source_type": source_type, "target_type": target_type}
                    # TODO: check whether module config actually supports those, for now, only 'CreateFromModule' subtypes are supported
                    _func = getattr(module_cls, func)
                    doc = DocumentationMetadataModel.from_function(_func)

                    oc = ManifestOperationConfig(
                        module_type=name, module_config=mc, doc=doc
                    )
                    op_id = self._calculate_op_id(
                        source_type=source_type, target_type=target_type
                    )
                    result[op_id] = oc
            except Exception as e:
                if is_debug():
                    import traceback

                    traceback.print_exc()
                logger.debug(
                    "ignore.create_operation_instance", module_type=name, reason=e
                )
                continue

        return result.values()

    def check_matching_operation(
        self, module: "KiaraModule"
    ) -> Optional[CreateValueFromDetails]:

        if not isinstance(module, CreateFromModule):
            return None

        source_type = None
        for field_name, schema in module.inputs_schema.items():
            if field_name == schema.type:
                if source_type is not None:
                    logger.debug(
                        "ignore.operation",
                        operation_type="create_from",
                        reason=f"more than one possible target type field: {field_name}",
                    )
                    return None
                source_type = field_name

        if source_type is None:
            return None

        target_type = None
        for field_name, schema in module.outputs_schema.items():
            if field_name == schema.type:
                if target_type is not None:
                    logger.debug(
                        "ignore.operation",
                        operation_type="create_from",
                        reason=f"more than one possible target type field: {field_name}",
                    )
                    return None
                target_type = field_name

        if target_type is None:
            return None

        op_id = self._calculate_op_id(source_type=source_type, target_type=target_type)

        if (
            "any" in self._kiara.type_registry.get_type_lineage(target_type)
            and target_type != "any"
        ):
            is_internal = False
        else:
            is_internal = True

        details = {
            "operation_id": op_id,
            "source_type": source_type,
            "target_type": target_type,
            "is_internal_operation": is_internal,
        }

        result = CreateValueFromDetails.create_operation_details(**details)
        return result
