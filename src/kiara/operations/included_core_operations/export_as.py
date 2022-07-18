# -*- coding: utf-8 -*-
import structlog
from pydantic import Field
from typing import TYPE_CHECKING, Iterable, Mapping, Union

from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.module.operation import (
    BaseOperationDetails,
    ManifestOperationConfig,
    OperationConfig,
)
from kiara.models.values.value_schema import ValueSchema
from kiara.modules.included_core_modules.export_as import DataExportModule
from kiara.operations import OperationType
from kiara.utils import log_exception

if TYPE_CHECKING:
    from kiara.modules import KiaraModule

logger = structlog.getLogger()


class ExportAsOperationDetails(BaseOperationDetails):

    source_type: str = Field(description="The type of the value to be created.")
    target_profile: str = Field(description="The result profile type.")
    optional_args: Mapping[str, ValueSchema] = Field(description="Optional arguments.")

    # def retrieve_inputs_schema(self) -> ValueSetSchema:
    #
    #     result: Dict[str, Union[ValueSchema, Dict[str, Any]]] = {
    #         self.source_type: {"type": self.source_type, "doc": "The source value."},
    #     }
    #     for field, schema in self.optional_args.items():
    #         if field in result.keys():
    #             raise Exception(
    #                 f"Can't create 'create_from' operation '{self.source_type}' -> '{self.target_profile}': duplicate input field '{field}'."
    #             )
    #         result[field] = schema
    #     return result
    #
    # def retrieve_outputs_schema(self) -> ValueSetSchema:
    #
    #     return {
    #         "export_details": {
    #             "type": "dict",
    #             "doc": "Details about the exported data/files.",
    #         }
    #     }


class ExportAsOperationType(OperationType[ExportAsOperationDetails]):

    _operation_type_name = "export_as"

    def _calculate_op_id(self, source_type: str, target_profile: str):

        if source_type == "any":
            operation_id = f"export.as.{target_profile}"
        else:
            operation_id = f"export.{source_type}.as.{target_profile}"

        return operation_id

    def retrieve_included_operation_configs(
        self,
    ) -> Iterable[Union[Mapping, OperationConfig]]:

        result = []
        for name, module_cls in self._kiara.module_type_classes.items():
            if not hasattr(module_cls, "retrieve_supported_export_combinations"):
                continue

            try:
                supported_combinations = module_cls.retrieve_supported_export_combinations()  # type: ignore
                for sup_comb in supported_combinations:
                    source_type = sup_comb["source_type"]
                    target_profile = sup_comb["target_profile"]
                    func = sup_comb["func"]

                    if source_type not in self._kiara.data_type_names:
                        logger.debug(
                            "ignore.operation_config",
                            module_type=name,
                            reason=f"Source type '{source_type}' not registered.",
                        )
                        continue

                    if not hasattr(module_cls, func):
                        logger.debug(
                            "ignore.operation_config",
                            module_type=name,
                            reason=f"Specified create function '{func}' not available.",
                        )
                        continue

                    mc = {"source_type": source_type, "target_profile": target_profile}
                    # TODO: check whether module config actually supports those, for now, only 'DataExportModule' subtypes are supported
                    _func = getattr(module_cls, func)
                    doc = DocumentationMetadataModel.from_function(_func)

                    oc = ManifestOperationConfig(
                        module_type=name, module_config=mc, doc=doc
                    )
                    result.append(oc)
            except Exception as e:
                log_exception(e)
                logger.debug(
                    "ignore.create_operation_instance", module_type=name, reason=e
                )
                continue

        return result

    def check_matching_operation(
        self, module: "KiaraModule"
    ) -> Union[ExportAsOperationDetails, None]:

        if not isinstance(module, DataExportModule):
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

        target_profile = module.config.target_profile

        op_id = self._calculate_op_id(
            source_type=source_type, target_profile=target_profile
        )

        optional = {}
        for field, schema in module.inputs_schema.items():
            if field in [source_type]:
                continue
            optional[field] = schema

        details = {
            "module_inputs_schema": module.inputs_schema,
            "module_outputs_schema": module.outputs_schema,
            "operation_id": op_id,
            "source_type": source_type,
            "target_profile": target_profile,
            "optional_args": optional,
            "is_internal_operation": False,
        }

        result = ExportAsOperationDetails.create_operation_details(**details)
        return result
