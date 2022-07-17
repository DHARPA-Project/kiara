# -*- coding: utf-8 -*-

import structlog
from pydantic import Field
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Union

from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.module import KiaraModuleClass
from kiara.models.module.manifest import Manifest
from kiara.models.module.operation import (
    BaseOperationDetails,
    ManifestOperationConfig,
    Operation,
    OperationConfig,
)
from kiara.models.module.pipeline import PipelineConfig
from kiara.models.values.value import Value, ValueMap
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import ValueSetSchema
from kiara.modules.included_core_modules.filter import FilterModule
from kiara.operations import OperationType
from kiara.operations.included_core_operations.pipeline import PipelineOperationDetails
from kiara.utils import find_free_id, log_exception

if TYPE_CHECKING:
    from kiara.modules import KiaraModule

logger = structlog.getLogger()


class FilterOperationDetails(BaseOperationDetails):

    data_type: str = Field(description="The data type of the value to be filtered.")
    data_type_config: Mapping[str, Any] = Field(
        description="The configuration of the data type to be filtered.",
        default_factory=dict,
    )
    filter_name: str = Field(description="The filter operation name.")
    optional_args: Mapping[str, ValueSchema] = Field(description="Optional arguments.")

    def retrieve_inputs_schema(self) -> ValueSetSchema:

        result: Dict[str, Union[ValueSchema, Dict[str, Any]]] = {
            "value": {
                "type": self.data_type,
                "type_config": self.data_type_config,
                "doc": "The value.",
            },
        }
        for field, schema in self.optional_args.items():
            if field in result.keys():
                raise Exception(
                    f"Can't create 'filter' operation '{self.filter_name}': duplicate input field '{field}'."
                )
            result[field] = schema
        return result

    def retrieve_outputs_schema(self) -> ValueSetSchema:

        return {
            "value": {
                "type": self.data_type,
                "type_config": self.data_type_config,
                "doc": "Details about the exported data/files.",
            },
        }

    def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:

        _inputs = dict(inputs)
        v = _inputs.pop("value")
        assert self.data_type not in _inputs.keys()
        _inputs[self.data_type] = v
        return _inputs

    def create_operation_outputs(self, outputs: ValueMap) -> Mapping[str, Value]:

        _outputs = dict(outputs)
        v = _outputs.pop(self.data_type)
        assert "value" not in _outputs.keys()
        _outputs["value"] = v
        return _outputs


class FilterOperationType(OperationType[FilterOperationDetails]):

    _operation_type_name = "filter"

    def retrieve_included_operation_configs(
        self,
    ) -> Iterable[Union[Mapping, OperationConfig]]:

        result = []

        for name, module_cls in self._kiara.module_type_classes.items():

            if not issubclass(module_cls, FilterModule):
                continue

            try:
                data_type_data = module_cls.get_supported_type()
                data_type: str = data_type_data["type"]  # type: ignore
                # data_type_config: Mapping[str, Any] = data_type["type_config"]  # type: ignore

                # TODO; try to create data type obj?
                if data_type not in self._kiara.data_type_names:
                    logger.debug(
                        "ignore.operation_config",
                        module_type=name,
                        reason=f"Data type '{data_type}' not registered.",
                    )
                    continue

                supported_filters = module_cls.get_supported_filters()
                for filter in supported_filters:

                    func_name = f"filter__{filter}"

                    if not hasattr(module_cls, func_name):
                        logger.debug(
                            "ignore.operation_config",
                            module_type=name,
                            reason=f"Specified filter function '{func_name}' not available.",
                        )
                        continue

                    mc = {"filter_name": filter}
                    # TODO: check whether module config actually supports those, for now, only 'DataExportModule' subtypes are supported
                    _func = getattr(module_cls, func_name)
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
    ) -> Union[FilterOperationDetails, None]:

        if not isinstance(module, FilterModule):
            return None

        data_type_data = module.__class__.get_supported_type()
        data_type: str = data_type_data["type"]  # type: ignore
        data_type_config: Mapping[str, Any] = data_type_data["type_config"]  # type: ignore

        filter_name = module.get_config_value("filter_name")

        op_id = f"{data_type}_filter.{filter_name}"

        optional = {}
        for field, schema in module.inputs_schema.items():
            if field in [data_type]:
                continue
            optional[field] = schema

        details = {
            "operation_id": op_id,
            "data_type": data_type,
            "data_type_config": data_type_config,
            "filter_name": filter_name,
            "optional_args": optional,
            "is_internal_operation": False,
        }

        result = FilterOperationDetails.create_operation_details(**details)
        return result

    def get_filter(self, data_type: str, filter_name: str) -> Operation:

        op_id = f"{data_type}_filter.{filter_name}"
        op = self.operations.get(op_id, None)
        if op is None:
            raise Exception(
                f"No filter operation '{filter_name}' available for type '{data_type}'."
            )

        return op

    def create_filter_operation(
        self, data_type: str, filters: Iterable[str]
    ) -> Operation:

        steps: List[Mapping[str, Any]] = []
        last_filter = None
        step_ids: List[str] = []
        for filter_name in filters:
            op = self.get_filter(data_type=data_type, filter_name=filter_name)
            step_id = find_free_id(stem=filter_name, current_ids=step_ids)
            step_ids.append(step_id)
            step_data = {"module_type": op.operation_id, "step_id": step_id}
            if last_filter:
                step_data["input_links"] = {"value": f"{last_filter}.value"}
            last_filter = step_id
            steps.append(step_data)

        pipeline_config = PipelineConfig.from_config(
            pipeline_name="_filter_pipeline", data={"steps": steps}
        )

        from kiara.utils.graphs import print_ascii_graph

        print_ascii_graph(pipeline_config.structure.execution_graph)

        manifest = Manifest(
            module_type="pipeline", module_config=pipeline_config.dict()
        )
        module = self._kiara.create_module(manifest=manifest)

        doc = DocumentationMetadataModel.create(
            f"Auto generated filter operation for type '{data_type}'."
        )

        op_details = PipelineOperationDetails.create_operation_details(
            operation_id=module.config.pipeline_name,
            pipeline_inputs_schema=module.inputs_schema,
            pipeline_outputs_schema=module.outputs_schema,
            pipeline_config=module.config,
        )
        operation = Operation(
            module_type=manifest.module_type,
            module_config=manifest.module_config,
            operation_id=op_details.operation_id,
            operation_details=op_details,
            module_details=KiaraModuleClass.from_module(module),
            metadata={},
            doc=doc,
        )
        return operation
