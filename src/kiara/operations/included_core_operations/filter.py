# -*- coding: utf-8 -*-

from typing import TYPE_CHECKING, Any, ClassVar, Dict, Iterable, List, Mapping, Union

import structlog
from pydantic import Field

from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.module.manifest import Manifest
from kiara.models.module.operation import (
    BaseOperationDetails,
    Filter,
    ManifestOperationConfig,
    Operation,
    OperationConfig,
)
from kiara.models.module.pipeline import PipelineConfig
from kiara.models.python_class import KiaraModuleInstance
from kiara.models.values.value_schema import ValueSchema
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

    # def retrieve_inputs_schema(self) -> ValueSetSchema:
    #
    #     result: Dict[str, Union[ValueSchema, Dict[str, Any]]] = {
    #         self.data_type: {
    #             "type": self.data_type,
    #             "type_config": self.data_type_config,
    #             "doc": "The value.",
    #         },
    #     }
    #     for field, schema in self.optional_args.items():
    #         if field in result.keys():
    #             raise Exception(
    #                 f"Can't create 'filter' operation '{self.filter_name}': duplicate input field '{field}'."
    #             )
    #         result[field] = schema
    #     return result
    #
    # def retrieve_outputs_schema(self) -> ValueSetSchema:
    #
    #     return {
    #         self.data_type: {
    #             "type": self.data_type,
    #             "type_config": self.data_type_config,
    #             "doc": "Details about the exported data/files.",
    #         },
    #     }

    # def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
    #
    #     _inputs = dict(inputs)
    #     v = _inputs.pop("value")
    #     assert self.data_type not in _inputs.keys()
    #     _inputs[self.data_type] = v
    #     return _inputs
    #
    # def create_operation_outputs(self, outputs: ValueMap) -> Mapping[str, Value]:
    #
    #     _outputs = dict(outputs)
    #     v = _outputs.pop(self.data_type)
    #     assert "value" not in _outputs.keys()
    #     _outputs["value"] = v
    #     return _outputs


class FilterOperationType(OperationType[FilterOperationDetails]):

    _operation_type_name: ClassVar[str] = "filter"

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
            "module_inputs_schema": module.inputs_schema,
            "module_outputs_schema": module.outputs_schema,
            "operation_id": op_id,
            "data_type": data_type,
            "data_type_config": data_type_config,
            "filter_name": filter_name,
            "optional_args": optional,
            "is_internal_operation": False,
        }

        result: FilterOperationDetails = (
            FilterOperationDetails.create_operation_details(**details)
        )
        return result

    def find_filter_operations_for_data_type(
        self, data_type: str
    ) -> Dict[str, Operation]:

        result = {}
        for op in self.operations.values():
            details: FilterOperationDetails = op.operation_details  # type: ignore
            if details.data_type == data_type:
                result[details.filter_name] = op

        return result

    def get_filter(self, data_type: str, filter_name: str) -> Filter:

        try:
            op = self._kiara.operation_registry.get_operation(operation_id=filter_name)
        except Exception:
            op_id = f"{data_type}_filter.{filter_name}"
            op = self.operations.get(op_id, None)  # type: ignore
            if op is None:
                raise Exception(
                    f"No filter operation '{filter_name}' available for type '{data_type}'."
                )

        inp_match = []
        for input_name, schema in op.inputs_schema.items():
            # TODO: check lineage/profiles
            if schema.type == data_type:
                inp_match.append(input_name)

        if not inp_match:
            raise Exception(
                f"Can't retrieve filter with name '{filter_name}' for data type: '{data_type}': input fields for operation '{op.operation_id}' don't match."
            )
        if len(inp_match) > 1:
            if "value" in inp_match:
                inp_match = ["value"]
            elif data_type in inp_match:
                inp_match = [data_type]
            else:
                raise Exception(
                    f"Can't retrieve filter with name '{filter_name}' for data type: '{data_type}', operation '{op.operation_id}' has multiple potential input fields: {', '.join(inp_match)}."
                )

        input_field = inp_match[0]

        outp_match = []
        for output_name, schema in op.outputs_schema.items():
            # TODO: check lineage/profiles
            if schema.type == data_type:
                outp_match.append(output_name)

        if not outp_match:
            raise Exception(
                f"Can't retrieve filter with name '{filter_name}' for data type: '{data_type}': output fields for operation '{op.operation_id}' don't match."
            )
        if len(outp_match) > 1:
            if "value" in outp_match:
                outp_match = ["value"]
            elif data_type in outp_match:
                outp_match = [data_type]
            else:
                raise Exception(
                    f"Can't retrieve filter with name '{filter_name}' for data type: '{data_type}', operation '{op.operation_id}' has multiple potential output fields: {', '.join(outp_match)}."
                )

        output_field = outp_match[0]
        filter = Filter(
            operation=op,
            input_name=input_field,
            output_name=output_field,
            data_type=data_type,
        )
        return filter

    def assemble_filter_pipeline_config(
        self,
        data_type: str,
        filters: Union[str, Iterable[str], Mapping[str, str]],
        endpoint: Union[None, Manifest, str] = None,
        endpoint_input_field: Union[str, None] = None,
        endpoint_step_id: Union[str, None] = None,
        extra_input_aliases: Union[None, Mapping[str, str]] = None,
        extra_output_aliases: Union[None, Mapping[str, str]] = None,
    ) -> PipelineConfig:
        """
        Assemble a (pipeline) module config to filter values of a specific data type.

        Optionally, a module that uses the filtered dataset as input can be specified.

        # TODO: document filter names
        For the 'filters' argument, the accepted inputs are:
        - a string, in which case a single-step pipeline will be created, with the string referencing the operation id or filter
        - a list of strings: in which case a multi-step pipeline will be created, the step_ids will be calculated automatically
        - a map of string pairs: the keys are step ids, the values operation ids or filter names

        Arguments:
        ---------
            data_type: the type of the data to filter
            filters: a list of operation ids or filter names (and potentiall step_ids if type is a mapping)
            endpoint: optional module to put as last step in the created pipeline
            endpoing_input_field: field name of the input that will receive the filtered value
            endpoint_step_id: id to use for the endpoint step (module type name will be used if not provided)
            extra_output_aliases: extra output aliases to add to the pipeline config

        Returns:
        -------
            the (pipeline) module configuration of the filter pipeline
        """
        steps: List[Mapping[str, Any]] = []
        last_filter_id: Union[str, None] = None
        last_filter_output_name: Union[str, None] = None
        input_aliases: Dict[str, str] = {}
        output_aliases: Dict[str, str] = {}

        if isinstance(filters, str):
            filters = {filters: filters}

        if not isinstance(filters, Mapping):
            _filters = {}
            _step_ids: List[str] = []
            for filter_name in filters:
                step_id = find_free_id(stem=filter_name, current_ids=_step_ids)
                _filters[step_id] = filter_name
            filters = _filters

        for filter_name, step_id in filters.items():
            if not input_aliases:
                input_aliases[f"{filter_name}.value"] = "value"
            filter = self.get_filter(data_type=data_type, filter_name=filter_name)
            step_data = {
                "module_type": filter.operation.operation_id,
                "step_id": step_id,
            }
            if last_filter_id:
                step_data["input_links"] = {
                    filter.input_name: f"{last_filter_id}.{last_filter_output_name}"
                }
            last_filter_id = step_id
            last_filter_output_name = filter.output_name
            steps.append(step_data)
            output_aliases[f"{step_id}.value"] = f"{step_id}__filtered"

        output_aliases[f"{last_filter_id}.{last_filter_output_name}"] = "filtered_value"

        doc = f"Auto generated filter operation ({'->'.join(filters.keys())}) for type '{data_type}'"

        if endpoint:
            endpoint_module = self._kiara.module_registry.create_module(
                manifest=endpoint
            )
            if endpoint_input_field is None:
                matches = []
                for field_name, schema in endpoint_module.inputs_schema.items():
                    # TODO: check profiles/lineage
                    if schema.type == data_type:
                        matches.append(field_name)
                if not matches:
                    raise Exception(
                        f"Can't assemble filter operation: no potential input field of type {data_type} for endpoint module found."
                    )
                elif len(matches) > 1:
                    raise Exception(
                        f"Can't assemble filter operation: multiple potential input fields of type {data_type} for endpoint module found: {', '.join(matches)}"
                    )
                endpoint_input_field = matches[0]

            if not endpoint_step_id:
                endpoint_step_id = find_free_id(
                    stem=endpoint_module.module_type_name, current_ids=filters.values()
                )
            step_data = {
                "module_type": endpoint_module.module_type_name,
                "module_config": endpoint_module.config.model_dump(),
                "step_id": endpoint_step_id,
            }
            step_data["input_links"] = {
                endpoint_input_field: {f"{last_filter_id}.{last_filter_output_name}"}
            }
            # for field_name in endpoint_module.output_names:
            #     output_aliases[f"{endpoint_step_id}.{field_name}"] = f"endpoint__{field_name}"
            doc = f"{doc}, feeding into endpoing module '{endpoint_module.module_type_name}'."
            steps.append(step_data)
        else:
            doc = f"{doc}."

        if extra_output_aliases:
            for k, v in extra_output_aliases.items():
                output_aliases[k] = v

        if extra_input_aliases:
            input_aliases.update(extra_input_aliases)
            # raise NotImplementedError("Extra input aliases not supported yet.")

        pipeline_config = PipelineConfig.from_config(
            pipeline_name="_filter_pipeline",
            data={
                "steps": steps,
                "input_aliases": input_aliases,
                "output_aliases": output_aliases,
                "doc": doc,
            },
        )

        return pipeline_config

    def create_filter_operation(
        self,
        data_type: str,
        filters: Union[Iterable[str], Mapping[str, str]],
        endpoint: Union[None, Manifest, str] = None,
        endpoint_input_field: Union[str, None] = None,
        endpoint_step_id: Union[str, None] = None,
    ) -> Operation:

        pipeline_config = self.assemble_filter_pipeline_config(
            data_type=data_type,
            filters=filters,
            endpoint=endpoint,
            endpoint_input_field=endpoint_input_field,
            endpoint_step_id=endpoint_step_id,
        )

        manifest = Manifest(
            module_type="pipeline", module_config=pipeline_config.model_dump()
        )
        module = self._kiara.module_registry.create_module(manifest=manifest)

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
            module_details=KiaraModuleInstance.from_module(module),
            metadata={},
            doc=pipeline_config.doc,
        )
        return operation
