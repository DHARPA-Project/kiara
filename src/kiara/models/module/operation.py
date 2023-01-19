# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import orjson
import structlog
from pydantic import Field, PrivateAttr, validator
from rich import box
from rich.console import Group, RenderableType
from rich.syntax import Syntax
from rich.table import Table
from typing import TYPE_CHECKING, Any, Dict, Iterable, Mapping, Union

from kiara.defaults import PYDANTIC_USE_CONSTRUCT
from kiara.models import KiaraModel
from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.module.jobs import JobConfig
from kiara.models.module.manifest import Manifest
from kiara.models.module.pipeline import PipelineConfig
from kiara.models.python_class import KiaraModuleInstance
from kiara.models.values.value import ValueMap, ValueMapReadOnly
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import InputOutputObject, KiaraModule, ValueMapSchema
from kiara.utils.output import create_table_from_field_schemas

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.models.module.pipeline.structure import PipelineStructure

logger = structlog.getLogger()


class OperationSchema(InputOutputObject):
    def __init__(
        self, alias: str, inputs_schema: ValueMapSchema, outputs_schema: ValueMapSchema
    ):

        allow_empty_inputs = True
        allow_empty_outputs = True

        self._inputs_schema_static: ValueMapSchema = inputs_schema
        self._outputs_schema_static: ValueMapSchema = outputs_schema
        super().__init__(
            alias=alias,
            allow_empty_inputs_schema=allow_empty_inputs,
            allow_empty_outputs_schema=allow_empty_outputs,
        )

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:
        return self._inputs_schema_static

    def create_outputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        return self._outputs_schema_static


class OperationDetails(KiaraModel):

    _kiara_model_id = "instance.operation_details"

    # inputs_map: Dict[str, str] = Field(description="A map with the operations input fields as keys, and the underlying modules input fields as values, used to translate input value maps.")
    # outputs_map: Dict[str, str] = Field(description="A map with the operations input fields as keys, and the underlying modules input fields as values, used to translate input value maps.")

    @classmethod
    def create_operation_details(cls, **details: Any):

        if PYDANTIC_USE_CONSTRUCT:
            result = cls.construct(**details)
        else:
            result = cls(**details)

        return result

    operation_id: str = Field(description="The id of the operation.")
    is_internal_operation: bool = Field(
        description="Whether this operation is mainly used kiara-internally. Helps to hide it in UIs (operation lists etc.).",
        default=False,
    )

    def _retrieve_id(self) -> str:
        return self.operation_id

    @property
    def inputs_schema(self) -> Mapping[str, ValueSchema]:
        """The input schema for this module."""

        return self.get_operation_schema().inputs_schema

    @property
    def outputs_schema(self) -> Mapping[str, ValueSchema]:
        """The input schema for this module."""

        return self.get_operation_schema().outputs_schema

    def get_operation_schema(self) -> OperationSchema:
        raise NotImplementedError()


class BaseOperationDetails(OperationDetails):

    _kiara_model_id = "instance.operation_details.base"

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


class OperationConfig(KiaraModel):

    doc: DocumentationMetadataModel = Field(
        description="Documentation for this operation."
    )

    @validator("doc", pre=True)
    def validate_doc(cls, value):
        return DocumentationMetadataModel.create(value)

    @abc.abstractmethod
    def retrieve_module_type(self, kiara: "Kiara") -> str:
        pass

    @abc.abstractmethod
    def retrieve_module_config(self, kiara: "Kiara") -> Mapping[str, Any]:
        pass


class ManifestOperationConfig(OperationConfig):

    _kiara_model_id = "instance.operation_config.manifest"

    module_type: str = Field(description="The module type.")
    module_config: Dict[str, Any] = Field(
        default_factory=dict, description="The configuration for the module."
    )

    def retrieve_module_type(self, kiara: "Kiara") -> str:
        return self.module_type

    def retrieve_module_config(self, kiara: "Kiara") -> Mapping[str, Any]:
        return self.module_config


class PipelineOperationConfig(OperationConfig):

    _kiara_model_id = "instance.operation_config.pipeline"

    pipeline_name: str = Field(description="The pipeline id.")
    pipeline_config: Mapping[str, Any] = Field(description="The pipeline config data.")
    module_map: Dict[str, Any] = Field(
        description="A lookup map to resolves operation ids to module names/configs.",
        default_factory=dict,
    )
    metadata: Mapping[str, Any] = Field(
        description="Additional metadata for the pipeline.", default_factory=dict
    )

    @validator("pipeline_config")
    def validate_pipeline_config(cls, value):
        # TODO
        assert isinstance(value, Mapping)
        assert "steps" in value.keys()

        return value

    def retrieve_module_type(self, kiara: "Kiara") -> str:
        return "pipeline"

    def retrieve_module_config(self, kiara: "Kiara") -> Mapping[str, Any]:

        # using _from_config here because otherwise we'd enter an infinite loop
        pipeline_config = PipelineConfig._from_config(
            pipeline_name=self.pipeline_name,
            data=self.pipeline_config,
            kiara=kiara,
            module_map=self.module_map,
        )
        return pipeline_config.dict()

    @property
    def required_module_types(self) -> Iterable[str]:

        return [step["module_type"] for step in self.pipeline_config["steps"]]

    def __repr__(self):

        return f"{self.__class__.__name__}(pipeline_name={self.pipeline_name} required_modules={list(self.required_module_types)} instance_id={self.instance_id} fields=[{', '.join(self.__fields__.keys())}])"


class Operation(Manifest):

    _kiara_model_id = "instance.operation"

    @classmethod
    def create_from_module(
        cls, module: KiaraModule, doc: Union[Any, None] = None
    ) -> "Operation":

        from kiara.operations.included_core_operations import (
            CustomModuleOperationDetails,
        )

        op_id = f"{module.module_type_name}._{module.module_instance_cid}"
        if module.is_pipeline():
            from kiara.operations.included_core_operations.pipeline import (
                PipelineOperationDetails,
            )

            details = PipelineOperationDetails.create_operation_details(
                operation_id=module.config.pipeline_name,
                pipeline_inputs_schema=module.inputs_schema,
                pipeline_outputs_schema=module.outputs_schema,
                pipeline_config=module.config,
            )
        else:
            details = CustomModuleOperationDetails.create_from_module(module=module)

        if doc is not None:
            doc = DocumentationMetadataModel.create(doc)
        else:
            doc = DocumentationMetadataModel.from_class_doc(module.__class__)

        operation = Operation(
            module_type=module.module_type_name,
            module_config=module.config.dict(),
            operation_id=op_id,
            operation_details=details,
            module_details=KiaraModuleInstance.from_module(module),
            doc=doc,
        )
        operation._module = module
        return operation

    operation_id: str = Field(description="The (unique) id of this operation.")
    operation_details: OperationDetails = Field(
        description="The operation specific details of this operation."
    )
    doc: DocumentationMetadataModel = Field(
        description="Documentation for this operation."
    )

    module_details: KiaraModuleInstance = Field(
        description="The class of the underlying module."
    )
    metadata: Mapping[str, Any] = Field(
        description="Additional metadata for this operation.", default_factory=dict
    )

    _module: Union["KiaraModule", None] = PrivateAttr(default=None)
    _pipeline_config: Union[None, PipelineConfig] = PrivateAttr(default=None)

    def _retrieve_data_to_hash(self) -> Any:
        return {"operation_id": self.operation_id, "manifest": self.manifest_cid}

    def _retrieve_id(self) -> str:
        return self.operation_id

    @property
    def module(self) -> "KiaraModule":
        if self._module is None:
            m_cls = self.module_details.get_class()
            self._module = m_cls(module_config=self.module_config)
        return self._module

    @property
    def inputs_schema(self) -> Mapping[str, ValueSchema]:
        return self.operation_details.inputs_schema

    @property
    def outputs_schema(self) -> Mapping[str, ValueSchema]:
        return self.operation_details.outputs_schema

    def prepare_job_config(
        self, kiara: "Kiara", inputs: Mapping[str, Any]
    ) -> JobConfig:

        augmented_inputs = (
            self.operation_details.get_operation_schema().augment_module_inputs(
                inputs=inputs
            )
        )

        # module_inputs = self.operation_details.create_module_inputs(
        #     inputs=augmented_inputs
        # )

        job_config = kiara.job_registry.prepare_job_config(
            manifest=self, inputs=augmented_inputs
        )
        return job_config

    def run(self, kiara: "Kiara", inputs: Mapping[str, Any]) -> ValueMap:

        logger.debug("run.operation", operation_id=self.operation_id)
        job_config = self.prepare_job_config(kiara=kiara, inputs=inputs)

        job_id = kiara.job_registry.execute_job(job_config=job_config)
        outputs: ValueMap = kiara.job_registry.retrieve_result(job_id=job_id)

        result = self.process_job_outputs(outputs=outputs)

        return result

    def process_job_outputs(self, outputs: ValueMap) -> ValueMap:

        # op_outputs = self.operation_details.create_operation_outputs(outputs=outputs)

        value_set = ValueMapReadOnly(value_items=outputs, values_schema=self.outputs_schema)  # type: ignore
        return value_set

    # def run(self, _attach_lineage: bool = True, **inputs: Any) -> ValueMap:
    #
    #     return self.module.run(_attach_lineage=_attach_lineage, **inputs)

    # def create_html(self, **config) -> str:
    #
    #     r = self.create_renderable(**config)
    #     p = Panel(r, title=f"Operation: {self.operation_id}", title_align="left")
    #     mime_bundle = p._repr_mimebundle_(include=[], exclude=[])  # type: ignore
    #     return mime_bundle["text/html"]

    @property
    def pipeline_config(self) -> PipelineConfig:

        if not self.module.is_pipeline():
            raise Exception(
                f"Can't retrieve pipeline details from operation '{self.operation_id}: not a pipeline operation type.'"
            )

        op_details = self.operation_details
        return op_details.pipeline_config  # type: ignore

    @property
    def pipeline_structure(self) -> "PipelineStructure":
        return self.pipeline_config.structure

    def create_renderable(self, **config: Any) -> RenderableType:
        """Create a printable overview of this operations details.

        Available render_config options:
          - 'include_full_doc' (default: True): whether to include the full documentation, or just a description
          - 'include_src' (default: False): whether to include the module source code
        """

        include_full_doc = config.get("include_full_doc", True)
        include_src = config.get("include_src", False)
        include_inputs = config.get("include_inputs", True)
        include_outputs = config.get("include_outputs", True)
        include_module_details = config.get("include_module_details", False)

        table = Table(box=box.SIMPLE, show_header=False, show_lines=True)
        table.add_column("Property", style="i")
        table.add_column("Value")

        if self.doc:
            if include_full_doc:
                table.add_row("Documentation", self.doc.full_doc)
            else:
                table.add_row("Description", self.doc.description)

        # module_type_md = self.module.get_type_metadata()

        if include_inputs:
            inputs_table = create_table_from_field_schemas(
                _add_required=True,
                _add_default=True,
                _show_header=True,
                _constants=None,
                fields=self.operation_details.inputs_schema,
            )
            table.add_row("Inputs", inputs_table)
        if include_outputs:
            outputs_table = create_table_from_field_schemas(
                _add_required=False,
                _add_default=False,
                _show_header=True,
                _constants=None,
                fields=self.operation_details.outputs_schema,
            )
            table.add_row("Outputs", outputs_table)

        if include_module_details:
            table.add_row("Module type", self.module_type)

            module_config = self.module.config.json(option=orjson.OPT_INDENT_2)
            conf = Syntax(
                module_config,
                "json",
                background_color="default",
            )
            table.add_row("Module config", conf)

            from kiara.interfaces.python_api import ModuleTypeInfo

            module_type_md = ModuleTypeInfo.create_from_type_class(
                type_cls=self.module_details.get_class(),  # type: ignore
                kiara=None,  # type: ignore
            )

            desc = module_type_md.documentation.description
            module_md = module_type_md.create_renderable(
                include_doc=False, include_src=False, include_config_schema=False
            )
            m_md = Group(desc, module_md)
            table.add_row("Module metadata", m_md)

        if include_src:
            table.add_row("Source code", module_type_md.process_src)

        return table


# class OperationTypeInfo(TypeInfo):
#
#     _kiara_model_id = "info.operation_type"
#
#     @classmethod
#     def create_from_type_class(
#         cls, kiara: "Kiara", type_cls: Type["OperationType"]
#     ) -> "OperationTypeInfo":
#
#         authors_md = AuthorsMetadataModel.from_class(type_cls)
#         doc = DocumentationMetadataModel.from_class_doc(type_cls)
#         python_class = PythonClass.from_class(type_cls)
#         properties_md = ContextMetadataModel.from_class(type_cls)
#
#         return OperationTypeInfo.construct(
#             **{
#                 "type_name": type_cls._operation_type_name,  # type: ignore
#                 "documentation": doc,
#                 "authors": authors_md,
#                 "context": properties_md,
#                 "python_class": python_class,
#             }
#         )
#
#     @classmethod
#     def base_class(self) -> Type["OperationType"]:
#         from kiara.operations import OperationType
#
#         return OperationType
#
#     @classmethod
#     def category_name(cls) -> str:
#         return "operation_type"
#
#     def _retrieve_id(self) -> str:
#         return self.type_name
#
#     def _retrieve_data_to_hash(self) -> Any:
#         return self.type_name
#
#
# class OperationTypeClassesInfo(TypeInfoItemGroup):
#
#     _kiara_model_id = "info.operation_types"
#
#     @classmethod
#     def base_info_class(cls) -> Type[TypeInfo]:
#         return OperationTypeInfo
#
#     type_name: Literal["operation_type"] = "operation_type"
#     item_infos: Mapping[str, OperationTypeInfo] = Field(
#         description="The operation info instances for each type."
#     )


# class OperationInfo(ItemInfo):
#
#     _kiara_model_id = "info.operation"
#
#     @classmethod
#     def create_from_operation(cls, kiara: "Kiara", operation: Operation):
#
#         module = operation.module
#         module_cls = module.__class__
#
#         authors_md = AuthorsMetadataModel.from_class(module_cls)
#         properties_md = ContextMetadataModel.from_class(module_cls)
#
#         op_types = kiara.operation_registry.find_all_operation_types(
#             operation_id=operation.operation_id
#         )
#
#         op_info = OperationInfo.construct(
#             type_name=operation.operation_id,
#             operation_types=list(op_types),
#             operation=operation,
#             documentation=operation.doc,
#             authors=authors_md,
#             context=properties_md,
#         )
#
#         return op_info
#
#     @classmethod
#     def category_name(cls) -> str:
#         return "operation"
#
#     operation: Operation = Field(description="The operation instance.")
#     operation_types: List[str] = Field(
#         description="The operation types this operation belongs to."
#     )
#
#     def create_renderable(self, **config: Any) -> RenderableType:
#
#         include_doc = config.get("include_doc", True)
#
#         table = Table(box=box.SIMPLE, show_header=False, padding=(0, 0, 0, 0))
#         table.add_column("property", style="i")
#         table.add_column("value")
#
#         if include_doc:
#             table.add_row(
#                 "Documentation",
#                 Panel(self.documentation.create_renderable(), box=box.SIMPLE),
#             )
#         table.add_row("Author(s)", self.authors.create_renderable(**config))
#         table.add_row("Context", self.context.create_renderable(**config))
#
#         table.add_row("Operation details", self.operation.create_renderable(**config))
#         return table
#
#
# class OperationGroupInfo(InfoModelGroup):
#
#     _kiara_model_id = "info.operations"
#
#     @classmethod
#     def base_info_class(cls) -> Type[ItemInfo]:
#         return OperationInfo
#
#     @classmethod
#     def create_from_operations(
#         cls, kiara: "Kiara", group_alias: Union[str, None] = None, **items: Operation
#     ) -> "OperationGroupInfo":
#
#         op_infos = {
#             k: OperationInfo.create_from_operation(kiara=kiara, operation=v)
#             for k, v in items.items()
#         }
#         op_group_info = cls.construct(group_alias=group_alias, item_infos=op_infos)
#         return op_group_info
#
#     # type_name: Literal["operation_type"] = "operation_type"
#     item_infos: Mapping[str, OperationInfo] = Field(
#         description="The operation info instances for each type."
#     )
#
#     def create_renderable(self, **config: Any) -> RenderableType:
#
#         by_type = config.get("by_type", False)
#
#         if by_type:
#             return self._create_renderable_by_type(**config)
#         else:
#             return self._create_renderable_list(**config)
#
#     def _create_renderable_list(self, **config):
#
#         include_internal_operations = config.get("include_internal_operations", True)
#         full_doc = config.get("full_doc", False)
#         filter = config.get("filter", [])
#
#         table = Table(box=box.SIMPLE, show_header=True)
#         table.add_column("Id", no_wrap=True, style="i")
#         table.add_column("Type(s)", style="green")
#         table.add_column("Description")
#
#         for op_id, op_info in self.item_infos.items():
#
#             if (
#                 not include_internal_operations
#                 and op_info.operation.operation_details.is_internal_operation
#             ):
#                 continue
#
#             types = op_info.operation_types
#
#             if "custom_module" in types:
#                 types.remove("custom_module")
#
#             desc_str = op_info.documentation.description
#             if full_doc:
#                 desc = Markdown(op_info.documentation.full_doc)
#             else:
#                 desc = Markdown(op_info.documentation.description)
#
#             if filter:
#                 match = True
#                 for f in filter:
#                     if (
#                         f.lower() not in op_id.lower()
#                         and f.lower() not in desc_str.lower()
#                     ):
#                         match = False
#                         break
#                 if match:
#                     table.add_row(op_id, ", ".join(types), desc)
#
#             else:
#                 table.add_row(op_id, ", ".join(types), desc)
#
#         return table
#
#     def _create_renderable_by_type(self, **config):
#
#         include_internal_operations = config.get("include_internal_operations", True)
#         full_doc = config.get("full_doc", False)
#         filter = config.get("filter", [])
#
#         by_type = {}
#         for op_id, op in self.item_infos.items():
#             if filter:
#                 match = True
#                 for f in filter:
#                     if (
#                         f.lower() not in op_id.lower()
#                         and f.lower() not in op.documentation.description.lower()
#                     ):
#                         match = False
#                         break
#                 if not match:
#                     continue
#             for op_type in op.operation_types:
#                 by_type.setdefault(op_type, {})[op_id] = op
#
#         table = Table(box=box.SIMPLE, show_header=True)
#         table.add_column("Type", no_wrap=True, style="b green")
#         table.add_column("Id", no_wrap=True, style="i")
#         if full_doc:
#             table.add_column("Documentation", no_wrap=False, style="i")
#         else:
#             table.add_column("Description", no_wrap=False, style="i")
#
#         for operation_name in sorted(by_type.keys()):
#
#             # if operation_name == "custom_module":
#             #     continue
#
#             first_line_value = True
#             op_infos = by_type[operation_name]
#
#             for op_id in sorted(op_infos.keys()):
#                 op_info: OperationInfo = op_infos[op_id]
#
#                 if (
#                     not include_internal_operations
#                     and op_info.operation.operation_details.is_internal_operation
#                 ):
#                     continue
#
#                 if full_doc:
#                     desc = Markdown(op_info.documentation.full_doc)
#                 else:
#                     desc = Markdown(op_info.documentation.description)
#
#                 row = []
#                 if first_line_value:
#                     row.append(operation_name)
#                 else:
#                     row.append("")
#
#                 row.append(op_id)
#                 row.append(desc)
#
#                 table.add_row(*row)
#                 first_line_value = False
#
#         return table


class Filter(KiaraModel):

    operation: Operation = Field(
        description="The underlying operation providing which does the filtering."
    )
    input_name: str = Field(
        description="The input name to use for the dataset to filter."
    )
    output_name: str = Field(
        description="The output name to use for the dataset to filter."
    )
    data_type: str = Field(description="The type of the dataset that gets filtered.")
