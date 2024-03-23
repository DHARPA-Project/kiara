# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Iterable, Mapping, Union

import structlog
from pydantic import Field, PrivateAttr, field_validator
from rich import box
from rich.console import Group, RenderableType
from rich.markdown import Markdown
from rich.syntax import Syntax
from rich.table import Table

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

try:
    from typing import Self  # type: ignore
except ImportError:
    from typing_extensions import Self  # type: ignore

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

    _kiara_model_id: ClassVar = "instance.operation_details"

    # inputs_map: Dict[str, str] = Field(description="A map with the operations input fields as keys, and the underlying modules input fields as values, used to translate input value maps.")
    # outputs_map: Dict[str, str] = Field(description="A map with the operations input fields as keys, and the underlying modules input fields as values, used to translate input value maps.")

    @classmethod
    def create_operation_details(cls, **details: Any) -> Self:

        if PYDANTIC_USE_CONSTRUCT:
            result = cls(**details)
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

    _kiara_model_id: ClassVar = "instance.operation_details.base"

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

    @field_validator("doc", mode="before")
    @classmethod
    def validate_doc(cls, value):
        return DocumentationMetadataModel.create(value)

    @abc.abstractmethod
    def retrieve_module_type(self, kiara: "Kiara") -> str:
        pass

    @abc.abstractmethod
    def retrieve_module_config(self, kiara: "Kiara") -> Mapping[str, Any]:
        pass


class ManifestOperationConfig(OperationConfig):

    _kiara_model_id: ClassVar = "instance.operation_config.manifest"

    module_type: str = Field(description="The module type.")
    module_config: Dict[str, Any] = Field(
        default_factory=dict, description="The configuration for the module."
    )

    _manifest_cache: Union[None, Manifest] = PrivateAttr(default=None)

    @field_validator("doc", mode="before")
    @classmethod
    def validate_doc(cls, value):
        return DocumentationMetadataModel.create(value)

    def retrieve_module_type(self, kiara: "Kiara") -> str:
        return self.module_type

    def retrieve_module_config(self, kiara: "Kiara") -> Mapping[str, Any]:
        return self.module_config

    def get_manifest(self) -> Manifest:

        if self._manifest_cache is None:
            self._manifest_cache = Manifest(
                module_type=self.module_type, module_config=self.module_config
            )
        return self._manifest_cache


class PipelineOperationConfig(OperationConfig):

    _kiara_model_id: ClassVar = "instance.operation_config.pipeline"

    pipeline_name: str = Field(description="The pipeline id.")
    pipeline_config: Mapping[str, Any] = Field(description="The pipeline config data.")
    module_map: Dict[str, Any] = Field(
        description="A lookup map to resolves operation ids to module names/configs.",
        default_factory=dict,
    )
    metadata: Mapping[str, Any] = Field(
        description="Additional metadata for the pipeline.", default_factory=dict
    )

    @field_validator("pipeline_config")
    @classmethod
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
        # ODO: pydantic refactoring -- maybe test that the dumped config is equivalent to the original one?
        result = pipeline_config.model_dump()

        return result

    @property
    def required_module_types(self) -> Iterable[str]:

        return [step["module_type"] for step in self.pipeline_config["steps"]]

    def __repr__(self):

        return f"{self.__class__.__name__}(pipeline_name={self.pipeline_name} required_modules={list(self.required_module_types)} instance_id={self.instance_id} fields=[{', '.join(self.model_fields.keys())}])"


class Operation(Manifest):

    _kiara_model_id: ClassVar = "instance.operation"

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
            module_config=module.config.model_dump(),
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
        """
        Create a printable overview of this operations details.

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
                doc = self.doc.full_doc
                title = "Documentation"
            else:
                doc = self.doc.description
                title = "Description"

            table.add_row(title, Markdown(doc))

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

        from kiara.interfaces.python_api.models.info import ModuleTypeInfo

        module_type_md: Union[ModuleTypeInfo, None] = None

        if include_module_details:
            table.add_row("Module type", self.module_type)

            module_config = self.module.config.model_dump_json(indent=2)
            conf = Syntax(
                module_config,
                "json",
                background_color="default",
            )
            table.add_row("Module config", conf)

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
            if module_type_md is None:
                module_type_md = ModuleTypeInfo.create_from_type_class(
                    type_cls=self.module_details.get_class(),  # type: ignore
                    kiara=None,  # type: ignore
                )

            table.add_row("Source code", module_type_md.module_src)

        return table


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
