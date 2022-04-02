# -*- coding: utf-8 -*-
import abc
import orjson
from pydantic import Field, PrivateAttr, validator
from rich import box
from rich.console import RenderableType, RenderGroup
from rich.syntax import Syntax
from rich.table import Table
from typing import TYPE_CHECKING, Any, Dict, Iterable, Mapping, Optional, Union

from kiara.defaults import (
    OPERATION_CATEOGORY_ID,
    OPERATION_CONFIG_CATEOGORY_ID,
    OPERATION_DETAILS_CATEOGORY_ID,
    PYDANTIC_USE_CONSTRUCT,
)
from kiara.models import KiaraModel
from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.module import KiaraModuleClass, KiaraModuleTypeMetadata
from kiara.models.module.jobs import JobConfig
from kiara.models.module.manifest import Manifest
from kiara.models.module.pipeline import PipelineConfig
from kiara.models.values.value import Value, ValueSet, ValueSetReadOnly
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import InputOutputObject, KiaraModule, ValueSetSchema
from kiara.utils.output import create_table_from_field_schemas

if TYPE_CHECKING:
    from kiara.kiara import Kiara


class OperationSchema(InputOutputObject):
    def __init__(
        self, alias: str, inputs_schema: ValueSetSchema, outputs_schema: ValueSetSchema
    ):

        allow_empty_inputs = True
        allow_empty_outputs = True

        self._inputs_schema_static: ValueSetSchema = inputs_schema
        self._outputs_schema_static: ValueSetSchema = outputs_schema
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

    def _retrieve_category_id(self) -> str:
        return OPERATION_DETAILS_CATEOGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return self.dict()

    @abc.abstractmethod
    def get_operation_schema(self) -> OperationSchema:
        pass

    @property
    def inputs_schema(self) -> Mapping[str, ValueSchema]:
        """The input schema for this module."""

        return self.get_operation_schema().inputs_schema

    @property
    def outputs_schema(self) -> Mapping[str, ValueSchema]:
        """The input schema for this module."""

        return self.get_operation_schema().outputs_schema

    @abc.abstractmethod
    def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
        pass

    @abc.abstractmethod
    def create_operation_outputs(self, outputs: ValueSet) -> Mapping[str, Value]:
        pass


class BaseOperationDetails(OperationDetails):

    _op_schema: OperationSchema = PrivateAttr(default=None)

    @classmethod
    @abc.abstractmethod
    def retrieve_inputs_schema(cls) -> ValueSetSchema:
        pass

    @classmethod
    @abc.abstractmethod
    def retrieve_outputs_schema(cls) -> ValueSetSchema:
        pass

    def get_operation_schema(self) -> OperationSchema:

        if self._op_schema is not None:
            return self._op_schema

        self._op_schema = OperationSchema(
            alias=self.__class__.__name__,
            inputs_schema=self.__class__.retrieve_inputs_schema(),
            outputs_schema=self.__class__.retrieve_outputs_schema(),
        )
        return self._op_schema


class OperationConfig(KiaraModel):

    doc: DocumentationMetadataModel = Field(
        description="Documentation for this operation."
    )

    @validator("doc", pre=True)
    def validate_doc(cls, value):
        return DocumentationMetadataModel.create(value)

    def _retrieve_id(self) -> str:
        return str(self.model_data_hash)

    def _retrieve_category_id(self) -> str:
        return OPERATION_CONFIG_CATEOGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return self.dict()

    @abc.abstractmethod
    def retrieve_module_type(self, kiara: "Kiara") -> str:
        pass

    @abc.abstractmethod
    def retrieve_module_config(self, kiara: "Kiara") -> Mapping[str, Any]:
        pass


class ManifestOperationConfig(OperationConfig):

    module_type: str = Field(description="The module type.")
    module_config: Dict[str, Any] = Field(
        default_factory=dict, description="The configuration for the module."
    )

    def retrieve_module_type(self, kiara: "Kiara") -> str:
        return self.module_type

    def retrieve_module_config(self, kiara: "Kiara") -> Mapping[str, Any]:
        return self.module_config


class PipelineOperationConfig(OperationConfig):

    pipeline_id: str = Field(description="The pipeline id.")
    pipeline_config: Mapping[str, Any] = Field(description="The pipeline config data.")
    module_map: Dict[str, Any] = Field(
        description="A lookup map to resolves module names to operations.",
        default_factory=dict,
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
        pipeline_config = PipelineConfig.from_config(
            pipeline_id=self.pipeline_id,
            data=self.pipeline_config,
            kiara=kiara,
            module_map=self.module_map,
        )
        return pipeline_config

    @property
    def required_module_types(self) -> Iterable[str]:

        return [step["module_type"] for step in self.pipeline_config["steps"]]

    def __repr__(self):

        return f"{self.__class__.__name__}(pipeline_id={self.pipeline_id} required_modules={list(self.required_module_types)} id={self.id}, category={self.category_id}, fields=[{', '.join(self.__fields__.keys())}])"


class Operation(Manifest):
    @classmethod
    def create_from_module(cls, module: KiaraModule) -> "Operation":

        from kiara.modules.operations.included_core_operations import (
            CustomModuleOperationDetails,
        )

        op_id = f"{module.module_type_name}._{module.module_instance_hash}"

        details = CustomModuleOperationDetails.create_from_module(module=module)
        operation = Operation(
            module_type=module.module_type_name,
            module_config=module.config,
            operation_id=op_id,
            operation_type="plain_module",
            operation_details=details,
            module_details=KiaraModuleClass.from_module(module),
            doc=DocumentationMetadataModel.from_class_doc(module.__class__),
        )
        operation._module = module
        return operation

    operation_id: str = Field(description="The (unique) id of this operation.")
    operation_type: str = Field(description="The type of this operation.")
    operation_details: OperationDetails = Field(
        description="The operation specific details of this operation."
    )
    doc: DocumentationMetadataModel = Field(
        description="Documentation for this operation."
    )

    module_details: KiaraModuleClass = Field(
        description="The class of the underlying module."
    )

    _module: Optional["KiaraModule"] = PrivateAttr(default=None)

    def _retrieve_id(self) -> str:
        return str(self.model_data_hash)

    def _retrieve_category_id(self) -> str:
        return OPERATION_CATEOGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        raise NotImplementedError()

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

    def prepare_job_config(self, kiara: "Kiara", inputs: Mapping[str, Any]) -> JobConfig:

        augmented_inputs = self.operation_details.get_operation_schema().augment_module_inputs(
            inputs=inputs
        )
        module_inputs = self.operation_details.create_module_inputs(
            inputs=augmented_inputs
        )

        job_config = kiara.job_registry.prepare_job_config(
            manifest=self, inputs=module_inputs
        )
        return job_config

    def run(self, kiara: "Kiara", inputs: Any) -> ValueSet:

        job_config = self.prepare_job_config(kiara=kiara, inputs=inputs)

        job_id = kiara.job_registry.execute_job(job_config=job_config)
        outputs: ValueSet = kiara.job_registry.retrieve_result(job_id=job_id)

        result = self.process_job_outputs(outputs=outputs)
        return result

    def process_job_outputs(self, outputs: ValueSet) -> ValueSet:

        op_outputs = self.operation_details.create_operation_outputs(outputs=outputs)

        value_set = ValueSetReadOnly(value_items=op_outputs, values_schema=self.outputs_schema)  # type: ignore
        return value_set

    # def run(self, _attach_lineage: bool = True, **inputs: Any) -> ValueSet:
    #
    #     return self.module.run(_attach_lineage=_attach_lineage, **inputs)

    def create_renderable(self, **config: Any) -> RenderableType:
        """Create a printable overview of this operations details.

        Available render_config options:
          - 'include_full_doc' (default: True): whether to include the full documentation, or just a description
          - 'include_src' (default: False): whether to include the module source code
        """

        include_full_doc = config.get("include_full_doc", True)

        table = Table(box=box.SIMPLE, show_header=False, show_lines=True)
        table.add_column("Property", style="i")
        table.add_column("Value")

        if self.doc:
            if include_full_doc:
                table.add_row("Documentation", self.doc.full_doc)
            else:
                table.add_row("Description", self.doc.description)

        # module_type_md = self.module.get_type_metadata()

        inputs_table = create_table_from_field_schemas(
            _add_required=True,
            _add_default=True,
            _show_header=True,
            **self.operation_details.inputs_schema,
        )
        # constants = self.module_config.get("constants")
        # inputs_table = create_table_from_field_schemas(
        #     _add_required=True,
        #     _add_default=True,
        #     _show_header=True,
        #     _constants=constants,
        #     **self.module.inputs_schema,
        # )
        table.add_row("Inputs", inputs_table)
        outputs_table = create_table_from_field_schemas(
            _add_required=False,
            _add_default=False,
            _show_header=True,
            **self.operation_details.outputs_schema,
        )
        # outputs_table = create_table_from_field_schemas(
        #     _add_required=False,
        #     _add_default=False,
        #     _show_header=True,
        #     _constants=None,
        #     **self.module.outputs_schema,
        # )
        table.add_row("Outputs", outputs_table)

        table.add_row("Module type", self.module_type)

        module_config = self.module.config.json(option=orjson.OPT_INDENT_2)
        conf = Syntax(
            module_config,
            "json",
            background_color="default",
        )
        table.add_row("Module config", conf)

        module_type_md = KiaraModuleTypeMetadata.from_module_class(
            self.module_details.get_class()
        )

        desc = module_type_md.documentation.description
        module_md = module_type_md.create_renderable(
            include_doc=False, include_src=False, include_config_schema=False
        )
        m_md = RenderGroup(desc, module_md)
        table.add_row("Module metadata", m_md)

        if config.get("include_src", True):
            table.add_row("Source code", module_type_md.process_src)

        return table