# -*- coding: utf-8 -*-
import os
import uuid
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Iterable, List, Mapping, Union

import orjson
from boltons.strutils import slugify
from pydantic import ConfigDict, Field, PrivateAttr, field_validator, model_validator
from rich import box
from rich.console import RenderableType
from rich.markdown import Markdown
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from kiara.exceptions import InvalidPipelineStepConfig
from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.module import KiaraModuleConfig
from kiara.models.module.jobs import ExecutionContext
from kiara.models.module.manifest import Manifest
from kiara.models.module.pipeline.value_refs import (
    PipelineInputRef,
    PipelineOutputRef,
    StepValueAddress,
)
from kiara.models.python_class import KiaraModuleInstance
from kiara.utils import find_free_id, is_jupyter
from kiara.utils.data import get_data_from_string
from kiara.utils.files import get_data_from_file
from kiara.utils.json import orjson_dumps
from kiara.utils.modules import module_config_is_empty
from kiara.utils.output import create_table_from_field_schemas
from kiara.utils.pipelines import (
    ensure_step_value_addresses,
    extract_data_to_hash_from_pipeline_config,
)
from kiara.utils.string_vars import replace_var_names_in_obj

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.models.module.pipeline.pipeline import Pipeline
    from kiara.models.module.pipeline.structure import PipelineStructure
    from kiara.modules import KiaraModule


class StepStatus(Enum):
    """Enum to describe the state of a workflow."""

    INPUTS_INVALID = "inputs_invalid"
    INPUTS_READY = "inputs_ready"
    RESULTS_READY = "results_ready"

    def to_console_renderable(self) -> RenderableType:
        if self == StepStatus.INPUTS_INVALID:
            return "[red]inputs invalid[/red]"
        elif self == StepStatus.INPUTS_READY:
            return "[yellow]inputs ready[/yellow]"
        elif self == StepStatus.RESULTS_READY:
            return "[green]results ready[/green]"


class PipelineStep(Manifest):
    """A step within a pipeline-structure, includes information about it's connection(s) and other metadata."""

    _kiara_model_id: ClassVar = "instance.pipeline_step"
    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    @classmethod
    def create_step(
        cls,
        step: Union["PipelineStep", Mapping[str, Any]],
        kiara: "Kiara",
        module_map: Union[Mapping[str, Any], None] = None,
        auto_step_id: bool = False,
        taken_step_ids: Union[List[str], None] = None,
    ):
        """
        Create a step object from step data.

        Beware: the provided 'module_map' dictionary will be modified, as will the 'taken_step_ids'.
        """
        if module_map is None:
            module_map = {}

        if taken_step_ids is None:
            taken_step_ids = []

        if not isinstance(step, PipelineStep):

            module_type = step.get("module_type", None)

            if not module_type:
                raise InvalidPipelineStepConfig(
                    "Can't create step, no 'module_type' specified.", step_config=step
                )

            module_config = step.get("module_config", {})

            src_manifest = Manifest(
                module_type=module_type, module_config=module_config
            )

            if module_type not in kiara.module_type_names:

                if module_type in module_map.keys():

                    resolved_module_type = module_map[module_type]["module_type"]
                    resolved_module_config = module_map[module_type]["module_config"]

                    if module_config:
                        merged_module_config = dict(resolved_module_config)
                        merged_module_config.setdefault("defaults", {})
                        merged_module_config.setdefault("constants", {})
                        defaults = module_config.get("defaults", {})
                        constants = module_config.get("constants", {})
                        merged_module_config["defaults"].update(defaults)
                        merged_module_config["constants"].update(constants)
                    else:
                        merged_module_config = resolved_module_config

                    manifest = kiara.create_manifest(
                        module_or_operation=resolved_module_type,
                        config=merged_module_config,
                    )

                elif (
                    kiara.operation_registry.is_initialized
                    and module_type in kiara.operation_registry.operation_ids
                ):

                    op = kiara.operation_registry.operations[module_type]
                    resolved_module_type = op.module_type
                    resolved_module_config = op.module_config

                    if module_config:
                        merged_module_config = dict(resolved_module_config)
                        merged_module_config.setdefault("defaults", {})
                        merged_module_config.setdefault("constants", {})
                        defaults = module_config.get("defaults", {})
                        constants = module_config.get("constants", {})
                        merged_module_config["defaults"].update(defaults)
                        merged_module_config["constants"].update(constants)
                    else:
                        merged_module_config = resolved_module_config

                    manifest = kiara.create_manifest(
                        module_or_operation=resolved_module_type,
                        config=merged_module_config,
                    )
                else:
                    raise InvalidPipelineStepConfig(
                        f"Can't resolve module type: {module_type}", step_config=step
                    )
            else:
                manifest = kiara.create_manifest(
                    module_or_operation=module_type, config=module_config
                )
                resolved_module_type = module_type
                resolved_module_config = module_config

            module = kiara.module_registry.create_module(manifest=manifest)

            step_id = step.get("step_id", None)
            if not step_id:
                if not auto_step_id:
                    raise InvalidPipelineStepConfig(
                        "Can't create step, no 'step_id' specified in config.",
                        step_config=step,
                    )

                else:
                    step_id = find_free_id(
                        slugify(manifest.module_type, delim="_"),
                        current_ids=taken_step_ids,
                    )

            if step_id in taken_step_ids:
                raise ValueError(f"Can't create step: duplicate step id '{step_id}'.")

            taken_step_ids.append(step_id)

            input_links = {}
            for input_field, sources in step.get("input_links", {}).items():
                if isinstance(sources, str):
                    sources = [sources]
                input_links[input_field] = sources

            doc = step.get("doc", None)
            if doc is None:
                if module.module_type_name == "pipeline":
                    pc: PipelineConfig = module.config
                    doc = pc.doc
                else:
                    doc = module.doc

            # TODO: do we really need the deepcopy here?
            _s = PipelineStep(
                step_id=step_id,
                module_type=resolved_module_type,
                module_config=dict(resolved_module_config),
                input_links=input_links,  # type: ignore
                doc=doc,
                module_details=KiaraModuleInstance.from_module(module=module),
                manifest_src=src_manifest,
            )
            _s._module = module
        else:
            _s = step

        return _s

    @classmethod
    def create_steps(
        cls,
        *steps: Union["PipelineStep", Mapping[str, Any]],
        kiara: "Kiara",
        module_map: Union[Mapping[str, Any], None] = None,
        auto_step_ids: bool = False,
    ) -> List["PipelineStep"]:

        if module_map is None:
            module_map = {}
        else:
            module_map = dict(module_map)

        result: List[PipelineStep] = []

        step_ids: List[str] = []
        for step in steps:

            _s = cls.create_step(
                step=step,
                kiara=kiara,
                module_map=module_map,
                auto_step_id=auto_step_ids,
                taken_step_ids=step_ids,
            )
            result.append(_s)

        return result

    @field_validator("step_id")
    @classmethod
    def _validate_step_id(cls, v):

        assert isinstance(v, str)
        if "." in v:
            raise ValueError("Step ids can't contain '.' characters.")

        return v

    step_id: str = Field(
        description="Locally unique id (within a pipeline) of this step."
    )

    module_type: str = Field(description="The module type.")
    module_config: Dict[str, Any] = Field(
        description="The module config.", default_factory=dict
    )
    manifest_src: Manifest = Field(
        description="The original manfifest provided by the user."
    )

    # required: bool = Field(
    #     description="Whether this step is required within the workflow.\n\nIn some cases, when none of the pipeline outputs have a required input that connects to a step, then it is not necessary for this step to have been executed, even if it is placed before a step in the execution hierarchy. This also means that the pipeline inputs that are connected to this step might not be required.",
    #     default=True,
    # )
    # processing_stage: Optional[int] = Field(
    #     default=None,
    #     description="The stage number this step is executed within the pipeline.",
    # )
    input_links: Mapping[str, List[StepValueAddress]] = Field(
        description="The links that connect to inputs of the module. Keys are field names, value(s) are connected outputs.",
        default_factory=dict,
    )
    module_details: KiaraModuleInstance = Field(
        description="The class of the underlying module."
    )
    doc: DocumentationMetadataModel = Field(
        description="A description what this step does."
    )
    _module: Union["KiaraModule", None] = PrivateAttr(default=None)

    @model_validator(mode="before")
    @classmethod
    def create_step_id(cls, values):

        if "module_type" not in values:
            raise ValueError("No 'module_type' specified.")
        if "step_id" not in values or not values["step_id"]:
            values["step_id"] = slugify(values["module_type"], delim="_")

        return values

    def _retrieve_data_to_hash(self) -> Any:

        data = extract_data_to_hash_from_pipeline_config(self.module_config)
        return {
            "module_type": self.module_type,
            "module_config": data,
        }

    @field_validator("doc", mode="before")
    @classmethod
    def validate_doc(cls, value):
        doc = DocumentationMetadataModel.create(value)
        return doc

    @field_validator("step_id")
    @classmethod
    def ensure_valid_id(cls, v):

        # TODO: check with regex
        if "." in v or " " in v:
            raise ValueError(
                f"Step id can't contain special characters or whitespaces: {v}"
            )

        return v

    @field_validator("module_config", mode="before")
    @classmethod
    def ensure_dict(cls, v):

        if v is None:
            v = {}
        return v

    @field_validator("input_links", mode="before")
    @classmethod
    def ensure_input_links_valid(cls, v):

        if v is None:
            v = {}

        result = {}
        for input_name, output in v.items():

            input_links = ensure_step_value_addresses(
                default_field_name=input_name, link=output
            )
            result[input_name] = input_links

        return result

    @property
    def module(self) -> "KiaraModule":
        if self._module is None:
            m_cls = self.module_details.get_class()
            self._module = m_cls(module_config=self.module_config)
        return self._module

    def find_pipeline_inputs(
        self, pipeline: Union["Pipeline", "PipelineStructure"]
    ) -> Dict[str, PipelineInputRef]:
        """Return all the pipeline inputs that are connected to this step.

        Returns a dictionary with the name of the step input as key, and a reference to the pipeline input as value.
        """

        result = {}
        for field, field_ref in pipeline.pipeline_input_refs.items():
            for con_inp in field_ref.connected_inputs:
                if con_inp.step_id == self.step_id:
                    result[con_inp.value_name] = field_ref

        return result

    def find_pipeline_outputs(
        self, pipeline: Union["Pipeline", "PipelineStructure"]
    ) -> Dict[str, PipelineOutputRef]:

        result = {}
        for field, field_ref in pipeline.pipeline_output_refs.items():
            if field_ref.connected_output.step_id == self.step_id:
                result[field] = field_ref
        return result

    def __repr__(self):

        return f"{self.__class__.__name__}(step_id={self.step_id} module_type={self.module_type})"

    def __str__(self):
        return f"step: {self.step_id} (module: {self.module_type})"

    def create_renderable(self, **config: Any) -> RenderableType:

        in_panel = config.get("in_panel", None)
        display_step_id = config.get("display_step_id", True)
        if in_panel is None:
            if is_jupyter():
                in_panel = True
            else:
                in_panel = False

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("key", style="i")
        table.add_column("value")

        if self.doc.is_set:
            table.add_row("", Markdown(self.doc.full_doc))
        if display_step_id:
            table.add_row("step_id", self.step_id)
        table.add_row("module type", self.module_type)
        if not module_config_is_empty(self.module_config):
            mc = dict(self.module_config)
            if not mc.get("defaults", None):
                mc.pop("defaults", None)
            if not mc.get("constants", None):
                mc.pop("constants", None)

            if "steps" in mc.keys():
                _steps = []
                for step in mc["steps"]:
                    _s = {
                        "step_id": step["step_id"],
                        "module_type": step["manifest_src"]["module_type"],
                    }
                    sc = step.get("module_config", {})
                    if sc:
                        _s["module_config"] = sc
                    _steps.append(_s)
                mc["steps"] = _steps
            config_str = orjson_dumps(mc, option=orjson.OPT_INDENT_2)
            table.add_row(
                "module_config",
                Syntax(config_str, "json", background_color="default", theme="default"),
            )
        module_doc = DocumentationMetadataModel.from_class_doc(self.module.__class__)
        table.add_row("module doc", Markdown(module_doc.full_doc))
        inputs = create_table_from_field_schemas(
            _add_default=True,
            _add_required=True,
            _show_header=True,
            fields={
                f"{self.step_id}.{k}": v for k, v in self.module.inputs_schema.items()
            },
        )
        table.add_row("inputs", inputs)
        outputs = create_table_from_field_schemas(
            _add_default=False,
            _add_required=False,
            _show_header=True,
            fields={
                f"{self.step_id}.{k}": v for k, v in self.module.outputs_schema.items()
            },
        )
        table.add_row("outputs", outputs)

        if in_panel:
            return Panel(table, title=f"Step: {self.step_id}", title_align="left")
        else:
            return table


def create_input_alias_map(steps: Iterable[PipelineStep]) -> Dict[str, str]:

    aliases: Dict[str, str] = {}
    for step in steps:
        field_names = step.module.input_names
        for field_name in field_names:
            alias = generate_pipeline_endpoint_name(
                step_id=step.step_id, value_name=field_name
            )
            assert alias not in aliases.keys()
            aliases[f"{step.step_id}.{field_name}"] = alias

    return aliases


def create_output_alias_map(steps: Iterable[PipelineStep]) -> Dict[str, str]:

    aliases: Dict[str, str] = {}
    for step in steps:
        field_names = step.module.output_names
        for field_name in field_names:
            alias = generate_pipeline_endpoint_name(
                step_id=step.step_id, value_name=field_name
            )
            assert alias not in aliases.keys()
            aliases[f"{step.step_id}.{field_name}"] = alias

    return aliases


class PipelineConfig(KiaraModuleConfig):
    """
    A class to hold the configuration for a [PipelineModule][kiara.pipeline.module.PipelineModule].

    If you want to control the pipeline input and output names, you need to have to provide a map that uses the
    autogenerated field name ([step_id]__[alias] -- 2 underscores!!) as key, and the desired field name
    as value. The reason that schema for the autogenerated field names exist is that it's hard to ensure
    the uniqueness of each field; some steps can have the same input field names, but will need different input
    values. In some cases, some inputs of different steps need the same input. Those sorts of things.
    So, to make sure that we always use the right values, I chose to implement a conservative default approach,
    accepting that in some cases the user will be prompted for duplicate inputs for the same value.

    To remedy that, the pipeline creator has the option to manually specify a mapping to rename some or all of
    the input/output fields.

    Further, because in a lot of cases there won't be any overlapping fields, the creator can specify ``auto``,
    in which case *Kiara* will automatically create a mapping that tries to map autogenerated field names
    to the shortest possible names for each case.

    Examples:
    --------
        Configuration for a pipeline module that functions as a ``nand`` logic gate (in Python):

        ``` python
        and_step = PipelineStepConfig(module_type="and", step_id="and")
        not_step = PipelineStepConfig(module_type="not", step_id="not", input_links={"a": ["and.y"]}
        nand_p_conf = PipelineConfig(doc="Returns 'False' if both inputs are 'True'.",
                            steps=[and_step, not_step],
                            input_aliases={
                                "and.a": "a",
                                "and.b": "b"
                            },
                            output_aliases={
                                "not.y": "y"
                            }}
        ```

        Or, the same thing in json:

        ``` json
        {
          "module_type_name": "nand",
          "doc": "Returns 'False' if both inputs are 'True'.",
          "steps": [
            {
              "module_type": "and",
              "step_id": "and"
            },
            {
              "module_type": "not",
              "step_id": "not",
              "input_links": {
                "a": "and.y"
              }
            }
          ],
          "input_aliases": {
            "and.a": "a",
            "and.b": "b"
          },
          "output_aliases": {
            "not.y": "y"
          }
        }
        ```
    """

    _kiara_model_id: ClassVar = "instance.module_config.pipeline"

    @classmethod
    def from_file(
        cls,
        path: str,
        kiara: Union["Kiara", None] = None,
        pipeline_name: Union[None, str] = None,
        # module_map: Optional[Mapping[str, Any]] = None,
    ) -> "PipelineConfig":

        data = get_data_from_file(path)
        _pipeline_name = data.pop("pipeline_name", None)

        if pipeline_name:
            _pipeline_name = pipeline_name

        if _pipeline_name is None:
            _pipeline_name = os.path.basename(path)

        pipeline_dir = os.path.abspath(os.path.dirname(path))

        execution_context = ExecutionContext(pipeline_dir=pipeline_dir)
        return cls.from_config(
            pipeline_name=_pipeline_name,
            data=data,
            kiara=kiara,
            execution_context=execution_context,
        )

    @classmethod
    def from_string(
        cls,
        string_data: str,
        kiara: Union["Kiara", None] = None,
        pipeline_name: Union[None, str] = None,
        # module_map: Optional[Mapping[str, Any]] = None,
    ) -> "PipelineConfig":

        data = get_data_from_string(string_data)
        _pipeline_name = data.pop("pipeline_name", None)

        if pipeline_name:
            _pipeline_name = pipeline_name

        return cls.from_config(
            pipeline_name=_pipeline_name,
            data=data,
            kiara=kiara,
        )

    @classmethod
    def from_config(
        cls,
        data: Mapping[str, Any],
        pipeline_name: Union[str, None] = None,
        kiara: Union["Kiara", None] = None,
        module_map: Union[Mapping[str, Any], None] = None,
        execution_context: Union[ExecutionContext, None] = None,
        auto_step_ids: bool = False,
    ) -> "PipelineConfig":

        if kiara is None:
            from kiara.context import Kiara

            kiara = Kiara.instance()

        if not kiara.operation_registry.is_initialized:
            kiara.operation_registry.operations

        if execution_context is None:
            execution_context = ExecutionContext()

        config = cls._from_config(
            pipeline_name=pipeline_name,
            data=data,
            kiara=kiara,
            module_map=module_map,
            execution_context=execution_context,
            auto_step_ids=auto_step_ids,
        )
        return config

    @classmethod
    def _from_config(
        cls,
        data: Mapping[str, Any],
        kiara: "Kiara",
        pipeline_name: Union[str, None] = None,
        module_map: Union[Mapping[str, Any], None] = None,
        execution_context: Union[ExecutionContext, None] = None,
        auto_step_ids: bool = False,
    ) -> "PipelineConfig":

        if execution_context is None:
            execution_context = ExecutionContext()

        repl_dict = execution_context.model_dump()

        data = dict(data)

        _pipeline_name = data.pop("pipeline_name", None)
        if pipeline_name:
            _pipeline_name = pipeline_name

        if not _pipeline_name:
            _pipeline_name = str(uuid.uuid4())

        _steps = data.pop("steps")
        steps = PipelineStep.create_steps(
            *_steps, kiara=kiara, module_map=module_map, auto_step_ids=auto_step_ids
        )
        data["steps"] = steps
        if not data.get("input_aliases"):
            data["input_aliases"] = create_input_alias_map(steps)
        if not data.get("output_aliases"):
            data["output_aliases"] = create_output_alias_map(steps)

        if "defaults" in data.keys():
            defaults = data.pop("defaults")
            replaced = replace_var_names_in_obj(defaults, repl_dict=repl_dict)
            data["defaults"] = replaced

        if "constants" in data.keys():
            constants = data.pop("constants")
            replaced = replace_var_names_in_obj(constants, repl_dict=repl_dict)
            data["constants"] = replaced

        if "inputs" in data.keys():
            inputs = data.pop("inputs")
            replaced = replace_var_names_in_obj(inputs, repl_dict=repl_dict)
            data["inputs"] = replaced

        if "doc" not in data.keys():
            data["doc"] = None

        result = cls(pipeline_name=_pipeline_name, **data)
        return result

    model_config = ConfigDict(extra="ignore", validate_assignment=True)

    pipeline_name: str = Field(description="The name of this pipeline.")
    steps: List[PipelineStep] = Field(
        description="A list of steps/modules of this pipeline, and their connections.",
    )
    input_aliases: Dict[str, str] = Field(
        description="A map of input aliases, with the location of the input (in the format '[step_id].[input_field]') as key, and the pipeline input field name as value.",
    )
    output_aliases: Dict[str, str] = Field(
        description="A map of output aliases, with the location of the output (in the format '[step_id].[output_field]') as key, and the pipeline output field name as value.",
    )
    doc: DocumentationMetadataModel = Field(
        default="-- n/a --", description="Documentation about what the pipeline does."  # type: ignore
    )
    context: Dict[str, Any] = Field(
        default_factory=dict, description="Metadata for this workflow."
    )
    _structure: Union["PipelineStructure", None] = PrivateAttr(default=None)

    @field_validator("doc", mode="before")
    @classmethod
    def validate_doc(cls, value):
        return DocumentationMetadataModel.create(value)

    @field_validator("steps", mode="before")
    @classmethod
    def _validate_steps(cls, v):

        steps = []
        for step in v:
            if not step:
                raise ValueError("No step data provided.")
            if isinstance(step, PipelineStep):
                steps.append(step)
            elif isinstance(step, Mapping):
                steps.append(PipelineStep(**step))  # type: ignore
            else:
                raise TypeError(step)
        return steps

    @property
    def structure(self) -> "PipelineStructure":

        if self._structure is not None:
            return self._structure

        from kiara.models.module.pipeline.structure import PipelineStructure

        self._structure = PipelineStructure(pipeline_config=self)  # type: ignore
        return self._structure

    def get_raw_config(self) -> Dict[str, Any]:

        steps = []
        for step in self.steps:
            src: Dict[str, Any] = {
                "module_type": step.manifest_src.module_type,
            }
            if step.manifest_src.module_config:
                src["module_config"] = step.manifest_src.module_config
            src["step_id"] = step.step_id
            for field, links in step.input_links.items():
                for link in links:
                    src.setdefault("input_links", {})[
                        field
                    ] = f"{link.step_id}.{link.value_name}"
            steps.append(src)

        return {
            "pipeline_name": self.pipeline_name,
            "doc": self.doc.full_doc,
            "steps": steps,
            "input_aliases": self.input_aliases,
            "output_aliases": self.output_aliases,
        }

    def _retrieve_data_to_hash(self) -> Any:

        data = {
            "defaults": self.defaults,
            "constants": self.constants,
            "steps": [step.model_dump() for step in self.steps],
            "input_aliases": self.input_aliases,
            "output_aliases": self.output_aliases,
        }
        hash_data = extract_data_to_hash_from_pipeline_config(data)
        return hash_data

    def create_renderable(self, **config: Any) -> RenderableType:

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("key", style="i")
        table.add_column("value")

        table.add_row("doc", self.doc.full_doc)
        table.add_row("structure", self.structure.create_renderable(**config))
        return table

        # return create_table_from_model_object(self, exclude_fields={"steps"})

        # return create_table_from_model_object(self)

    # def create_input_alias_map(self) -> Dict[str, str]:
    #
    #     aliases: Dict[str, List[str]] = {}
    #     for step in self.steps:
    #         field_names = step.module.input_names
    #         for field_name in field_names:
    #             aliases.setdefault(field_name, []).append(step.step_id)
    #
    #     result: Dict[str, str] = {}
    #     for field_name, step_ids in aliases.items():
    #         for step_id in step_ids:
    #             generated = generate_pipeline_endpoint_name(step_id, field_name)
    #             result[generated] = generated
    #
    #     return result
    #
    # def create_output_alias_map(self) -> Dict[str, str]:
    #
    #     aliases: Dict[str, List[str]] = {}
    #     for step in self.steps:
    #         field_names = step.module.input_names
    #         for field_name in field_names:
    #             aliases.setdefault(field_name, []).append(step.step_id)
    #
    #     result: Dict[str, str] = {}
    #     for field_name, step_ids in aliases.items():
    #         for step_id in step_ids:
    #             generated = generate_pipeline_endpoint_name(step_id, field_name)
    #             result[generated] = generated
    #
    #     return result


def generate_pipeline_endpoint_name(step_id: str, value_name: str):

    return f"{step_id}__{value_name}"
