# -*- coding: utf-8 -*-
import copy
import os
from enum import Enum
from pydantic import Extra, Field, PrivateAttr, root_validator, validator
from slugify import slugify
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Union

from kiara.defaults import (
    PIPELINE_CONFIG_TYPE_CATEGORY_ID,
    PIPELINE_STEP_TYPE_CATEGORY_ID,
)
from kiara.models.module import KiaraModuleClass, KiaraModuleConfig
from kiara.models.module.manifest import Manifest
from kiara.models.module.pipeline.value_refs import StepValueAddress
from kiara.utils import get_data_from_file
from kiara.utils.pipelines import ensure_step_value_addresses

if TYPE_CHECKING:
    from kiara.kiara import Kiara
    from kiara.models.module.pipeline.structure import PipelineStructure
    from kiara.modules import KiaraModule


class StepStatus(Enum):
    """Enum to describe the state of a workflow."""

    INPUTS_INVALID = "inputs_invalid"
    INPUTS_READY = "inputs_ready"
    RESULTS_READY = "results_ready"


class PipelineStep(Manifest):
    """A step within a pipeline-structure, includes information about it's connection(s) and other metadata."""

    class Config:
        validate_assignment = True
        extra = Extra.forbid

    @classmethod
    def create_steps(
        cls,
        *steps: Mapping[str, Any],
        kiara: "Kiara",
        module_map: Optional[Mapping[str, Any]] = None,
    ) -> List["PipelineStep"]:

        if module_map is None:
            module_map = {}

        result: List[PipelineStep] = []

        for step in steps:

            module_type = step.get("module_type", None)
            if not module_type:
                raise ValueError("Can't create step, no 'module_type' specified.")

            module_config = step.get("module_config", {})

            if module_type not in kiara.module_type_names:
                module_type_resolved = module_map.get(module_type, None)
                if module_type_resolved is None:
                    raise Exception(f"Can't resolve module type name: {module_type}")

                module_config_resolved = dict(
                    module_type_resolved.get("module_config", {})
                )
                module_config_resolved.update(module_config)
                module_type = module_type_resolved["module_type"]
            else:
                module_config_resolved = module_config

            manifest = Manifest.construct(
                module_type=module_type, module_config=module_config_resolved
            )
            module = kiara.create_module(manifest=manifest)

            step_id = step.get("step_id", None)
            if not step_id:
                raise ValueError("Can't create step, no 'step_id' specified.")

            input_links = {}
            for input_field, sources in step.get("input_links", {}).items():
                if isinstance(sources, str):
                    sources = [sources]
                    input_links[input_field] = sources

            # TODO: do we really need the deepcopy here?
            _s = PipelineStep(
                step_id=step_id,
                module_type=module_type,
                module_config=copy.deepcopy(module_config_resolved),
                input_links=input_links,  # type: ignore
                module_details=KiaraModuleClass.from_module(module=module),
            )
            _s._module = module
            result.append(_s)

        return result

    @validator("step_id")
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
    # required: bool = Field(
    #     description="Whether this step is required within the workflow.\n\nIn some cases, when none of the pipeline outputs have a required input that connects to a step, then it is not necessary for this step to have been executed, even if it is placed before a step in the execution hierarchy. This also means that the pipeline inputs that are connected to this step might not be required.",
    #     default=True,
    # )
    # processing_stage: Optional[int] = Field(
    #     default=None,
    #     description="The stage number this step is executed within the pipeline.",
    # )
    input_links: Mapping[str, List[StepValueAddress]] = Field(
        description="The links that connect to inputs of the module.",
        default_factory=list,
    )
    module_details: KiaraModuleClass = Field(
        description="The class of the underlying module."
    )
    _module: Optional["KiaraModule"] = PrivateAttr(default=None)

    def _retrieve_data_to_hash(self) -> Any:
        return self.dict()

    def _retrieve_id(self) -> str:
        return str(self.model_data_hash)

    def _retrieve_category_id(self) -> str:
        return PIPELINE_STEP_TYPE_CATEGORY_ID

    @root_validator(pre=True)
    def create_step_id(cls, values):

        if "module_type" not in values:
            raise ValueError("No 'module_type' specified.")
        if "step_id" not in values or not values["step_id"]:
            values["step_id"] = slugify(values["module_type"])

        return values

    @validator("step_id")
    def ensure_valid_id(cls, v):

        # TODO: check with regex
        if "." in v or " " in v:
            raise ValueError(
                f"Step id can't contain special characters or whitespaces: {v}"
            )

        return v

    @validator("module_config", pre=True)
    def ensure_dict(cls, v):

        if v is None:
            v = {}
        return v

    @validator("input_links", pre=True)
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


class PipelineConfig(KiaraModuleConfig):
    """A class to hold the configuration for a [PipelineModule][kiara.pipeline.module.PipelineModule].

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

        Configuration for a pipeline module that functions as a ``nand`` logic gate (in Python):

        ``` python
        and_step = PipelineStepConfig(module_type="and", step_id="and")
        not_step = PipelineStepConfig(module_type="not", step_id="not", input_links={"a": ["and.y"]}
        nand_p_conf = PipelineConfig(doc="Returns 'False' if both inputs are 'True'.",
                            steps=[and_step, not_step],
                            input_aliases={
                                "and__a": "a",
                                "and__b": "b"
                            },
                            output_aliases={
                                "not__y": "y"
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
            "and__a": "a",
            "and__b": "b"
          },
          "output_aliases": {
            "not__y": "y"
          }
        }
        ```
    """

    @classmethod
    def from_file(
        cls, path: str, kiara: "Kiara", module_map: Optional[Mapping[str, Any]] = None
    ):

        data = get_data_from_file(path)
        pipeline_id = data.pop("module_type_name", None)
        if pipeline_id is None:
            pipeline_id = os.path.basename(path)
        steps = data.pop("steps")
        steps = PipelineStep.create_steps(*steps, kiara=kiara, module_map=module_map)
        data["steps"] = steps
        return PipelineConfig(pipeline_id=pipeline_id, **data)

    @classmethod
    def from_config(
        cls,
        pipeline_id: str,
        data: Mapping[str, Any],
        kiara: "Kiara",
        module_map: Optional[Mapping[str, Any]] = None,
    ):

        data = dict(data)
        steps = data.pop("steps")
        steps = PipelineStep.create_steps(*steps, kiara=kiara, module_map=module_map)
        data["steps"] = steps
        return PipelineConfig(pipeline_id=pipeline_id, **data)

    class Config:
        extra = Extra.allow
        validate_assignment = True

    pipeline_id: str = Field(description="The name of this pipeline.")
    steps: List[PipelineStep] = Field(
        description="A list of steps/modules of this pipeline, and their connections.",
    )
    input_aliases: Union[str, Dict[str, str]] = Field(
        default_factory=dict,
        description="A map of input aliases, with the calculated (<step_id>__<input_name> -- double underscore!) name as key, and a string (the resulting workflow input alias) as value. Check the documentation for the config class for which marker strings can be used to automatically create this map if possible.",
    )
    output_aliases: Union[str, Dict[str, str]] = Field(
        default_factory=dict,
        description="A map of output aliases, with the calculated (<step_id>__<output_name> -- double underscore!) name as key, and a string (the resulting workflow output alias) as value.  Check the documentation for the config class for which marker strings can be used to automatically create this map if possible.",
    )
    documentation: str = Field(
        default="-- n/a --", description="Documentation about what the pipeline does."
    )

    context: Dict[str, Any] = Field(
        default_factory=dict, description="Metadata for this workflow."
    )
    _structure: Optional["PipelineStructure"] = PrivateAttr(default=None)

    def _retrieve_id(self) -> str:
        return str(self.model_data_hash)

    def _retrieve_category_id(self) -> str:
        return PIPELINE_CONFIG_TYPE_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return self.dict()

    @validator("steps", pre=True)
    def _validate_steps(cls, v):

        if not v:
            raise ValueError(f"Invalid type for 'steps' value: {type(v)}")

        steps = []
        for step in v:
            if not step:
                raise ValueError("No step data provided.")
            if isinstance(step, PipelineStep):
                steps.append(step)
            elif isinstance(step, Mapping):
                steps.append(PipelineStep(**step))
            else:
                raise TypeError(step)
        return steps

    @property
    def structure(self) -> "PipelineStructure":

        if self._structure is not None:
            return self._structure

        from kiara.models.module.pipeline.structure import PipelineStructure

        structure = PipelineStructure(pipeline_config=self)
        return structure

    def create_pipeline(
        self,
        controller: Optional["PipelineController"] = None,
        kiara: Optional["Kiara"] = None,
    ):

        # if parent_id is None:
        #     parent_id = DEFAULT_PIPELINE_PARENT_ID
        structure = self.create_pipeline_structure(kiara=kiara)

        raise NotImplementedError()
        # from kiara import Pipeline
        #
        # pipeline = Pipeline(
        #     structure=structure,
        #     controller=controller,
        # )
        # return pipeline

    def create_pipeline_module(
        self,
        module_id: Optional[str] = None,
        parent_id: Optional[str] = None,
        kiara: Optional["Kiara"] = None,
    ) -> "PipelineModule":

        if kiara is None:
            from kiara.kiara import Kiara

            kiara = Kiara.instance()

        raise NotImplementedError()
