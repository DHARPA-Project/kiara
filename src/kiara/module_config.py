# -*- coding: utf-8 -*-

"""Configuration models for the *Kiara* package."""

import collections
import deepdiff
import typing
from pathlib import Path
from pydantic import BaseModel, Extra, Field, PrivateAttr, validator
from rich import box
from rich.console import Console, ConsoleOptions, RenderResult
from rich.table import Table

from kiara.data.values import StepValueAddress
from kiara.defaults import DEFAULT_PIPELINE_PARENT_ID
from kiara.utils import get_data_from_file

if typing.TYPE_CHECKING:
    from kiara import Kiara, PipelineController, PipelineStructure


def create_step_value_address(
    value_address_config: typing.Union[str, typing.Mapping[str, typing.Any]],
    field_name: str,
) -> StepValueAddress:

    sub_value: typing.Optional[typing.Mapping[str, typing.Any]] = None
    if isinstance(value_address_config, str):

        tokens = value_address_config.split(".")
        if len(tokens) == 1:
            step_id = value_address_config
            output_name = field_name
        elif len(tokens) == 2:
            step_id = tokens[0]
            output_name = tokens[1]
        elif len(tokens) == 3:
            step_id = tokens[0]
            output_name = tokens[1]
            sub_value = {"config": tokens[2]}
        else:
            raise NotImplementedError()

    elif isinstance(value_address_config, collections.abc.Mapping):
        step_id = value_address_config["step_id"]
        output_name = value_address_config["output_name"]
        sub_value = value_address_config.get("sub_value", None)

    if sub_value is not None and not isinstance(sub_value, typing.Mapping):
        raise ValueError(
            f"Invalid type '{type(sub_value)}' for sub_value (step_id: {step_id}, value name: {output_name}): {sub_value}"
        )

    input_link = StepValueAddress(
        step_id=step_id, value_name=output_name, sub_value=sub_value
    )
    return input_link


class PipelineStepConfig(BaseModel):
    """A class to hold the configuration of one module within a [PipelineModule][kiara.pipeline.module.PipelineModule]."""

    class Config:
        extra = Extra.forbid
        validate_assignment = True

    module_type: str = Field(description="The name of the module type.")
    step_id: str = Field(description="The id of the step.")
    module_config: typing.Dict = Field(
        default_factory=dict,
        description="The configuration for the module (module-type specific).",
    )
    input_links: typing.Dict[str, typing.List[StepValueAddress]] = Field(
        default_factory=dict,
        description="The map with the name of an input link as key, and the connected module output name(s) as value.",
    )

    @validator("input_links", pre=True)
    def ensure_input_links_valid(cls, v):

        result = {}
        for input_name, output in v.items():

            if isinstance(output, (str, typing.Mapping)):
                input_links = [
                    create_step_value_address(
                        value_address_config=output, field_name=input_name
                    )
                ]

            elif isinstance(output, collections.abc.Sequence):
                input_links = []
                for o in output:
                    il = create_step_value_address(
                        value_address_config=o, field_name=input_name
                    )
                    input_links.append(il)
            else:
                raise TypeError(
                    f"Can't parse input map, invalid type for output: {output}"
                )

            result[input_name] = input_links

        return result


class KiaraModuleConfig(BaseModel):
    """Base class that describes the configuration a [KiaraModule][kiara.module.KiaraModule] class accepts.

    This is stored in the ``_config_cls`` class attribute in each ``KiaraModule`` class. By default,
    such a ``KiaraModule`` is not configurable.

    """

    _config_hash: str = PrivateAttr(default=None)
    constants: typing.Dict[str, typing.Any] = Field(
        default_factory=dict, description="Value constants for this module."
    )

    class Config:
        extra = Extra.forbid
        validate_assignment = True

    def get(self, key: str) -> typing.Any:

        if key not in self.__fields__:
            raise Exception(
                f"No config value '{key}' in module config class '{self.__class__.__name__}'."
            )

        return getattr(self, key)

    @property
    def config_hash(self):

        if self._config_hash is None:
            _d = self.dict()
            hashes = deepdiff.DeepHash(_d)
            self._config_hash = hashes[_d]
        return self._config_hash

    def __eq__(self, other):

        if self.__class__ != other.__class__:
            return False

        return self.dict() == other.dict()

    def __hash__(self):

        return hash(self.config_hash)

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        my_table = Table(box=box.MINIMAL, show_header=False)
        my_table.add_column("Field name", style="i")
        my_table.add_column("Value")
        for field in self.__fields__:
            my_table.add_row(field, getattr(self, field))

        yield my_table


KIARA_CONFIG = typing.TypeVar("KIARA_CONFIG", bound=KiaraModuleConfig)


class PipelineModuleConfig(KiaraModuleConfig):
    """A class to hold the configuration for a [PipelineModule][kiara.pipeline.module.PipelineModule].

    If you want to control the pipeline input and output names, you need to have to provide a map that uses the
    autogenerated field name ([step_id]__[field_name] -- 2 underscores!!) as key, and the desired field name
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
        nand_p_conf = PipelineModuleConfig(doc="Returns 'False' if both inputs are 'True'.",
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

    class Config:
        extra = Extra.allow
        validate_assignment = True

    steps: typing.List[PipelineStepConfig] = Field(
        default_factory=list,
        description="A list of steps/modules of this pipeline, and their connections.",
    )
    input_aliases: typing.Union[str, typing.Dict[str, str]] = Field(
        default_factory=dict,
        description="A map of input aliases, with the calculated (<step_id>__<input_name> -- double underscore!) name as key, and a string (the resulting workflow input alias) as value. Check the documentation for the config class for which marker strings can be used to automatically create this map if possible.",
    )
    output_aliases: typing.Union[str, typing.Dict[str, str]] = Field(
        default_factory=dict,
        description="A map of output aliases, with the calculated (<step_id>__<output_name> -- double underscore!) name as key, and a string (the resulting workflow output alias) as value.  Check the documentation for the config class for which marker strings can be used to automatically create this map if possible.",
    )
    doc: str = Field(
        default="-- n/a --", description="Documentation about what the pipeline does."
    )

    meta: typing.Dict[str, typing.Any] = Field(
        default_factory=dict, description="Metadata for this workflow."
    )

    def create_structure(
        self, parent_id: str, kiara: typing.Optional["Kiara"] = None
    ) -> "PipelineStructure":
        from kiara import Kiara, PipelineStructure

        if kiara is None:
            kiara = Kiara.instance()

        ps = PipelineStructure(
            parent_id=parent_id,
            steps=self.steps,
            input_aliases=self.input_aliases,
            output_aliases=self.output_aliases,
            kiara=kiara,
        )
        return ps

    def create_pipeline(
        self,
        parent_id: typing.Optional[str] = None,
        constants: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        controller: typing.Optional["PipelineController"] = None,
        kiara: typing.Optional["Kiara"] = None,
    ):

        if parent_id is None:
            parent_id = DEFAULT_PIPELINE_PARENT_ID
        structure = self.create_structure(parent_id=parent_id, kiara=kiara)

        from kiara import Pipeline

        pipeline = Pipeline(
            structure=structure,
            constants=constants,
            controller=controller,
        )
        return pipeline


class KiaraWorkflowConfig(BaseModel):
    """The object to hold a configuration for a workflow."""

    class Config:
        extra = Extra.forbid
        validate_assignment = True

    @classmethod
    def from_file(cls, path: typing.Union[str, Path]):

        data = get_data_from_file(path)
        return KiaraWorkflowConfig(module_type="pipeline", module_config=data)

    module_type: str = Field(
        description="The name of the 'root' module of this workflow.",
        default="pipeline",
    )
    module_config: typing.Dict[str, typing.Any] = Field(
        default_factory=dict,
        description="The configuration for the 'root' module of this workflow.",
    )
