# -*- coding: utf-8 -*-
from pydantic.config import Extra
from pydantic.fields import Field
from pydantic.main import BaseModel
from typing import Any, Dict, Iterable, List, Mapping, Optional, Union


class StepValueAddress(BaseModel):
    """Small model to describe the address of a value of a step, within a Pipeline/PipelineStructure."""

    class Config:
        extra = Extra.forbid

    step_id: str = Field(description="The id of a step within a pipeline.")
    value_name: str = Field(
        description="The name of the value (output name or pipeline input name)."
    )
    sub_value: Optional[Dict[str, Any]] = Field(
        default=None,
        description="A reference to a subitem of a value (e.g. column, list item)",
    )

    @property
    def alias(self):
        """An alias string for this address (in the form ``[step_id].[value_name]``)."""
        return generate_step_alias(self.step_id, self.value_name)

    def __eq__(self, other):

        if not isinstance(other, StepValueAddress):
            return False

        return (self.step_id, self.value_name, self.sub_value) == (
            other.step_id,
            other.value_name,
            other.sub_value,
        )

    def __hash__(self):

        return hash((self.step_id, self.value_name, self.sub_value))

    def __repr__(self):

        if self.sub_value:
            sub_value = f" sub_value={self.sub_value}"
        else:
            sub_value = ""
        return f"StepValueAddres(step_id={self.step_id}, value_name={self.value_name}{sub_value})"

    def __str__(self):
        return self.__repr__()


def generate_step_alias(step_id: str, value_name):
    return f"{step_id}.{value_name}"


def create_step_value_address(
    value_address_config: Union[str, Mapping[str, Any]],
    default_field_name: str,
) -> "StepValueAddress":

    if isinstance(value_address_config, StepValueAddress):
        return value_address_config

    sub_value: Optional[Mapping[str, Any]] = None

    if isinstance(value_address_config, str):

        tokens = value_address_config.split(".")
        if len(tokens) == 1:
            step_id = value_address_config
            output_name = default_field_name
        elif len(tokens) == 2:
            step_id = tokens[0]
            output_name = tokens[1]
        elif len(tokens) == 3:
            step_id = tokens[0]
            output_name = tokens[1]
            sub_value = {"config": tokens[2]}
        else:
            raise NotImplementedError()

    elif isinstance(value_address_config, Mapping):

        step_id = value_address_config["step_id"]
        output_name = value_address_config["output_name"]
        sub_value = value_address_config.get("sub_value", None)
    else:
        raise TypeError(
            f"Invalid type for creating step value address: {type(value_address_config)}"
        )

    if sub_value is not None and not isinstance(sub_value, Mapping):
        raise ValueError(
            f"Invalid type '{type(sub_value)}' for sub_value (step_id: {step_id}, value name: {output_name}): {sub_value}"
        )

    input_link = StepValueAddress(
        step_id=step_id, value_name=output_name, sub_value=sub_value
    )
    return input_link


def ensure_step_value_addresses(
    link: Union[str, Mapping, Iterable], default_field_name: str
) -> List["StepValueAddress"]:

    if isinstance(link, (str, Mapping)):
        input_links: List[StepValueAddress] = [
            create_step_value_address(
                value_address_config=link, default_field_name=default_field_name
            )
        ]

    elif isinstance(link, Iterable):
        input_links = []
        for o in link:
            il = create_step_value_address(
                value_address_config=o, default_field_name=default_field_name
            )
            input_links.append(il)
    else:
        raise TypeError(f"Can't parse input map, invalid type for output: {link}")

    return input_links
