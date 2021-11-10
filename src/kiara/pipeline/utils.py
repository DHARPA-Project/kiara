# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import collections
import typing

if typing.TYPE_CHECKING:
    from kiara.pipeline import StepValueAddress
    from kiara.pipeline.config import PipelineConfig
    from kiara.pipeline.pipeline import Pipeline
    from kiara.pipeline.structure import PipelineStructure


def extend_pipeline(
    pipeline: typing.Union["Pipeline", "PipelineStructure"],
    other: typing.Union[
        "Pipeline",
        "PipelineStructure",
        "PipelineConfig",
        typing.Mapping[str, typing.Any],
    ],
    input_links: typing.Optional[
        typing.Mapping[str, typing.Iterable["StepValueAddress"]]
    ] = None,
):

    from kiara.pipeline.config import PipelineConfig
    from kiara.pipeline.pipeline import Pipeline
    from kiara.pipeline.structure import PipelineStructure

    if isinstance(pipeline, Pipeline):
        structure: PipelineStructure = pipeline.structure
    elif isinstance(pipeline, PipelineStructure):
        structure = pipeline
    else:
        raise TypeError(f"Invalid type '{type(pipeline)}' for pipeline.")

    other_pipeline_config: typing.Optional[PipelineConfig] = None

    if isinstance(other, typing.Mapping):
        other_pipeline_config = PipelineConfig(**other)
    elif isinstance(other, PipelineConfig):
        other_pipeline_config = other
    elif isinstance(other, PipelineStructure):
        other_pipeline_config = other.structure_config

    if other_pipeline_config is None:
        from kiara.pipeline.pipeline import Pipeline

        if isinstance(other, Pipeline):
            other_pipeline_config = other.structure.structure_config

    if other_pipeline_config is None:
        raise TypeError(
            f"Can't extend pipeline structure, invalid type: {type(other)}."
        )

    other_structure: PipelineStructure = PipelineStructure(
        config=other_pipeline_config, kiara=structure._kiara
    )

    step_id_overlap = [
        el for el in set(structure.step_ids) if el in other_structure.step_ids
    ]
    if step_id_overlap:
        raise Exception(
            f"Can't extend pipeline, duplicate step id(s) are not allowed: {', '.join(step_id_overlap)}."
        )

    if input_links is None:
        input_links = {}
    else:
        input_links = dict(input_links)

    full_input_links: typing.Dict[
        str, typing.Dict[str, typing.Iterable[StepValueAddress]]
    ] = {}
    for input_name, input in other_structure.pipeline_inputs.items():
        if input_name not in input_links.keys():
            # this means we try to see whether there is an output in this structure with the same name
            if input_name in structure.pipeline_outputs.keys():
                output_step_address: typing.Iterable[StepValueAddress] = [
                    structure.pipeline_outputs[input_name].connected_output
                ]
            else:
                # output_step_address = "NEW INPUT"
                raise NotImplementedError()
        else:
            output_step_address = input_links[input_name]

        connected_inputs = other_structure.pipeline_inputs[input_name].connected_inputs
        if len(connected_inputs) != 1:
            raise NotImplementedError()
        else:
            connected_input = connected_inputs[0]
            full_input_links.setdefault(connected_input.step_id, {})[
                connected_input.value_name
            ] = output_step_address

    new_input_aliases = dict(structure._input_aliases)
    if not other_structure._output_aliases:
        new_output_aliases: typing.Union[str, typing.Mapping[str, str]] = "auto"
    else:
        new_output_aliases = dict(other_structure._output_aliases)

    config = structure.structure_config.dict(
        exclude={"input_aliases", "output_aliases", "steps"}
    )
    config["input_aliases"] = new_input_aliases
    config["output_aliases"] = new_output_aliases

    new_steps = [
        step.dict(exclude={"parent_id", "processing_stage", "required"})
        for step in structure.steps
    ]

    for step in other_structure.steps:
        step_dict = step.dict(exclude={"parent_id", "processing_stage", "required"})
        if step.step_id in full_input_links.keys():
            if step.step_id not in full_input_links.keys():
                new_steps.append(step_dict)
            else:
                for input_name in full_input_links[step.step_id].keys():
                    if input_name in step_dict["input_links"].keys():
                        raise NotImplementedError()
                    step_dict["input_links"][input_name] = full_input_links[
                        step.step_id
                    ][input_name]
                new_steps.append(step_dict)

    config["steps"] = new_steps
    pmc = PipelineConfig(**config)
    new_structure = pmc.create_pipeline_structure(kiara=structure._kiara)
    return new_structure


def generate_step_alias(step_id: str, value_name):
    return f"{step_id}.{value_name}"


def create_step_value_address(
    value_address_config: typing.Union[str, typing.Mapping[str, typing.Any]],
    default_field_name: str,
) -> "StepValueAddress":

    from kiara.pipeline.values import StepValueAddress

    if isinstance(value_address_config, StepValueAddress):
        return value_address_config

    sub_value: typing.Optional[typing.Mapping[str, typing.Any]] = None

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

    elif isinstance(value_address_config, collections.abc.Mapping):
        print(value_address_config)
        step_id = value_address_config["step_id"]
        output_name = value_address_config["output_name"]
        sub_value = value_address_config.get("sub_value", None)
    else:
        raise TypeError(
            f"Invalid type for creating step value address: {type(value_address_config)}"
        )

    if sub_value is not None and not isinstance(sub_value, typing.Mapping):
        raise ValueError(
            f"Invalid type '{type(sub_value)}' for sub_value (step_id: {step_id}, value name: {output_name}): {sub_value}"
        )

    input_link = StepValueAddress(
        step_id=step_id, value_name=output_name, sub_value=sub_value
    )
    return input_link


def ensure_step_value_addresses(
    link: typing.Union[str, typing.Mapping, typing.Iterable], default_field_name: str
) -> typing.List["StepValueAddress"]:

    if isinstance(link, (str, typing.Mapping)):
        input_links: typing.List[StepValueAddress] = [
            create_step_value_address(
                value_address_config=link, default_field_name=default_field_name
            )
        ]

    elif isinstance(link, typing.Iterable):
        input_links = []
        for o in link:
            il = create_step_value_address(
                value_address_config=o, default_field_name=default_field_name
            )
            input_links.append(il)
    else:
        raise TypeError(f"Can't parse input map, invalid type for output: {link}")

    return input_links
