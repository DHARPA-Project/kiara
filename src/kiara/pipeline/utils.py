# -*- coding: utf-8 -*-
import typing

from kiara.data.values import StepValueAddress, generate_step_alias
from kiara.defaults import PIPELINE_PARENT_MARKER

if typing.TYPE_CHECKING:
    from kiara.module_config import PipelineModuleConfig
    from kiara.pipeline.pipeline import Pipeline
    from kiara.pipeline.structure import PipelineStructure, PipelineStructureDesc


def create_pipeline_structure_desc(
    pipeline: typing.Union["Pipeline", "PipelineStructure"]
) -> "PipelineStructureDesc":

    from kiara.pipeline.pipeline import Pipeline
    from kiara.pipeline.structure import (
        PipelineStructure,
        PipelineStructureDesc,
        StepDesc,
    )

    if isinstance(pipeline, Pipeline):
        structure: PipelineStructure = pipeline.structure
    elif isinstance(pipeline, PipelineStructure):
        structure = pipeline

    steps = {}
    workflow_inputs: typing.Dict[str, typing.List[str]] = {}
    workflow_outputs: typing.Dict[str, str] = {}

    for m_id, details in structure.steps_details.items():

        step = details["step"]

        input_connections: typing.Dict[str, typing.List[str]] = {}
        for k, v in details["inputs"].items():

            if v.connected_pipeline_input is not None:
                connected_item = v.connected_pipeline_input
                input_connections[k] = [
                    generate_step_alias(PIPELINE_PARENT_MARKER, connected_item)
                ]
                workflow_inputs.setdefault(f"{connected_item}", []).append(v.alias)
            elif v.connected_outputs is not None:
                assert len(v.connected_outputs) > 0
                for co in v.connected_outputs:
                    input_connections.setdefault(k, []).append(co.alias)
            else:
                raise TypeError(f"Invalid connection type: {type(connected_item)}")

        output_connections: typing.Dict[str, typing.Any] = {}
        for k, v in details["outputs"].items():
            for connected_item in v.connected_inputs:

                output_connections.setdefault(k, []).append(
                    generate_step_alias(
                        connected_item.step_id, connected_item.value_name
                    )
                )
            if v.pipeline_output:
                output_connections.setdefault(k, []).append(
                    generate_step_alias(PIPELINE_PARENT_MARKER, v.pipeline_output)
                )
                workflow_outputs[v.pipeline_output] = v.alias

        steps[step.step_id] = StepDesc(
            step=step,
            processing_stage=details["processing_stage"],
            input_connections=input_connections,
            output_connections=output_connections,
            required=step.required,
        )

    return PipelineStructureDesc(
        pipeline_id=structure._pipeline_id,
        steps=steps,
        processing_stages=structure.processing_stages,
        pipeline_input_connections=workflow_inputs,
        pipeline_output_connections=workflow_outputs,
        pipeline_inputs=structure.pipeline_inputs,
        pipeline_outputs=structure.pipeline_outputs,
    )


def extend_pipeline(
    pipeline: typing.Union["Pipeline", "PipelineStructure"],
    other: typing.Union[
        "Pipeline",
        "PipelineStructure",
        "PipelineModuleConfig",
        typing.Mapping[str, typing.Any],
    ],
    input_links: typing.Optional[
        typing.Mapping[str, typing.Iterable[StepValueAddress]]
    ] = None,
):

    from kiara.module_config import PipelineModuleConfig
    from kiara.pipeline.pipeline import Pipeline
    from kiara.pipeline.structure import PipelineStructure

    if isinstance(pipeline, Pipeline):
        structure: PipelineStructure = pipeline.structure
    elif isinstance(pipeline, PipelineStructure):
        structure = pipeline

    other_pipeline_config: typing.Optional[PipelineModuleConfig] = None

    other_name = "extended"

    if isinstance(other, typing.Mapping):
        other_pipeline_config = PipelineModuleConfig(**other)
    elif isinstance(other, PipelineModuleConfig):
        other_pipeline_config = other
    elif isinstance(other, PipelineStructure):
        other_pipeline_config = other.structure_config
        other_name = other.pipeline_id

    if other_pipeline_config is None:
        from kiara.pipeline.pipeline import Pipeline

        if isinstance(other, Pipeline):
            other_pipeline_config = other.structure.structure_config

    if other_pipeline_config is None:
        raise TypeError(
            f"Can't extend pipeline structure, invalid type: {type(other)}."
        )

    other_structure: PipelineStructure = PipelineStructure(
        parent_id="_", config=other_pipeline_config, kiara=structure._kiara
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
    pmc = PipelineModuleConfig(**config)
    new_structure = pmc.create_structure(
        f"{structure.pipeline_id}_{other_name}", kiara=structure._kiara
    )
    return new_structure
