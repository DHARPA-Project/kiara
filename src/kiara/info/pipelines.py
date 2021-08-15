# -*- coding: utf-8 -*-
import typing
from pydantic import BaseModel, Extra, Field, PrivateAttr
from rich import box
from rich.console import (
    Console,
    ConsoleOptions,
    RenderableType,
    RenderGroup,
    RenderResult,
)
from rich.markdown import Markdown
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from kiara.defaults import PIPELINE_PARENT_MARKER, SpecialValue
from kiara.info import KiaraInfoModel
from kiara.metadata.module_models import KiaraModuleTypeMetadata
from kiara.pipeline import PipelineValuesInfo, StepStatus
from kiara.pipeline.config import StepDesc
from kiara.pipeline.utils import generate_step_alias
from kiara.pipeline.values import PipelineInputField, PipelineOutputField, ValueField
from kiara.utils import StringYAML, create_table_from_config_class, print_ascii_graph

if typing.TYPE_CHECKING:
    from kiara import Kiara
    from kiara.pipeline.config import PipelineConfig
    from kiara.pipeline.pipeline import Pipeline
    from kiara.pipeline.structure import PipelineStructure

yaml = StringYAML()


class StepsInfo(KiaraInfoModel):

    pipeline_id: str = Field(description="The pipeline id.")
    steps: typing.Dict[str, StepDesc] = Field(description="A list of step details.")
    processing_stages: typing.List[typing.List[str]] = Field(
        description="The stages in which the steps are processed."
    )

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        table = Table(box=box.SIMPLE, show_lines=True)
        table.add_column("Stage", justify="center")
        table.add_column("Step Id")
        table.add_column("Module type", style="i")
        table.add_column("Description")

        for nr, stage in enumerate(self.processing_stages):

            for i, step_id in enumerate(stage):
                step: StepDesc = self.steps[step_id]
                if step.required:
                    title = f"[b]{step_id}[/b]"
                else:
                    title = f"[b]{step_id}[/b] [i](optional)[/i]"

                if hasattr(step.step.module, "instance_doc"):
                    doc = step.step.module.module_instance_doc
                else:
                    doc = step.step.module.get_type_metadata().model_doc()
                row: typing.List[typing.Any] = []
                if i == 0:
                    row.append(str(nr))
                else:
                    row.append("")
                row.append(title)

                # TODO; generate the right link here
                module_link = (
                    step.step.module.get_type_metadata().context.references.get(
                        "sources", None
                    )
                )
                if module_link:
                    module_str = f"[link={module_link}]{step.step.module_type}[/link]"
                else:
                    module_str = step.step.module_type
                row.append(module_str)
                if doc and doc != "-- n/a --":
                    m = Markdown(doc + "\n\n---\n")
                    row.append(m)
                else:
                    row.append("")
                table.add_row(*row)

        return table
        # yield Panel(table, title_align="left", title="Processing stages")

    # def __rich_console_old__(
    #     self, console: Console, options: ConsoleOptions
    # ) -> RenderResult:
    #
    #     explanation = {}
    #
    #     for nr, stage in enumerate(self.processing_stages):
    #
    #         stage_details = {}
    #         for step_id in stage:
    #             step: StepDesc = self.steps[step_id]
    #             if step.required:
    #                 title = step_id
    #             else:
    #                 title = f"{step_id} (optional)"
    #             stage_details[title] = step.step.module.get_type_metadata().model_doc()
    #
    #         explanation[nr + 1] = stage_details
    #
    #     lines = []
    #     for stage_nr, stage_steps in explanation.items():
    #         lines.append(f"[bold]Processing stage {stage_nr}[/bold]:")
    #         lines.append("")
    #         for step_id, desc in stage_steps.items():
    #             if desc == DEFAULT_NO_DESC_VALUE:
    #                 lines.append(f"  - {step_id}")
    #             else:
    #                 lines.append(f"  - {step_id}: [i]{desc}[/i]")
    #             lines.append("")
    #
    #     padding = (1, 2, 0, 2)
    #     yield Panel(
    #         "\n".join(lines),
    #         box=box.ROUNDED,
    #         title_align="left",
    #         title=f"Stages for pipeline: [b]{self.pipeline_id}[/b]",
    #         padding=padding,
    #     )


class PipelineStructureDesc(BaseModel):
    """Outlines the internal structure of a [Pipeline][kiara.pipeline.pipeline.Pipeline]."""

    @classmethod
    def create_pipeline_structure_desc(
        cls, pipeline: typing.Union["Pipeline", "PipelineStructure"]
    ) -> "PipelineStructureDesc":

        from kiara.pipeline.pipeline import Pipeline
        from kiara.pipeline.structure import PipelineStructure

        if isinstance(pipeline, Pipeline):
            structure: PipelineStructure = pipeline.structure
        elif isinstance(pipeline, PipelineStructure):
            structure = pipeline
        else:
            raise TypeError(f"Invalid type '{type(pipeline)}' for pipeline.")

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
                    raise TypeError(f"Invalid connection type: {v}")

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

    class Config:
        allow_mutation = False
        extra = Extra.forbid

    pipeline_id: str = Field(description="The (unique) pipeline id.")
    steps: typing.Dict[str, StepDesc] = Field(
        description="The steps contained in this pipeline, with the 'step_id' as key."
    )
    processing_stages: typing.List[typing.List[str]] = Field(
        description="The order in which this pipeline has to be processed (basically the dependencies of each step on other steps, if any)."
    )
    pipeline_input_connections: typing.Dict[str, typing.List[str]] = Field(
        description="The connections of this pipelines input fields. One input field can be connected to one or several step input fields."
    )
    pipeline_output_connections: typing.Dict[str, str] = Field(
        description="The connections of this pipelines output fields. Each pipeline output is connected to exactly one step output field."
    )
    pipeline_inputs: typing.Dict[str, PipelineInputField] = Field(
        description="The pipeline inputs."
    )
    pipeline_outputs: typing.Dict[str, PipelineOutputField] = Field(
        description="The pipeline outputs."
    )

    @property
    def steps_info(self) -> StepsInfo:

        return StepsInfo(
            pipeline_id=self.pipeline_id,
            processing_stages=self.processing_stages,
            steps=self.steps,
        )

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        yield f"[b]Pipeline structure: {self.pipeline_id}[/b]\n"

        yield "[b]Inputs / Outputs[/b]"

        data_panel: typing.List[typing.Any] = []
        inp_table = Table(show_header=True, box=box.SIMPLE, show_lines=True)
        inp_table.add_column("Name", style="i")
        inp_table.add_column("Type")
        inp_table.add_column("Description")
        inp_table.add_column("Required", justify="center")
        inp_table.add_column("Default", justify="center")

        for inp, details in self.pipeline_inputs.items():
            req = details.value_schema.is_required()
            if not req:
                req_str = "no"
            else:
                d = details.value_schema.default
                if d in [None, SpecialValue.NO_VALUE, SpecialValue.NOT_SET]:
                    req_str = "[b]yes[/b]"
                else:
                    req_str = "no"
            default = details.value_schema.default
            if default in [None, SpecialValue.NO_VALUE, SpecialValue.NOT_SET]:
                default = "-- no default --"
            else:
                default = str(default)
            inp_table.add_row(
                inp,
                details.value_schema.type,
                details.value_schema.doc,
                req_str,
                default,
            )

        p_inp = Panel(
            inp_table, box=box.ROUNDED, title="Input fields", title_align="left"
        )
        data_panel.append(p_inp)

        # yield "[b]Pipeline outputs[/b]"

        out_table = Table(show_header=True, box=box.SIMPLE, show_lines=True)
        out_table.add_column("Name", style="i")
        out_table.add_column("Type")
        out_table.add_column("Description")

        for inp, details_o in self.pipeline_outputs.items():

            out_table.add_row(
                inp,
                details_o.value_schema.type,
                details_o.value_schema.doc,
            )

        outp = Panel(
            out_table, box=box.ROUNDED, title="Output fields", title_align="left"
        )
        data_panel.append(outp)
        yield Panel(RenderGroup(*data_panel), box=box.SIMPLE)

        color_list = [
            "green",
            "blue",
            "bright_magenta",
            "dark_red",
            "gold3",
            "cyan",
            "orange1",
            "light_yellow3",
            "light_slate_grey",
            "deep_pink4",
        ]

        step_color_map = {}
        for i, s in enumerate(self.steps.values()):
            step_color_map[s.step.step_id] = color_list[i % len(color_list)]

        rg = []
        for nr, stage in enumerate(self.processing_stages):

            render_group = []

            for s in self.steps.values():

                if s.step.step_id not in stage:
                    continue

                step_table = create_step_table(s, step_color_map)
                render_group.append(step_table)

            panel = Panel(
                RenderGroup(*render_group),
                box=box.ROUNDED,
                title=f"Processing stage: {nr+1}",
                title_align="left",
            )
            rg.append(panel)

        yield "[b]Steps[/b]"
        r_panel = Panel(RenderGroup(*rg), box=box.SIMPLE)
        yield r_panel


class PipelineModuleInfo(KiaraModuleTypeMetadata):
    class Config:
        extra = Extra.forbid

    @classmethod
    def from_type_name(cls, module_type_name: str, kiara: "Kiara"):

        m = kiara.get_module_class(module_type=module_type_name)

        base_conf: "PipelineConfig" = m._base_pipeline_config  # type: ignore
        structure = base_conf.create_pipeline_structure(
            parent_id=module_type_name, kiara=kiara
        )
        struc_desc = PipelineStructureDesc.create_pipeline_structure_desc(
            pipeline=structure
        )
        attrs = PipelineModuleInfo.extract_module_attributes(module_cls=m)
        attrs["structure"] = struc_desc
        pmi = PipelineModuleInfo(**attrs)
        pmi._kiara = kiara
        pmi._structure = structure
        return pmi

    _kiara: "Kiara" = PrivateAttr()
    _structure: "PipelineStructure" = PrivateAttr()
    structure: PipelineStructureDesc = Field(description="The pipeline structure.")

    def print_data_flow_graph(self, simplified: bool = True) -> None:

        structure = self._structure

        if simplified:
            graph = structure.data_flow_graph_simple
        else:
            graph = structure.data_flow_graph

        print_ascii_graph(graph)

    def print_execution_graph(self) -> None:

        structure = self._structure
        print_ascii_graph(structure.execution_graph)

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        my_table = Table(box=box.SIMPLE, show_lines=True, show_header=False)
        my_table.add_column("Property", style="i")
        my_table.add_column("Value")
        my_table.add_row("class", self.python_class.full_name)
        my_table.add_row("is pipeline", "yes")

        my_table.add_row("doc", self.model_doc())
        my_table.add_row("config class", self.config.python_class.full_name)
        my_table.add_row(
            "config",
            create_table_from_config_class(
                self.config.python_class.get_class()._config_class,  # type: ignore
                remove_pipeline_config=True,
            ),
        )

        structure = self._structure

        p_inputs = {}
        for input_name, schema in structure.pipeline_input_schema.items():
            p_inputs[input_name] = {"type": schema.type, "doc": schema.doc}
        inputs_str = yaml.dump(p_inputs)
        _inputs_txt = Syntax(inputs_str, "yaml", background_color="default")
        my_table.add_row("pipeline inputs", _inputs_txt)

        outputs = {}
        for output_name, schema in structure.pipeline_output_schema.items():
            outputs[output_name] = {"type": schema.type, "doc": schema.doc}
        outputs_str = yaml.dump(outputs)
        _outputs_txt = Syntax(outputs_str, "yaml", background_color="default")
        my_table.add_row("pipeline outputs", _outputs_txt)

        stages: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
        for nr, stage in enumerate(structure.processing_stages):
            for s_id in stage:
                step = structure.get_step(s_id)
                mc = self._kiara.get_module_class(step.module_type)
                desc = mc.get_type_metadata().model_doc()
                inputs: typing.Dict["ValueField", typing.List[str]] = {}
                for inp in structure.steps_inputs.values():
                    if inp.step_id != s_id:
                        continue
                    if inp.connected_outputs:
                        for co in inp.connected_outputs:
                            inputs.setdefault(inp, []).append(co.alias)
                    else:
                        inputs.setdefault(inp, []).append(
                            f"__pipeline__.{inp.connected_pipeline_input}"
                        )

                inp_str = []
                for k, v in inputs.items():
                    s = f"{k.value_name} ← {', '.join(v)}"
                    inp_str.append(s)

                outp_str = []
                for outp in structure.steps_outputs.values():
                    if outp.step_id != s_id:
                        continue
                    if outp.pipeline_output:
                        outp_str.append(
                            f"{outp.value_name} → __pipeline__.{outp.pipeline_output}"
                        )
                    else:
                        outp_str.append(outp.value_name)

                stages.setdefault(f"stage {nr}", {})[s_id] = {
                    "module": step.module_type,
                    "desc": desc,
                    "inputs": inp_str,
                    "outputs": outp_str,
                }

        stages_str = yaml.dump(stages)
        _stages_txt = Syntax(stages_str, "yaml", background_color="default")
        my_table.add_row("processing stages", _stages_txt)

        return my_table


class PipelineTypesGroupInfo(KiaraInfoModel):
    __root__: typing.Dict[str, PipelineModuleInfo]

    @classmethod
    def create(
        cls,
        kiara: "Kiara",
    ):

        type_names = kiara.available_pipeline_module_types

        classes = {}
        for tn in type_names:
            classes[tn] = PipelineModuleInfo.from_type_name(
                module_type_name=tn, kiara=kiara
            )

        return PipelineTypesGroupInfo(__root__=classes)

    @classmethod
    def create_renderable_from_pipeline_info_map(
        cls,
        pipeline_module_types: typing.Mapping[str, PipelineModuleInfo],
        **config: typing.Any,
    ):

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("name", style="b")
        table.add_column("desc", style="i")
        # table.add_column("steps")

        for name, info in pipeline_module_types.items():
            row = []
            row.append(name)
            row.append(info.documentation.description)
            # row.append("steps:\n\n  - " + '\n  - '.join(info.structure.step_ids) + "\n")

            table.add_row(*row)

        return table

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        return PipelineTypesGroupInfo.create_renderable_from_pipeline_info_map(
            pipeline_module_types=self.__root__
        )


def create_step_table(
    step_desc: StepDesc, step_color_map: typing.Mapping[str, str]
) -> Table:

    step = step_desc.step

    table = Table(show_header=True, box=box.SIMPLE, show_lines=False)
    table.add_column("step_id:", style="i", no_wrap=True)
    c = step_color_map[step.step_id]
    table.add_column(f"[b {c}]{step.step_id}[/b {c}]", no_wrap=True)

    # TODO: double check
    doc_ref = step.module.get_type_metadata().context.references.get(
        "documentation", None
    )
    doc_link = None
    if doc_ref:
        doc_link = doc_ref.url
    if doc_link:
        module_str = f"[link={doc_link}]{step.module_type}[/link]"
    else:
        module_str = step.module_type

    table.add_row("", f"\n{step.module.get_type_metadata().model_doc()}\n")
    table.add_row("type", module_str)

    table.add_row(
        "required", "[red]yes[/red]" if step.required else "[green]no[/green]"
    )
    table.add_row("is pipeline", "yes\n" if step.module.is_pipeline() else "no\n")

    input_links: typing.List[typing.Any] = []
    max_source_len = 0
    for source, targets in step_desc.input_connections.items():
        source_type = step_desc.step.module.input_schemas[source].type
        source = f"{source} ([i]type: {source_type}[/i])"
        source_len = len(source)
        if source_len > max_source_len:
            max_source_len = source_len
        for i, target in enumerate(targets):
            if i == 0:
                input_links.append((source, target))
            else:
                input_links.append((None, target))

    last_source = None
    for i, il in enumerate(input_links):
        source = il[0]
        if source is None:
            padding = (
                len(last_source) - 6
            ) * " "  # calculate without the [i]..[/i] markers
            source_str = padding + "  "
        else:
            last_source = source.ljust(max_source_len)
            source_str = last_source + " ← "
        target = il[1]
        tokens = target.split(".")
        assert len(tokens) == 2
        if tokens[0] == PIPELINE_PARENT_MARKER:
            target_str = f"[b]PIPE_INPUT[/b].{tokens[1]}"
        else:
            c = step_color_map[tokens[0]]
            target_str = f"[b {c}]{tokens[0]}[/b {c}].{tokens[1]}"

        postfix = ""
        if len(input_links) == i + 1:
            postfix = "\n"
        if i == 0:
            row_str = f"{source_str}{target_str}{postfix}"
            table.add_row("inputs", row_str)
        else:
            row_str = f"{source_str}{target_str}{postfix}"
            table.add_row("", row_str)

    output_links: typing.List[typing.Any] = []
    max_source_len = 0
    for source, targets in step_desc.output_connections.items():
        target_type = step_desc.step.module.output_schemas[source].type
        source = f"{source} ([i]type: {target_type}[/i])"
        source_len = len(source)
        if source_len > max_source_len:
            max_source_len = source_len
        for i, target in enumerate(targets):
            if i == 0:
                output_links.append((source, target))
            else:
                output_links.append((None, target))

    last_source = None
    for i, il in enumerate(output_links):
        source = il[0]
        if source is None:
            padding = (
                len(last_source) - 6
            ) * " "  # calculate without the [i]..[/i] markers
            source_str = padding + "  "
        else:
            last_source = source.ljust(max_source_len)
            source_str = last_source + " → "
        target = il[1]
        tokens = target.split(".")
        assert len(tokens) == 2
        if tokens[0] == PIPELINE_PARENT_MARKER:
            target_str = f"[b]PIPE_OUTPUT[/b].{tokens[1]}"
        else:
            c = step_color_map[tokens[0]]
            target_str = f"[b {c}]{tokens[0]}[/b {c}].{tokens[1]}"
        if i == 0:
            row_str = f"{source_str}{target_str}"
            table.add_row("outputs", row_str)
        else:
            row_str = f"{source_str}{target_str}"
            table.add_row("", row_str)

    return table


class PipelineState(KiaraInfoModel):
    """Describes the current state of a pipeline.

    This includes the structure of the pipeline (how the internal modules/steps are connected to each other), as well
    as all current input/output values for the pipeline itself, as well as for all internal steps.

    Use the ``dict`` or ``json`` methods to convert this object into a generic data structure.
    """

    structure: PipelineStructureDesc = Field(
        description="The structure (interconnections of modules/steps) of the pipeline."
    )
    pipeline_inputs: PipelineValuesInfo = Field(
        description="The current (externally facing) input values of this pipeline."
    )
    pipeline_outputs: PipelineValuesInfo = Field(
        description="The current (externally facing) output values of this pipeline."
    )
    step_states: typing.Dict[str, StepStatus] = Field(
        description="The status of each step."
    )
    step_inputs: typing.Dict[str, PipelineValuesInfo] = Field(
        description="The current (internal) input values of each step of this pipeline."
    )
    step_outputs: typing.Dict[str, PipelineValuesInfo] = Field(
        description="The current (internal) output values of each step of this pipeline."
    )
    status: StepStatus = Field(description="The current overal status of the pipeline.")

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        from kiara.pipeline.pipeline import StepStatus

        all: typing.List[RenderableType] = []
        all.append(f"Pipeline state for: [b]{self.structure.pipeline_id}[/b]")
        all.append("")
        if self.status == StepStatus.RESULTS_READY:
            c = "green"
        elif self.status == StepStatus.INPUTS_READY:
            c = "yellow"
        else:
            c = "red"
        all.append(f"[b]Status[/b]: [b i {c}]{self.status.name}[/b i {c}]")
        all.append("")
        all.append("[b]Inputs / Outputs[/b]")

        r_gro = []

        inp_table = Table(show_header=True, box=box.SIMPLE)
        inp_table.add_column("Field name", style="i")
        inp_table.add_column("Type")
        inp_table.add_column("Description")
        inp_table.add_column("Required")
        inp_table.add_column("Status", justify="center")
        inp_table.add_column("Ready", justify="center")
        for field_name, value in self.pipeline_inputs.values.items():
            req = value.value_schema.is_required()
            if not req:
                req_string = "no"
            else:
                req_string = "[bold]yes[/bold]"

            if value.is_constant:
                status = "-- constant --"
                valid = "[green]yes[/green]"
            elif value.is_set:
                status = "-- set --"
                valid = "[green]yes[/green]"
            elif value.is_streaming:
                status = "[yellow]-- streaming --[/yellow]"
                valid = "[yellow]yes[/yellow]"
            else:
                valid = "[green]yes[/green]" if value.is_valid else "[red]no[/red]"
                if value.is_valid:
                    status = "-- not set --"
                else:
                    status = "-- not set --"

            inp_table.add_row(
                field_name,
                value.value_schema.type,
                value.value_schema.doc,
                req_string,
                status,
                valid,
            )
        r_gro.append(
            Panel(inp_table, box=box.ROUNDED, title_align="left", title="Inputs")
        )

        out_table = Table(show_header=True, box=box.SIMPLE)
        out_table.add_column("Field name", style="i")
        out_table.add_column("Type")
        out_table.add_column("Description")
        out_table.add_column("Required")
        out_table.add_column("Status", justify="center")
        out_table.add_column("Ready", justify="center")
        for field_name, value in self.pipeline_outputs.values.items():
            req = value.value_schema.is_required()
            if not req:
                req_string = "no"
            else:
                req_string = "[bold]yes[/bold]"

            if value.is_constant:
                status = "-- constant --"
                valid = "[green]yes[/green]"
            elif value.is_set:
                status = "-- set --"
                valid = "[green]yes[/green]"
            elif value.is_streaming:
                status = "[yellow]-- streaming --[/yellow]"
                valid = "[yellow]yes[/yellow]"
            else:
                valid = "[green]yes[/green]" if value.is_valid else "[red]no[/red]"
                status = "-- not set --"

            out_table.add_row(
                field_name,
                value.value_schema.type,
                value.value_schema.doc,
                req_string,
                status,
                valid,
            )
        r_gro.append(
            Panel(out_table, box=box.ROUNDED, title_align="left", title="Outputs")
        )

        all.append(Panel(RenderGroup(*r_gro), box=box.SIMPLE))

        rg = []
        for nr, stage in enumerate(self.structure.processing_stages):

            render_group = []

            for s in self.structure.steps.values():

                if s.step.step_id not in stage:
                    continue

                step_table = create_pipeline_step_table(self, s)
                render_group.append(step_table)

            panel = Panel(
                RenderGroup(*render_group),
                box=box.ROUNDED,
                title=f"Processing stage: {nr+1}",
                title_align="left",
            )
            rg.append(panel)

        all.append("[b]Steps[/b]")
        r_panel = Panel(RenderGroup(*rg), box=box.SIMPLE)
        all.append(r_panel)

        return RenderGroup(*all)


def create_pipeline_step_table(
    pipeline_state: PipelineState, step_desc: StepDesc
) -> Table:

    step = step_desc.step

    table = Table(show_header=True, box=box.SIMPLE, show_lines=False)
    table.add_column("step_id:", style="i", no_wrap=True)
    table.add_column(f"[b]{step.step_id}[/b]", no_wrap=True)
    table.add_column("", no_wrap=True)

    doc_link = step.module.get_type_metadata().context.references.get(
        "documentation", None
    )
    if doc_link:
        # TODO: use direct link
        url = doc_link.url
        module_str = f"[link={url}]{step.module_type}[/link]"
    else:
        module_str = step.module_type
    table.add_row("type", module_str, "")
    status = pipeline_state.step_states[step.step_id]
    if status == StepStatus.STALE:
        c = "red"
    elif status == StepStatus.INPUTS_READY:
        c = "yellow"
    else:
        c = "green"
    table.add_row("status", f"[{c}]{status.name}[/{c}]")
    table.add_row("required", "[bold]yes[/bold]" if step.required else "no", "")
    table.add_row("is pipeline", "yes" if step.module.is_pipeline() else "no", "")

    inp_table = Table(show_header=False, box=box.SIMPLE)
    inp_table.add_column("Field name")
    inp_table.add_column("Status", justify="center")

    max_field_name_len = 0
    in_fields = []
    for field_name, details in pipeline_state.step_inputs[step.step_id].values.items():

        if details.is_constant:
            status_str = "[green]-- constant --[/green]"
        elif details.is_streaming:
            status_str = "[yellow]-- streaming --[/yellow]"
        elif details.is_set:
            status_str = "[green]-- set --[/green]"
        else:
            if details.is_valid:
                status_str = "[green]-- not set (not required) --[/green]"
            else:
                status_str = "[red]-- not set --[/red]"
        name = f"[b]{field_name}[/b] [i](type: {details.value_schema.type})[/i]"
        if len(name) > max_field_name_len:
            max_field_name_len = len(name)
        in_fields.append((name, status_str))

    out_fields = []
    for field_name, details in pipeline_state.step_outputs[step.step_id].values.items():

        if details.is_constant:
            status_str = "[green]-- constant --[/green]"
        elif details.is_streaming:
            status_str = "[yellow]-- streaming --[/yellow]"
        elif details.is_set:
            status_str = "[green]-- set --[/green]"
        else:
            if details.is_valid:
                status_str = "[green]-- not set (not required) --[/green]"
            else:
                status_str = "[red]-- not set --[/red]"
        name = f"[b]{field_name}[/b] [i](type: {details.value_schema.type})[/i]"
        if len(name) > max_field_name_len:
            max_field_name_len = len(name)
        out_fields.append((name, status_str))

    for i, (field, status_str) in enumerate(in_fields):
        field_str = field.ljust(max_field_name_len)
        if i == 0:
            table.add_row("inputs", f"{field_str}  {status_str}")
        else:
            table.add_row("", f"{field_str}  {status_str}")

    out_table = Table(show_header=False, box=box.SIMPLE)
    out_table.add_column("Field name")
    out_table.add_column("Status", justify="center")

    for i, (field, status_str) in enumerate(out_fields):
        field_str = field.ljust(max_field_name_len)
        if i == 0:
            table.add_row("outputs", f"{field_str}  {status_str}")
        else:
            table.add_row("", f"{field_str}  {status_str}")

    return table
