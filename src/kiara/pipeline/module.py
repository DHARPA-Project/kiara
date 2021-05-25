# -*- coding: utf-8 -*-
import typing
from pydantic import Extra
from rich import box
from rich.console import Console, ConsoleOptions, RenderResult
from rich.syntax import Syntax
from rich.table import Table

from kiara.data.values import ValueField, ValueSchema, ValueSet
from kiara.module import KiaraModule, ModuleInfo
from kiara.module_config import PipelineModuleConfig
from kiara.pipeline.controller import BatchController, PipelineController
from kiara.pipeline.structure import PipelineStructure
from kiara.utils import StringYAML, create_table_from_config_class, print_ascii_graph

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara

yaml = StringYAML()


class PipelineModule(KiaraModule[PipelineModuleConfig]):
    """A [KiaraModule][kiara.module.KiaraModule] that contains a collection of interconnected other modules."""

    _config_cls: typing.Type[PipelineModuleConfig] = PipelineModuleConfig  # type: ignore
    _module_type_id = "pipeline"

    @classmethod
    def is_pipeline(cls) -> bool:
        return True

    @classmethod
    def doc(cls) -> str:

        if hasattr(cls, "_base_pipeline_config"):
            bpc: "PipelineModuleConfig" = cls._base_pipeline_config  # type: ignore
            doc = bpc.doc
            return doc
        else:
            # means its a 'raw' pipeline
            return "-- n/a --"

    def __init__(
        self,
        id: str,
        parent_id: typing.Optional[str] = None,
        module_config: typing.Union[
            None, PipelineModuleConfig, typing.Mapping[str, typing.Any]
        ] = None,
        meta: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        controller: typing.Union[
            None, PipelineController, str, typing.Type[PipelineController]
        ] = None,
        kiara: typing.Optional["Kiara"] = None,
    ):

        if controller is not None and not isinstance(controller, PipelineController):
            raise NotImplementedError()
        if controller is None:
            controller = BatchController()

        self._pipeline_structure: typing.Optional[PipelineStructure] = None
        self._pipeline_controller: PipelineController = controller
        super().__init__(
            id=id,
            parent_id=parent_id,
            module_config=module_config,
            meta=meta,
            kiara=kiara,
        )

    @property
    def structure(self) -> PipelineStructure:
        """The ``PipelineStructure`` of this module."""

        if self._pipeline_structure is None:
            self._pipeline_structure = PipelineStructure(
                parent_id=self.full_id,
                steps=self._config.steps,
                input_aliases=self._config.input_aliases,
                output_aliases=self._config.output_aliases,
                kiara=self._kiara,
            )
        return self._pipeline_structure

    def create_input_schema(self) -> typing.Mapping[str, ValueSchema]:
        return self.structure.pipeline_input_schema

    def create_output_schema(self) -> typing.Mapping[str, ValueSchema]:
        return self.structure.pipeline_output_schema

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        from kiara import Pipeline

        # TODO: check for Value objects
        pipeline = Pipeline(structure=self.structure)
        inps = inputs.get_all_value_data()
        pipeline.inputs.set_values(**inps)

        outputs.set_values(**pipeline.outputs.get_all_value_data())


class PipelineModuleInfo(ModuleInfo):
    class Config:
        extra = Extra.forbid
        allow_mutation = False

    def create_structure(self) -> "PipelineStructure":

        base_conf: PipelineModuleConfig = self.module_cls._base_pipeline_config  # type: ignore
        return base_conf.create_structure(parent_id=self.module_type, kiara=self._kiara)

    @property
    def structure(self) -> "PipelineStructure":

        return self.create_structure()

    def print_data_flow_graph(self, simplified: bool = True) -> None:

        structure = self.create_structure()

        if simplified:
            graph = structure.data_flow_graph_simple
        else:
            graph = structure.data_flow_graph

        print_ascii_graph(graph)

    def print_execution_graph(self) -> None:

        structure = self.create_structure()
        print_ascii_graph(structure.execution_graph)

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        yield f"[i]PipelineModule[/i]: [b]{self.module_type}[/b]"
        my_table = Table(box=box.SIMPLE, show_lines=True, show_header=False)
        my_table.add_column("Property", style="i")
        my_table.add_column("Value")
        my_table.add_row(
            "class", f"{self.module_cls.__module__}.{self.module_cls.__qualname__}"
        )
        my_table.add_row("is pipeline", "yes")

        my_table.add_row("doc", self.doc)
        my_table.add_row(
            "config class",
            f"{self.config_cls.__module__}.{self.config_cls.__qualname__}",
        )
        my_table.add_row(
            "config",
            create_table_from_config_class(
                self.config_cls, remove_pipeline_config=True
            ),
        )

        structure = self.create_structure()

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
                desc = mc.doc()
                inputs: typing.Dict[ValueField, typing.List[str]] = {}
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

        yield my_table
