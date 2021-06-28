# -*- coding: utf-8 -*-
"""A Python API for creating workflow sessions and dynamic pipelines in *kiara*."""

import abc
import copy
import typing
import uuid
from pydantic import BaseModel, Field
from rich import box
from rich.console import Console, ConsoleOptions, RenderGroup, RenderResult
from rich.jupyter import JupyterMixin
from rich.panel import Panel
from slugify import slugify

from kiara import Kiara, KiaraModule, Pipeline, PipelineController, PipelineStructure
from kiara.data import Value, ValueSet
from kiara.interfaces.python_api.controller import ApiController
from kiara.metadata.module_models import KiaraModuleInstanceMetadata
from kiara.module_config import PipelineModuleConfig, PipelineStepConfig
from kiara.pipeline.pipeline import create_pipeline_step_table
from kiara.pipeline.structure import generate_pipeline_endpoint_name
from kiara.profiles import ModuleProfileConfig


class DataPointSubValue(object):
    def __init__(self, data_point: "DataPoint", sub_value_name: str):

        self._data_point: DataPoint = data_point
        self._sub_value_name: str = sub_value_name

    @property
    def point_id(self) -> str:

        return f"{self._data_point.point_id}.{self._sub_value_name}"


class DataPoint(JupyterMixin):
    def __init__(self, data_points: "DataPoints", field_name: str):

        self._points: DataPoints = data_points
        self._field_name: str = field_name
        self._links: typing.List[typing.Union[DataPoint, DataPointSubValue]] = []
        self._value: typing.Any = None
        self._sub_values: typing.Dict[str, DataPointSubValue] = {}

    def add_link(self, link: typing.Union["DataPoint", "DataPointSubValue"]):

        if self._value is not None:
            raise Exception("Can't link data point that has value set.")
        self._links.append(link)

    def set_links(self, *links: typing.Union["DataPoint", "DataPointSubValue"]):

        if self._value is not None:
            raise Exception("Can't link data point that has value set.")
        self._links.clear()
        self._links.extend(links)

    def get_links(
        self,
    ) -> typing.Iterable[typing.Union["DataPoint", "DataPointSubValue"]]:
        return self._links

    def set_value(self, value: typing.Any):

        if self._links:
            raise Exception("Can't set value for data point that has links set.")
        self._value = value

    def __getattr__(self, item):

        if item in ["value", "metadata", "data", "point_id"]:
            return self.__getattribute__(item)

        if item in self._sub_values.keys():
            return self._sub_values[item]

        self._sub_values[item] = DataPointSubValue(data_point=self, sub_value_name=item)
        return self._sub_values[item]

    @property
    def value(self) -> Value:
        step = self._points._get_step()
        if isinstance(self._points, InputDataPoints):
            value = step.workflow.get_input_value(step, self._field_name)
        else:
            value = step.workflow.get_output_value(step, self._field_name)
        value.get_metadata()
        return value

    @property
    def metadata(self) -> typing.Mapping[str, typing.Mapping[str, typing.Any]]:

        return self.value.get_metadata()

    @property
    def data(self) -> typing.Any:
        return self.value.get_value_data()

    @property
    def point_id(self) -> str:

        return f"{self._points._get_step().id}.{self._field_name}"

    def __repr__(self):
        if isinstance(self._points, InputDataPoints):
            _type = "input"
        else:
            _type = "output"
        return f"DataPoint(step={self._points._get_step().id} type={_type} field_name={self._field_name})"

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:
        yield self.value


class DataPoints(typing.MutableMapping[str, DataPoint]):
    def __init__(self, step: "Step"):

        self.__dict__["_data"] = {}
        self.__dict__["_step"] = step

    def __getitem__(self, item):

        return self._get_data_point(item)

    def __delitem__(self, key):

        raise NotImplementedError()

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __setitem__(self, key, value):
        raise NotImplementedError()

    def __getattr__(self, item):

        if item.startswith("_"):
            return self.__dict__[item]
        else:
            return self._get_data_point(item)

    def __setattr__(self, key, value):

        if key.startswith("_"):
            if key in self.__dict__.keys():
                raise Exception(f"{key} already set.")
            self.__dict__[key] = value
        elif key in self.__dict__["_data"].keys():
            self._update_data_point(key, self.__dict__["_data"][key], value)
        else:
            self._add_data_point(key, value)

    def _get_step(self) -> "Step":
        return self.__dict__["_step"]

    def _get_kiara(self) -> "Kiara":
        return self._get_step()._kiara

    def _get_data_point(self, field_name: str) -> DataPoint:

        if field_name in self._data.keys():
            value = self._data[field_name]
            assert isinstance(value, DataPoint)
            return self._data[field_name]
        else:
            return self._add_data_point(field_name, None)

    @abc.abstractmethod
    def _add_data_point(self, field_name: str, value: typing.Any) -> DataPoint:
        pass

    @abc.abstractmethod
    def _update_data_point(
        self, field_name: str, data_point: DataPoint, value: typing.Any
    ) -> DataPoint:
        pass


def iterable_is_all_data_points(input: typing.Iterable):

    if not isinstance(input, typing.Iterable):
        return False

    for item in input:
        if not isinstance(item, (DataPoint, DataPoints, Step, DataPointSubValue)):
            return False

    return True


class InputDataPoints(DataPoints):
    def __init__(self, step: "Step"):

        super().__init__(step=step)
        self._inputs: typing.Dict[str, typing.Any] = {}

    def _add_data_point(self, field_name: str, value: typing.Any) -> DataPoint:

        if field_name not in self._get_step().input_names:
            raise Exception(
                f"Invalid input name '{field_name}'. Available inputs: {', '.join(self._get_step().input_names)}"
            )

        dp = DataPoint(data_points=self, field_name=field_name)
        self._data[field_name] = dp
        return self._update_data_point(
            field_name=field_name, data_point=dp, value=value
        )

    def _update_data_point(
        self, field_name: str, data_point: DataPoint, value: typing.Any
    ) -> DataPoint:

        if value is None:
            # TODO: remote links/values?
            return data_point
        if isinstance(value, (DataPoint, DataPoints, Step, DataPointSubValue)):

            if isinstance(value, (DataPoint, DataPointSubValue)):
                self.add_link(value)
                data_point.set_links(value)
                return data_point
            else:
                raise NotImplementedError()

        elif isinstance(value, typing.Mapping):
            data_point.set_value(value)
            self._get_step().workflow.set_input(
                step_id=self._get_step().id, field_name=field_name, value=value
            )
            return data_point

        elif iterable_is_all_data_points(value):
            for link in value:
                self.add_link(link)

            data_point.set_links(*value)
            return data_point
        elif isinstance(value, typing.Iterable):
            data_point.set_value(value)
            self._get_step().workflow.set_input(
                step_id=self._get_step().id, field_name=field_name, value=value
            )
            return data_point
        else:
            data_point.set_value(value)
            self._get_step().workflow.set_input(
                step_id=self._get_step().id, field_name=field_name, value=value
            )
            return data_point
            # raise Exception(
            #     f"Can't update data point, invalid value type: {type(value)}"
            # )

    def add_link(
        self, link: typing.Union[DataPoint, DataPoints, "Step", DataPointSubValue]
    ):

        if isinstance(link, DataPoint):
            data_point = link
        elif isinstance(link, DataPointSubValue):
            data_point = link._data_point
        else:
            raise NotImplementedError()

        assert isinstance(data_point._points, OutputDataPoints)

        this_step: Step = self._get_step()
        other_step: Step = data_point._points._get_step()
        other_step.workflow = this_step.workflow


class OutputDataPoints(DataPoints):
    def __init__(self, step: "Step"):

        super().__init__(step=step)
        self._outputs: typing.Dict[str, typing.Any] = {}

    def _add_data_point(self, field_name: str, value: typing.Any) -> DataPoint:

        if field_name not in self._get_step().output_names:
            raise Exception(
                f"Invalid output name '{field_name}'. Available outputs: {', '.join(self._get_step().output_names)}"
            )

        dp = DataPoint(data_points=self, field_name=field_name)
        self._data[field_name] = dp
        self._update_data_point(field_name=field_name, data_point=dp, value=value)
        return dp

    def _update_data_point(
        self, field_name: str, data_point: DataPoint, value: typing.Any
    ) -> DataPoint:

        if value is not None:
            raise NotImplementedError()

        return data_point


class Workflow(JupyterMixin):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._id: str = str(uuid.uuid4())
        self._controller: PipelineController = ApiController()

        self._steps: typing.Dict[str, Step] = {}
        self._pipeline_conf: typing.Optional[PipelineModuleConfig] = None
        self._structure: typing.Optional[PipelineStructure] = None
        self._pipeline: typing.Optional[Pipeline] = None

        self._inputs: typing.Dict[str, typing.Any] = {}

    @property
    def id(self) -> str:
        return self._id

    def invalidate(self):
        self._pipeline_conf = None
        self._structure = None
        self._pipeline = None

    @property
    def steps(self) -> typing.Mapping[str, "Step"]:
        return self._steps

    def add_step(self, step: "Step"):

        if step.id in self._steps.keys():
            raise Exception(f"Step '{step.id}' already part of structure.")

        self._steps[step.id] = step
        self._steps[step.id]._structure = self
        self.invalidate()

    def set_input(self, step_id: str, field_name: str, value: typing.Any):

        # TODO: validate that this is not a link?
        self._inputs.setdefault(step_id, {})[field_name] = value
        if self._pipeline is not None:
            ep_name = generate_pipeline_endpoint_name(
                step_id=step_id, value_name=field_name
            )
            self._pipeline.inputs.set_value(ep_name, value)

    def merge(self, other_workflow: "Workflow"):

        if other_workflow == self:
            return

        duplicates = set()
        other_steps = set()
        for step in other_workflow.steps.values():
            if step.id in self.steps.keys():
                duplicates.add(step.id)
            else:
                other_steps.add(step)
                other_steps.update(step.workflow.steps.values())

        if duplicates:
            raise Exception(
                f"Can't merge workflows, duplicate step id(s): {', '.join(duplicates)}"
            )

        duplicate_inputs = set()
        for k in other_workflow._inputs.keys():
            if k in self._inputs.keys():
                duplicate_inputs.add(k)

        if duplicate_inputs:
            raise Exception(
                f"Can't merge workflows, duplicate input key(s): {', '.join(duplicate_inputs)}"
            )

        for step in other_steps:
            self.add_step(step)

        self._inputs.update(other_workflow._inputs)

    @property
    def pipeline_config(self) -> PipelineModuleConfig:

        if self._pipeline_conf is not None:
            return self._pipeline_conf

        steps: typing.List[PipelineStepConfig] = []
        for step in self.steps.values():
            input_links: typing.Dict[str, typing.List[str]] = {}
            for input_name, data_point in step.input.items():
                for link in data_point.get_links():
                    input_links.setdefault(input_name, []).append(link.point_id)

            step_conf = PipelineStepConfig(
                step_id=step.id,
                module_type=step.module_type,
                module_config=dict(step.module_config),
                input_links=input_links,  # type: ignore
            )
            steps.append(step_conf)

        self._pipeline_conf = PipelineModuleConfig(steps=steps)
        return self._pipeline_conf

    @property
    def structure(self) -> PipelineStructure:

        if self._structure is not None:
            return self._structure

        self._structure = self.pipeline_config.create_structure(
            "dynamic", kiara=self._kiara
        )
        return self._structure

    @property
    def pipeline(self) -> Pipeline:

        if self._pipeline is not None:
            return self._pipeline

        self._pipeline = Pipeline(structure=self.structure, controller=self._controller)
        inputs = {}
        for step_id, details in self._inputs.items():
            for value_name, value in details.items():
                ep_name = generate_pipeline_endpoint_name(
                    step_id=step_id, value_name=value_name
                )
                inputs[ep_name] = value
        self._pipeline.inputs.set_values(**inputs)
        return self._pipeline

    @property
    def inputs(self):

        table = self.pipeline.inputs._create_rich_table()
        return table

    @property
    def outputs(self):

        table = self.pipeline.outputs._create_rich_table()
        return table

    def get_input_value(
        self, step: typing.Union[str, "Step"], field_name: str
    ) -> Value:

        if isinstance(step, Step):
            step = step.id

        return self.pipeline.controller.get_pipeline_input(  # type: ignore
            step_id=step, field_name=field_name
        )

    def get_output_value(
        self, step: typing.Union[str, "Step"], field_name: str
    ) -> Value:

        if isinstance(step, Step):
            step = step.id

        return self.pipeline.controller.get_pipeline_output(  # type: ignore
            step_id=step, field_name=field_name
        )

    def get_all_output_values(self) -> ValueSet:
        return self.pipeline.controller.pipeline_outputs

    def process(self):

        return self.pipeline.controller.process_pipeline()

    def __eq__(self, other):

        if not isinstance(other, Workflow):
            return False
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        yield self.pipeline


class Step(JupyterMixin):
    def __init__(
        self,
        module_type: str,
        module_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        step_id: typing.Optional[str] = None,
        kiara: typing.Optional["Kiara"] = None,
    ):

        if kiara is None:
            from kiara.kiara import Kiara

            kiara = Kiara.instance()

        self._kiara: Kiara = kiara

        self._module_type: str = module_type
        if module_config is None:
            module_config = {}
        else:
            module_config = dict(copy.deepcopy(module_config))
        self._module_config: typing.Dict[str, typing.Any] = module_config
        self._module: typing.Optional[KiaraModule] = None

        if not step_id:
            add_uuid = False
            if "module_type" in self._module_config.keys():
                if add_uuid:
                    stem = f"{self._module_type}_{self._module_config['module_type']}_{uuid.uuid4()}"
                else:
                    stem = f"{self._module_type}_{self._module_config['module_type']}"
            else:
                if add_uuid:
                    stem = f"{self._module_type}_{uuid.uuid4()}"
                else:
                    stem = self._module_type

            step_id = slugify(stem, separator="_")

        self._step_id: str = step_id

        self._inputs: InputDataPoints = InputDataPoints(self)
        self._outputs: OutputDataPoints = OutputDataPoints(self)

        self._profile_config: typing.Optional[ModuleProfileConfig] = None

        self._structure: typing.Optional[Workflow] = None

        self._info: typing.Optional[StepInfo] = None

    @property
    def id(self) -> str:
        return self._step_id

    @property
    def info(self) -> "StepInfo":
        if self._info is None:
            module_metadata = KiaraModuleInstanceMetadata.from_module_obj(self.module)
            self._info = StepInfo(
                step_id=self._step_id, module_metadata=module_metadata
            )
        return self._info

    @property
    def module_type(self) -> str:
        return self._module_type

    @property
    def module_config(self) -> typing.Mapping[str, typing.Any]:
        return self._module_config

    def invalidate(self) -> None:
        self._profile_config = None
        self._module = None

    def config(self) -> ModuleProfileConfig:

        if self._profile_config is not None:
            return self._profile_config

        self._profile_config = ModuleProfileConfig(
            module_type=self._module_type, module_config=self._module_config
        )
        return self._profile_config

    @property
    def workflow(self) -> Workflow:

        if self._structure is not None:
            return self._structure

        self._structure = Workflow(kiara=self._kiara)
        self._structure.add_step(self)
        return self._structure

    @workflow.setter
    def workflow(self, structure: Workflow):

        current_structure = self.workflow
        structure.merge(current_structure)

    @property
    def module(self) -> KiaraModule:

        if self._module is not None:
            return self._module

        self._module = self._kiara.create_module(
            module_type=self._module_type, module_config=self._module_config
        )
        return self._module

    @property
    def input_names(self):
        return self.module.input_names

    @property
    def output_names(self):
        return self.module.output_names

    @property
    def input(self) -> DataPoints:
        return self._inputs

    @property
    def output(self) -> DataPoints:
        return self._outputs

    def __eq__(self, other):

        if not isinstance(other, Step):
            return False

        return self.id == other.id

    def __hash__(self):

        return hash(self.id)

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        current_state = self.workflow.pipeline.get_current_state()
        step = current_state.structure.steps[self.id]
        step_table = create_pipeline_step_table(current_state, step)
        yield step_table


class StepInfo(JupyterMixin, BaseModel):

    step_id: str = Field(description="The step id.")
    module_metadata: KiaraModuleInstanceMetadata = Field(
        description="The module metadata."
    )

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        doc = self.module_metadata.type_metadata.documentation.create_renderable()
        metadata = self.module_metadata.create_renderable(include_desc=False)

        panel = Panel(
            RenderGroup(Panel(doc, box=box.SIMPLE), metadata),
            title=f"Step info: [b]{self.step_id}[/b] (type: [i]{self.module_metadata.type_metadata.type_id}[/i])",
            title_align="left",
        )
        yield panel
