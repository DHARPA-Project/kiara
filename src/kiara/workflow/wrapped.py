# -*- coding: utf-8 -*-
import typing

if typing.TYPE_CHECKING:
    pass

# class WrappedStep(object):
#
#     def __init__(self, workflow: "WrappedWorkflow", module_type: str, module_config: typing.Optional[typing.Mapping[str, typing.Any]]=None, input_links: typing.Optional[typing.Mapping[str, typing.Any]]=None, step_id: typing.Optional[str]=None):
#
#         self._workflow: WrappedWorkflow = workflow
#         self._module_type: str = module_type
#         if module_config is None:
#             module_config = {}
#         else:
#             module_config = dict(module_config)
#         self._module_config: typing.Dict[str, typing] = module_config
#
#         if step_id is None:
#             step_id = slugify(module_type)
#         self._step_id: str = step_id
#
#         self._step_config: typing.Optional[PipelineStepConfig] = None
#         self._module: typing.Optional[KiaraModule] = None
#
#         self._input_links: typing.Dict[str, typing.List[StepValueAddress]] = {}
#         if input_links:
#             for k, v in input_links.items():
#                 self._add_input_link(k, v)
#
#         self._input_points = DataPoints(step=self)
#         self._output_points = DataPoints(step=self)
#
#     def invalidate(self):
#
#         self._step_config = None
#         self._module = None
#         self._workflow.invalidate()
#
#     @property
#     def step_id(self) -> str:
#         return self._step_id
#
#     @property
#     def module_type(self) -> str:
#         return self._module_type
#
#     @property
#     def module_config(self) -> typing.Dict[str, typing.Mapping[str, typing.Any]]:
#         return self._module_config
#
#     @property
#     def input(self) -> DataPoints:
#         return self._input_points
#
#     @property
#     def output(self) -> DataPoint:
#         return self._output_points
#
#     @property
#     def config(self) -> PipelineStepConfig:
#
#         if self._step_config is not None:
#             return self._step_config
#
#         self._step_config = PipelineStepConfig(step_id=self.step_id, module_type=self.module_type, module_config=self.module_config, input_links=self._input_links)
#         return self._step_config
#
#     @property
#     def module(self) -> KiaraModule:
#         if self._module is not None:
#             return self._module
#
#         self._module = self.config.create_module(kiara=self._workflow.kiara)
#         return self._module
#
#     @property
#     def input_names(self) -> typing.Iterable[str]:
#         return self.module.input_names
#
#     @property
#     def output_names(self) -> typing.Iterable[str]:
#         return self.module.output_names
#
#     @property
#     def input_schemas(self) -> typing.Mapping[str, ValueSchema]:
#         return self.module.input_schemas
#
#     @property
#     def output_schemas(self) -> typing.Mapping[str, ValueSchema]:
#         return self.module.output_schemas
#
#     def connect_input(self, input_name: str, target_step: typing.Union["WrappedStep", str], sub_target: typing.Union[str, typing.Mapping, typing.Iterable]=None):
#
#         if isinstance(target_step, WrappedStep):
#             step_id = target_step.step_id
#             target_step_obj = target_step
#             output_name = None
#             _sub_target = None
#         elif isinstance(target_step, str):
#             if "." not in target_step:
#                 step_id = target_step
#                 output_name = None
#                 _sub_target = None
#             else:
#                 step_id, rest = target_step.split(".", maxsplit=1)
#                 if "." not in rest:
#                     output_name = rest
#                     _sub_target = None
#                 else:
#                     output_name, _sub_target = rest.split(".", maxsplit=1)
#             target_step_obj = self._workflow.get_step(step_id=step_id)
#
#         if sub_target is not None:
#             if _sub_target:
#                 raise Exception(f"Sub-target specified twice: {sub_target} -- {_sub_target}")
#
#             if isinstance(sub_target, str):
#                 _sub_target = sub_target
#             elif isinstance(sub_target, typing.Mapping):
#                 raise NotImplementedError()
#             elif isinstance(sub_target, typing.Iterable):
#                 raise NotImplementedError()
#             else:
#                 raise TypeError(f"Invalid type '{type(sub_target)}' for sub_target value.")
#
#         if not output_name:
#             # here we try to find the right connnecting point automatically
#             if target_step_obj is None:
#                 raise NotImplementedError()
#
#             if input_name in target_step_obj.output_schemas.keys():
#                 req_type = self.input_schemas[input_name].type
#                 avail_type = target_step_obj.output_schemas[input_name].type
#                 if req_type == avail_type:
#                     output_name = input_name
#                 else:
#                     raise NotImplementedError()
#             else:
#                 # look if the connected step has a single output of the right type
#                 req_type = self.input_schemas[input_name].type
#                 match = []
#
#                 print(req_type)
#                 for n, s in target_step_obj.output_schemas.items():
#                     print(n)
#                     print(s.type)
#                     if s.type == req_type:
#                         match.append(n)
#
#                 if len(match) == 1:
#                     output_name = match[0]
#                 else:
#                     raise NotImplementedError()
#
#         target_dict = {
#             "step_id": step_id,
#             "output_name": output_name,
#             "sub_value": _sub_target
#
#         }
#
#         self._add_input_link(input_name=input_name, target=target_dict)
#
#     def _add_input_link(self, input_name: str, target: typing.Mapping):
#
#         if input_name in self._input_links.keys():
#             raise NotImplementedError()
#         vas = ensure_step_value_addresses(default_field_name=input_name, link=target)
#         self._input_links[input_name] = vas
#         self.invalidate()
#         self._workflow.invalidate()


# class WrappedWorkflow(object):
#
#     def __init__(self, kiara: "Kiara", id: str):
#
#         self._kiara: Kiara = kiara
#         self._id = id
#
#         self._steps: typing.Dict[str, PipelineStepConfig] = {}
#         self._current_structure: typing.Optional[PipelineStructure] = None
#
#     @property
#     def id(self) -> str:
#         return self._id
#
#     @property
#     def structure(self) -> PipelineStructure:
#
#         if self._current_structure is not None:
#             return self._current_structure
#
#         new_config = {}
#         steps = [x.config for x in self._steps.values()]
#         new_config["steps"] = steps
#         config = PipelineModuleConfig(**new_config)
#         self._current_structure = PipelineStructure(parent_id=self.id, config=config, kiara=self._kiara)
#         return self._current_structure
#
#     @property
#     def kiara(self) -> "Kiara":
#         return self._kiara
#
#     def invalidate(self):
#
#         self._current_structure = None
#
#     def add_step(self, module_type: str, module_config: typing.Optional[typing.Mapping[str, typing.Any]]=None, input_links: typing.Mapping[str, typing.Any]=None, step_id: typing.Optional[str]=None) -> WrappedStep:
#
#         step = WrappedStep(workflow=self, module_type=module_type, module_config=module_config, input_links=input_links, step_id=step_id)
#         if step.step_id in self._steps:
#             raise Exception(f"Duplicate step id: {step.step_id}")
#
#         self._steps[step.step_id] = step
#         self.invalidate()
#         return step
#
#     def get_step(self, step_id: str) -> WrappedStep:
#
#         if step_id in self._steps.keys():
#             return self._steps[step_id]
#         else:
#             raise Exception(f"No step with id: {step_id}")
