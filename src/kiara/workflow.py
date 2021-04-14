# -*- coding: utf-8 -*-
import os
import typing

from kiara.config import KiaraWorkflowConfig
from kiara.data.values import ValueSet
from kiara.kiara import Kiara
from kiara.pipeline.module import PipelineModule
from kiara.pipeline.pipeline import Pipeline, StepStatus
from kiara.pipeline.structure import PipelineStructure
from kiara.utils import get_auto_workflow_alias, get_data_from_file


class KiaraWorkflow(object):
    """A thin wrapper class around a [PipelineModule][kiara.pipeline.PipelineModule], mostly handling initialization from simplified configuration data."""

    def __init__(
        self,
        config: typing.Union[KiaraWorkflowConfig, typing.Mapping[str, typing.Any], str],
        workflow_id: str = None,
    ):

        if isinstance(config, typing.Mapping):
            self._workflow_config: KiaraWorkflowConfig = KiaraWorkflowConfig(**config)

        elif isinstance(config, str):
            if config == "pipeline":
                raise Exception(
                    "Can't create workflow from 'pipeline' module type without further configuration."
                )

            if config in Kiara.instance().available_module_types:
                self._workflow_config = KiaraWorkflowConfig(module_type=config)

            elif os.path.isfile(os.path.expanduser(config)):
                path = os.path.expanduser(config)
                workflow_config_data = get_data_from_file(path)
                self._workflow_config = KiaraWorkflowConfig(
                    module_config=workflow_config_data, module_type="pipeline"
                )
            else:
                raise Exception(
                    f"Can't create workflow config from string: {config}. Value either needs to be a (registered) module type name, or a path to a file."
                )
        elif isinstance(config, KiaraWorkflowConfig):
            self._workflow_config = config
        else:
            # raise TypeError(f"Invalid type '{type(workflow_config)}' for workflow configuration: {workflow_config}")
            raise TypeError(
                f"Invalid type '{type(config)}' for workflow configuration."
            )

        if not workflow_id:
            workflow_id = get_auto_workflow_alias(
                self._workflow_config.module_type, use_incremental_ids=True
            )

        self._workflow_id: str = workflow_id

        root_module_args: typing.Dict[str, typing.Any] = {"id": self._workflow_id}
        if self._workflow_config.module_type == "pipeline":
            root_module_args["module_type"] = "pipeline"
            root_module_args["module_config"] = self._workflow_config.module_config
        elif Kiara.instance().is_pipeline_module(self._workflow_config.module_type):
            root_module_args["module_type"] = self._workflow_config.module_type
            root_module_args["module_config"] = self._workflow_config.module_config
        else:
            # means it's a python module, and we wrap it into a single-module pipeline
            root_module_args["module_type"] = "pipeline"
            steps_conf = {
                "steps": [
                    {
                        "module_type": self._workflow_config.module_type,
                        "step_id": self._workflow_config.module_type,
                        "module_config": self._workflow_config.module_config,
                    }
                ]
            }
            root_module_args["module_config"] = steps_conf

        self._root_module: PipelineModule = Kiara.instance().create_module(**root_module_args)  # type: ignore
        assert isinstance(self._root_module, PipelineModule)
        self._pipeline: typing.Optional[Pipeline] = None

    @property
    def structure(self) -> PipelineStructure:
        return self._root_module.structure

    @property
    def pipeline(self) -> Pipeline:

        if self._pipeline is None:
            self._pipeline = Pipeline(self.structure)
        return self._pipeline

    @property
    def state(self) -> StepStatus:
        return self.pipeline.status

    @property
    def inputs(self) -> ValueSet:
        return self.pipeline.inputs

    # @inputs.setter
    # def inputs(self, inputs: typing.Mapping[str, typing.Any]):
    #     self.pipeline.set_pipeline_inputs(**inputs)

    @property
    def outputs(self) -> ValueSet:
        return self.pipeline.outputs

    @property
    def input_names(self) -> typing.List[str]:
        return list(self.inputs.keys())

    @property
    def output_names(self) -> typing.List[str]:
        return list(self.outputs.keys())

    @property
    def workflow_id(self) -> str:
        return self._workflow_id

    def __repr__(self):

        return f"{self.__class__.__name__}(workflow_id={self.workflow_id}, root_module={self._root_module})"
