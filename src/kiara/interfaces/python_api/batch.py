# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import os
import uuid
from pydantic import BaseModel, Field, PrivateAttr, root_validator, validator
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Union

from kiara import Kiara
from kiara.interfaces.python_api.utils import create_save_config
from kiara.models.module.pipeline import PipelineConfig
from kiara.models.module.pipeline.controller import SinglePipelineBatchController
from kiara.models.module.pipeline.pipeline import Pipeline
from kiara.models.values.value import ValueMap
from kiara.utils import get_data_from_file

if TYPE_CHECKING:
    pass


class BatchOperation(BaseModel):
    class Config:
        validate_assignment = True

    @classmethod
    def from_file(
        cls,
        path: str,
        kiara: Optional["Kiara"] = None,
    ):

        data = get_data_from_file(path)
        pipeline_id = data.get("pipeline_name", None)
        if pipeline_id is None:
            name = os.path.basename(path)
            if name.endswith(".json"):
                name = name[0:-5]
            elif name.endswith(".yaml"):
                name = name[0:-5]
            data["pipeline_name"] = name

        return cls.from_config(data=data, kiara=kiara)

    @classmethod
    def from_config(
        cls,
        data: Mapping[str, Any],
        kiara: Optional["Kiara"],
    ):

        data = dict(data)
        inputs = data.pop("inputs", {})
        save = data.pop("save", False)
        pipeline_id = data.pop("pipeline_name", None)
        if pipeline_id is None:
            pipeline_id = str(uuid.uuid4())

        if kiara is None:
            kiara = Kiara.instance()

        pipeline_config = PipelineConfig.from_config(
            pipeline_name=pipeline_id, data=data, kiara=kiara
        )

        result = cls(pipeline_config=pipeline_config, inputs=inputs, save=save)
        result._kiara = kiara
        return result

    alias: str = Field(description="The batch name/alias.")
    pipeline_config: PipelineConfig = Field(
        description="The configuration of the underlying pipeline."
    )
    inputs: Dict[str, Any] = Field(
        description="The (base) inputs to use. Can be augmented before running the operation."
    )

    save_defaults: Dict[str, List[str]] = Field(
        description="Configuration which values to save, under which alias(es).",
        default_factory=dict,
    )

    _kiara: Kiara = PrivateAttr(default=None)

    @root_validator(pre=True)
    def add_alias(cls, values):

        if not values.get("alias", None):
            pc = values.get("pipeline_config", None)
            if not pc:
                raise ValueError("No pipeline config provided.")
            if isinstance(pc, PipelineConfig):
                alias = pc.pipeline_name
            else:
                alias = pc.get("pipeline_name", None)
            values["alias"] = alias

        return values

    @validator("save_defaults", always=True, pre=True)
    def validate_save(cls, save, values):

        alias = values["alias"]
        pipeline_config = values["pipeline_config"]
        return cls.create_save_aliases(
            save=save, alias=alias, pipeline_config=pipeline_config
        )

    @classmethod
    def create_save_aliases(
        cls,
        save: Union[bool, None, str, Mapping],
        alias: str,
        pipeline_config: PipelineConfig,
    ) -> Mapping[str, Any]:

        assert isinstance(pipeline_config, PipelineConfig)

        if save in [False, None]:
            save_new: Dict[str, Any] = {}
        elif save is True:
            field_names = pipeline_config.structure.pipeline_outputs_schema.keys()
            save_new = create_save_config(field_names=field_names, aliases=alias)
        elif isinstance(save, str):
            field_names = pipeline_config.structure.pipeline_outputs_schema.keys()
            save_new = create_save_config(field_names=field_names, aliases=save)
        elif isinstance(save, Mapping):
            save_new = dict(save)
        else:
            raise ValueError(
                f"Invalid type '{type(save)}' for 'save' attribute: must be None, bool, string or Mapping."
            )

        return save_new

    def run(
        self,
        inputs: Optional[Mapping[str, Any]] = None,
        save: Union[None, bool, str, Mapping[str, Any]] = None,
    ) -> ValueMap:

        pipeline = Pipeline(
            structure=self.pipeline_config.structure,
            data_registry=self._kiara.data_registry,
        )
        pipeline_controller = SinglePipelineBatchController(
            pipeline=pipeline, job_registry=self._kiara.job_registry
        )

        run_inputs = dict(self.inputs)
        if inputs:
            run_inputs.update(inputs)

        pipeline.set_pipeline_inputs(inputs=run_inputs)
        pipeline_controller.process_pipeline()

        result = self._kiara.data_registry.load_values(
            pipeline.get_current_pipeline_outputs()
        )

        if save is not None:
            if save is True:
                save = self.save_defaults
            else:
                save = self.__class__.create_save_aliases(
                    save=save, alias=self.alias, pipeline_config=self.pipeline_config
                )

            self._kiara.save_values(values=result, alias_map=save)

        return result
