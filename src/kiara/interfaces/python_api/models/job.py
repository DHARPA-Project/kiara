# -*- coding: utf-8 -*-
from pydantic import BaseModel, Field
from typing import Any, Dict

from kiara.context import Kiara
from kiara.models.module.jobs import JobConfig
from kiara.models.module.manifest import Manifest


class JobDesc(BaseModel):
    """An object describing a compute job with both raw or referenced inputs."""

    module_type: str = Field(description="The module type.")
    module_config: Dict[str, Any] = Field(
        default_factory=dict, description="The configuration for the module."
    )
    inputs: Dict[str, Any] = Field(description="The inputs for the job.")

    def create_job_config(self, kiara: Kiara) -> JobConfig:

        manifest = Manifest(
            module_type=self.module_type, module_config=self.module_config
        )
        module = kiara.module_registry.create_module(manifest=manifest)
        job_config = JobConfig.create_from_module(
            data_registry=kiara.data_registry, module=module, inputs=self.inputs
        )
        return job_config
