# -*- coding: utf-8 -*-
import uuid
from dag_cbor.encoding import EncodableType
from pydantic import Field
from rich import box
from rich.table import Table
from slugify import slugify
from typing import Any, Dict, List, Mapping, Optional

from kiara import Kiara, KiaraModule
from kiara.models import KiaraModel
from kiara.models.module.jobs import ExecutionContext
from kiara.models.module.manifest import Manifest
from kiara.models.module.pipeline import PipelineConfig
from kiara.models.module.pipeline.structure import PipelineStructure
from kiara.modules.included_core_modules.pipeline import PipelineModule
from kiara.utils import find_free_id


class WorkflowState(KiaraModel):

    workflow_id: uuid.UUID = Field(
        description="The id of this states' workflow object."
    )
    pipeline_config: PipelineConfig = Field(
        description="The current pipeline config/structure."
    )
    inputs: Dict[str, uuid.UUID] = Field(description="The current input values.")

    def _retrieve_data_to_hash(self) -> EncodableType:
        return self.structure.instance_cid

    @property
    def structure(self) -> PipelineStructure:
        return self.pipeline_config.structure


class Workflow(object):
    def __init__(self, workflow_alias: str, kiara: Kiara):

        self._kiara: Kiara = kiara
        self._workflow_alias: str = workflow_alias
        self._steps: Dict[str, Manifest] = {}
        self._input_links: Dict[str, List[str]] = {}

        self._pipeline_config: Optional[PipelineConfig] = None
        self._pipeline_manifest: Optional[Manifest] = None
        self._pipeline_module: Optional[PipelineModule] = None

        self._execution_context: ExecutionContext = ExecutionContext()

    @property
    def workflow_alias(self) -> str:
        return self._workflow_alias

    def _invalidate(self):

        self._pipeline_config = None

    def _validate(self):

        pass

    def apply(self):

        self._validate()

    @property
    def pipeline_config(self) -> PipelineConfig:

        if self._pipeline_config is not None:
            return self._pipeline_config

        all_steps = []

        for step_id, manifest in self._steps.items():
            _step_data: Dict[str, Any] = dict(manifest.manifest_data)
            _step_data["step_id"] = step_id
            input_links = {}
            for source, target in self._input_links.items():
                if source.startswith(f"{step_id}."):
                    _, field_name = source.split(".", maxsplit=1)
                    input_links[field_name] = target
            _step_data["input_links"] = input_links
            all_steps.append(_step_data)

        self._pipeline_config = PipelineConfig.from_config(
            pipeline_name=self.workflow_alias,
            data={"steps": all_steps},
            kiara=self._kiara,
            execution_context=self._execution_context,
        )
        return self._pipeline_config

    @property
    def pipeline_manifest(self) -> Manifest:

        if self._pipeline_manifest is not None:
            return self._pipeline_manifest

        self._pipeline_manifest = self._kiara.create_manifest(
            module_or_operation="pipeline", config=self.pipeline_config.dict()
        )
        return self._pipeline_manifest

    @property
    def pipeline_module(self) -> KiaraModule:

        if self._pipeline_module is not None:
            return self._pipeline_module

        self._pipeline_module = self._kiara.create_module(  # type: ignore
            manifest=self.pipeline_manifest
        )
        return self._pipeline_module  # type: ignore

    def add_step(
        self,
        module_type: str,
        step_id: Optional[str] = None,
        module_config: Mapping[str, Any] = None,
        replace_existing: bool = False,
    ) -> str:

        if step_id is None:
            step_id = find_free_id(
                slugify(module_type, separator="_"), current_ids=self._steps.keys()
            )

        if "." in step_id:
            raise Exception(f"Invalid step id '{step_id}': id can't contain '.'.")

        if step_id in self._steps.keys() and not replace_existing:
            raise Exception(
                f"Can't add step with id '{step_id}': step already exists and 'replace_existing' not set."
            )
        elif step_id in self._steps.keys():
            raise NotImplementedError()

        manifest = self._kiara.create_manifest(
            module_or_operation=module_type, config=module_config
        )
        self._steps[step_id] = manifest
        return step_id

    def add_input_link(self, input_field: str, source: str):

        self._input_links[input_field] = [source]

    def create_renderable(self, **config: Any):

        table = Table(box=box.SIMPLE, show_header=False)
        table.add_column("key", style="i")
        table.add_column("value")

        table.add_row("workflow alias", self.workflow_alias)
        table.add_row("pipeline", self.pipeline_module.create_renderable(**config))

        return table
