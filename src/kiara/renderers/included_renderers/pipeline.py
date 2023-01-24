# -*- coding: utf-8 -*-
from typing import Any, List, Mapping, Type

from jinja2 import Template

from kiara.models.module.pipeline.pipeline import Pipeline
from kiara.renderers import RenderInputsSchema
from kiara.renderers.jinja import BaseJinjaRenderer, JinjaEnv


class PipelineRenderer(BaseJinjaRenderer[Type[Pipeline], RenderInputsSchema]):

    _renderer_name = "pipeline_renderer"

    _render_profiles: Mapping[str, Mapping[str, Any]] = {"html": {}}

    def retrieve_jinja_env(self) -> JinjaEnv:

        jinja_env = JinjaEnv(template_base="kiara")
        return jinja_env

    def get_template(self, render_config: RenderInputsSchema) -> Template:

        return self.get_jinja_env().get_template("pipeline/static_page/page.html.j2")

    def assemble_render_inputs(
        self, instance: Any, render_config: RenderInputsSchema
    ) -> Mapping[str, Any]:

        inputs = render_config.dict()
        inputs["pipeline"] = instance
        return inputs

    @classmethod
    def retrieve_supported_source_types(cls) -> List[Type[Any]]:
        return [Pipeline]
