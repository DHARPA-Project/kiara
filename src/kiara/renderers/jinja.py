# -*- coding: utf-8 -*-
import abc
from typing import TYPE_CHECKING, Any, Generic, Mapping, Union

from jinja2 import Environment, Template
from pydantic import BaseModel, Field, PrivateAttr

from kiara.exceptions import KiaraException
from kiara.renderers import (
    INPUTS_SCHEMA,
    SOURCE_TYPE,
    KiaraRenderer,
    KiaraRendererConfig,
    RenderInputsSchema,
)

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.registries.rendering import RenderRegistry


class JinjaEnv(BaseModel):

    template_base: Union[str, None] = Field(
        description="The alias of the base loader to use. Defaults to a special loader that combines all template sources.",
        default=None,
    )

    _render_registry: "RenderRegistry" = PrivateAttr(default=None)

    @property
    def instance(self) -> Environment:

        return self._render_registry.retrieve_jinja_env(self.template_base)


class JinjaRndererConfig(KiaraRendererConfig):

    env: JinjaEnv = Field(description="The loader to use for the jinja environment.")


class BaseJinjaRenderer(
    KiaraRenderer[SOURCE_TYPE, INPUTS_SCHEMA, str, JinjaRndererConfig],
    Generic[SOURCE_TYPE, INPUTS_SCHEMA],
):

    _renderer_config_cls = JinjaRndererConfig

    def __init__(
        self,
        kiara: "Kiara",
        renderer_config: Union[None, Mapping[str, Any], KiaraRendererConfig] = None,
    ):

        self._jinja_env: Union[Environment, None] = None
        super().__init__(kiara=kiara, renderer_config=renderer_config)

    def get_jinja_env(self) -> Environment:

        if self._jinja_env is None:
            je = self.retrieve_jinja_env()
            je._render_registry = self._kiara.render_registry
            self._jinja_env = je.instance
        return self._jinja_env

    def retrieve_jinja_env(self) -> JinjaEnv:
        return JinjaEnv()

    @abc.abstractmethod
    def get_template(self, render_config: INPUTS_SCHEMA) -> Template:
        pass

    @abc.abstractmethod
    def assemble_render_inputs(
        self, instance: Any, render_config: INPUTS_SCHEMA
    ) -> Mapping[str, Any]:
        pass

    def _render(self, instance: SOURCE_TYPE, render_config: INPUTS_SCHEMA) -> str:

        template = self.get_template(render_config=render_config)
        if template is None:
            msg = "Available templates:\n"
            for template_name in self.get_jinja_env().list_templates():
                msg += f" - {template_name}\n"
            raise KiaraException(msg=f"Could not find requested template for renderer '{self.__class__._renderer_name}'", details=msg)  # type: ignore

        inputs = self.assemble_render_inputs(instance, render_config=render_config)
        rendered: str = template.render(**inputs)
        return rendered


class JinjaRenderInputsSchema(RenderInputsSchema):
    template: str = Field(description="The template to use for rendering.")


class JinjaRenderer(BaseJinjaRenderer[Any, JinjaRenderInputsSchema]):

    _renderer_name = "jinja"
    _inputs_schema_cls = JinjaRenderInputsSchema

    def get_template(self, render_config: JinjaRenderInputsSchema) -> Template:

        try:
            template = self.get_jinja_env().get_template(render_config.template)
            return template
        except Exception as e:
            raise Exception(f"Error loading template '{render_config.template}': {e}")
