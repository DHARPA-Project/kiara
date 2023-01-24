# -*- coding: utf-8 -*-

import os
from functools import partial
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Type, Union

import mistune
import structlog
from jinja2 import (
    BaseLoader,
    Environment,
    FileSystemLoader,
    PackageLoader,
    PrefixLoader,
    Template,
    TemplateNotFound,
    select_autoescape,
)

from kiara.defaults import SpecialValue
from kiara.exceptions import KiaraException
from kiara.renderers import KiaraRenderer
from kiara.renderers.jinja import BaseJinjaRenderer, JinjaEnv
from kiara.utils import log_message
from kiara.utils.class_loading import find_all_kiara_renderers
from kiara.utils.values import extract_raw_value

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.models import KiaraModel

logger = structlog.getLogger()


def render_model_filter(render_registry: "RenderRegistry", instance: "KiaraModel"):

    template = render_registry.get_template("kiara/render/models/model_data.html")
    rendered = template.render(instance=instance)
    return rendered


def boolean_filter(data: bool):

    return "yes" if data else "no"


def default_filter(data: Any):

    if data in [None, SpecialValue.NO_VALUE, SpecialValue.NOT_SET]:
        return ""
    elif callable(data):
        return str(data())
    else:
        return str(data)


def render_markdown(markdown: mistune.Markdown, markdown_str: str):
    return markdown(markdown_str)


class RenderRegistry(object):
    """A registry collecting all Renderer types/objects that are available to render Value objects or internal kiara models."""

    _instance = None

    def __init__(self, kiara: "Kiara") -> None:

        self._kiara: Kiara = kiara

        self._renderer_types: Union[Mapping[str, Type[KiaraRenderer]], None] = None
        self._registered_renderers: Dict[type, Dict[str, KiaraRenderer]] = {}

        self._template_pkg_locations: Dict[str, PackageLoader] = {}
        self._template_dirs: Dict[str, FileSystemLoader] = {}
        self._template_loader: Union[None, PrefixLoader] = None
        self._default_jinja_env: Union[None, Environment] = None

        # TODO: make this lazy
        self.register_template_pkg_location(
            "kiara", "kiara", "resources/templates/render"
        )

        from kiara.renderers.included_renderers.pipeline import PipelineRenderer

        self.register_renderer_cls(PipelineRenderer)

    def register_renderer_cls(self, renderer_cls: Type[KiaraRenderer]):

        if not hasattr(renderer_cls, "_render_profiles"):
            log_message(
                "ignore.renderer",
                reason="no render_profiles attached",
                renderer_cls=renderer_cls,
            )
            return

        for source_type in renderer_cls.retrieve_supported_source_types():

            for alias, config in renderer_cls._render_profiles.items():  # type: ignore
                try:
                    self.register_renderer(source_type, alias, renderer_cls._renderer_name, config)  # type: ignore
                except Exception as e:
                    import traceback

                    traceback.print_exc()
                    log_message("ignore.renderer", error=e, renderer_cls=renderer_cls)

    def register_renderer(
        self,
        source_type: Type,
        alias: str,
        renderer_type: str,
        renderer_config: Union[Mapping[str, Any], None] = None,
    ):

        renderer_cls = self.renderer_types.get(renderer_type, None)
        if renderer_cls is None:
            raise Exception(f"No renderer found for type: {renderer_type}.")

        if alias in self._registered_renderers.setdefault(source_type, {}).keys():
            raise Exception(
                f"Duplicate renderer alias for source type '{source_type.__name__}': {alias}"
            )

        if renderer_config is None:
            renderer_config = {}
        else:
            renderer_config = dict(renderer_config)

        if BaseJinjaRenderer in renderer_cls.mro():
            if "env" not in renderer_config:
                default_env = JinjaEnv()
                renderer_config["env"] = default_env

            assert renderer_config["env"]._render_registry is None
            renderer_config["env"]._render_registry = self

        renderer = renderer_cls(kiara=self._kiara, renderer_config=renderer_config)
        self._registered_renderers[source_type][alias] = renderer

    @property
    def renderer_types(self) -> Mapping[str, Type[KiaraRenderer]]:

        if self._renderer_types is not None:
            return self._renderer_types

        self._renderer_types = find_all_kiara_renderers()
        return self._renderer_types

    @property
    def default_jinja_environment(self) -> Environment:

        return self.retrieve_jinja_env()

    def retrieve_jinja_env(self, template_base: Union[str, None] = None) -> Environment:

        if not template_base:
            if self._default_jinja_env is not None:
                return self._default_jinja_env
            loader: BaseLoader = self.template_loader
        else:

            if template_base in self._template_dirs.keys():
                loader = self._template_dirs[template_base]
            elif template_base in self._template_pkg_locations.keys():
                loader = self._template_pkg_locations[template_base]
            else:
                msg = "Available template bases:\n\n"
                bases = sorted(
                    list(self._template_dirs.keys())
                    + list(self._template_pkg_locations.keys())
                )
                for base in bases:
                    msg += f" - {base}\n"
                raise KiaraException(
                    f"No template base found for: {template_base}", details=msg
                )

        env = Environment(loader=loader, autoescape=select_autoescape())

        env.filters["render_model"] = partial(render_model_filter, self)
        env.filters["render_bool"] = boolean_filter
        env.filters["render_default"] = default_filter
        try:
            markdown = mistune.create_markdown()
        except Exception:
            markdown = mistune.Markdown()
        env.filters["markdown"] = partial(render_markdown, markdown)
        env.filters["extract_raw_data"] = partial(extract_raw_value, self._kiara)

        if not template_base:
            self._default_jinja_env = env

        return env

    def retrieve_renderers_for_type(self, item: Any) -> List[str]:

        if not isinstance(item, type):
            item_type = item.__class__
        else:
            item_type = item

        return list(self._registered_renderers.get(item_type, {}).keys())

    def render(
        self,
        item: Any,
        renderer_alias: str,
        render_config: Union[Mapping[str, Any], None] = None,
    ) -> Any:
        renderers = self.retrieve_renderers_for_type(item.__class__)
        if not renderers:
            raise Exception(
                f"No renderer(s) available for Python type: {type(item).__name__}"
            )

        renderer_instance = self._registered_renderers.get(item.__class__, {}).get(
            renderer_alias, None
        )
        if renderer_instance is None:
            msg = "Available renderers:\n\n"
            for r in renderers:
                msg += f" - {r}\n"
            raise KiaraException(
                f"No renderer with alias '{renderer_alias}' registered for Python type: {type(item).__name__}",
                details=msg,
            )

        rc = renderer_instance.__class__._inputs_schema(**render_config)

        return renderer_instance.render(item, render_config=rc)

    @property
    def template_loader(self) -> PrefixLoader:

        if self._template_loader is not None:
            return self._template_loader

        loaders: Dict[str, BaseLoader] = dict(self._template_pkg_locations)
        loaders.update(self._template_dirs)

        self._template_loader = PrefixLoader(loaders)
        return self._template_loader

    def register_template_folder(self, alias: str, path: str):

        if alias in self._template_dirs.keys():
            raise Exception(f"Duplicate template alias: {alias}")
        if alias in self._template_pkg_locations.keys():
            raise Exception(f"Duplicate template alias: {alias}")
        if not os.path.isdir(path):
            raise Exception(f"Template path doesn't exist or is not a folder: {path}")

        self._template_dirs[alias] = FileSystemLoader(path)
        self._template_loader = None

    def register_template_pkg_location(self, alias: str, pkg_name: str, path: str):

        if alias in self._template_pkg_locations.keys():
            raise Exception(f"Duplicate template alias: {alias}")
        if alias in self._template_dirs.keys():
            raise Exception(f"Duplicate template alias: {alias}")

        self._template_pkg_locations[alias] = PackageLoader(
            package_name=pkg_name, package_path=path
        )

        self._template_loader = None

    def get_template(
        self, name: str, template_base: Union[str, None] = None
    ) -> Template:
        env = self.retrieve_jinja_env(template_base=template_base)
        try:
            return env.get_template(name=name)
        except TemplateNotFound:
            available_templates = env.list_templates()
            if not available_templates:
                if template_base:
                    details = "No templates registered in default jinja environment."
                else:
                    details = f"No templates registered in jinja environment with template base: {template_base}."
            else:
                details = "Available templates:\n\n"
                for at in available_templates:
                    details += f"- {at}\n"

            raise KiaraException(f"Template not found: {name}", details=details)

    def get_template_names(self, template_base: Union[str, None] = None) -> List[str]:
        """List all available template names."""

        env = self.retrieve_jinja_env(template_base=template_base)
        return env.list_templates()
