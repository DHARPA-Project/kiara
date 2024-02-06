# -*- coding: utf-8 -*-
import os
from functools import partial
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Type, Union

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
from kiara.operations.included_core_operations.render_value import (
    RenderValueOperationType,
)
from kiara.renderers import KiaraRenderer
from kiara.renderers.jinja import BaseJinjaRenderer, JinjaEnv
from kiara.utils import log_exception, log_message
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
        self._registered_renderers: Dict[str, KiaraRenderer] = {}

        self._template_pkg_loaders: Union[None, Dict[str, PackageLoader]] = None
        self._template_folders: Union[None, Dict[str, FileSystemLoader]] = None

        self._template_loader: Union[None, PrefixLoader] = None
        self._default_jinja_env: Union[None, Environment] = None

    def register_renderer_cls(self, renderer_cls: Type[KiaraRenderer]):

        try:
            self.register_renderer(renderer_type=renderer_cls)
        except Exception as e:
            log_message(
                "ignore.renderer",
                error=e,
                renderer_cls=renderer_cls,
                reason="can't initiate default renderer instance",
            )

        if hasattr(renderer_cls, "_renderer_profiles"):

            try:
                profiles = renderer_cls._renderer_profiles  # type: ignore
                if callable(profiles):
                    profiles = profiles()
                for config in profiles.values():  # type: ignore
                    try:
                        self.register_renderer(renderer_cls, config)  # type: ignore
                    except Exception as e:
                        log_exception(e)
                        log_message(
                            "ignore.renderer.profile",
                            error=e,
                            renderer_cls=renderer_cls,
                            config=config,
                        )
            except Exception as xe:
                log_exception(xe)
                log_message(
                    "ignore.renderer.profiles", error=xe, renderer_cls=renderer_cls
                )

        from kiara.renderers.included_renderers.value import ValueRenderer

        if renderer_cls == ValueRenderer:
            target_types = set()
            op_type: RenderValueOperationType = self._kiara.operation_registry.get_operation_type("render_value")  # type: ignore
            for op in op_type.operations.values():
                details = op_type.retrieve_operation_details(op)
                target_type = details.target_data_type
                target_types.add(target_type)

            for target_type in target_types:
                self.register_renderer(
                    renderer_type=ValueRenderer,
                    renderer_config={"target_type": target_type},
                )

    def register_renderer(
        self,
        renderer_type: Union[str, Type[KiaraRenderer]],
        renderer_config: Union[Mapping[str, Any], None] = None,
    ):

        if isinstance(renderer_type, str):
            renderer_cls = self.renderer_types.get(renderer_type, None)
        else:
            renderer_cls = renderer_type

        if renderer_cls is None:
            raise Exception(f"No renderer found for type: {renderer_type}.")

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
        alias = renderer.get_renderer_alias()
        if alias in self._registered_renderers.keys():
            raise Exception(
                f"Can't register renderer, duplicate renderer alias: {alias}"
            )

        self._registered_renderers[alias] = renderer

    @property
    def renderer_types(self) -> Mapping[str, Type[KiaraRenderer]]:

        if self._renderer_types is not None:
            return self._renderer_types

        self._renderer_types = find_all_kiara_renderers()
        for value in self._renderer_types.values():
            self.register_renderer_cls(value)
        return self._renderer_types

    @property
    def default_jinja_environment(self) -> Environment:

        return self.retrieve_jinja_env()

    @property
    def template_loaders(self) -> Mapping[str, BaseLoader]:

        if self._template_pkg_loaders is not None:
            return self._template_pkg_loaders

        template_pkg_loaders = {}
        template_pkg_loaders["kiara"] = PackageLoader(
            package_name="kiara", package_path="resources/templates/render"
        )

        from importlib_metadata import entry_points

        for entry_point in entry_points(group="kiara.plugin"):

            try:
                template_pkg_loaders[entry_point.value] = PackageLoader(
                    package_name=entry_point.value, package_path="resources/templates"
                )
            except ValueError:
                # means no templates directory exists
                pass

        self._template_pkg_loaders = template_pkg_loaders
        self._template_loader = None
        return self._template_pkg_loaders

    @property
    def template_folders(self) -> Mapping[str, FileSystemLoader]:

        if self._template_folders is not None:
            return self._template_folders

        self._template_folders = {}
        return self._template_folders

    def retrieve_jinja_env(self, template_base: Union[str, None] = None) -> Environment:

        if not template_base:
            if self._default_jinja_env is not None:
                return self._default_jinja_env
            loader: BaseLoader = self.template_loader
        else:

            if template_base in self.template_folders.keys():
                loader = self.template_folders[template_base]
            elif template_base in self.template_loaders.keys():
                loader = self.template_loaders[template_base]
            else:
                msg = "Available template bases:\n\n"
                bases = sorted(
                    list(self.template_folders.keys())
                    + list(self.template_loaders.keys())
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
            # depends on version of mistune that is installed
            markdown = mistune.Markdown()
        env.filters["markdown"] = partial(render_markdown, markdown)
        env.filters["extract_raw_data"] = partial(extract_raw_value, self._kiara)

        if not template_base:
            self._default_jinja_env = env

        return env

    @property
    def registered_renderers(self) -> Iterable[KiaraRenderer]:

        # make sure all the renderers are registered
        self.renderer_types
        return self._registered_renderers.values()

    def retrieve_renderers_for_source_type(
        self, source_type: str
    ) -> List[KiaraRenderer]:

        result = []
        for renderer in self.registered_renderers:
            if source_type in renderer.retrieve_supported_render_sources():
                result.append(renderer)
        return result

    def retrieve_renderers_for_source_target_combination(
        self, source_type: str, target_type: str
    ) -> List[KiaraRenderer]:

        result = []

        for renderer in self.registered_renderers:
            if (
                source_type in renderer.retrieve_supported_render_sources()
                and target_type in renderer.retrieve_supported_render_targets()
            ):
                result.append(renderer)
        return result

    def render(
        self,
        source_type: str,
        item: Any,
        target_type: str,
        render_config: Union[Mapping[str, Any], None] = None,
    ) -> Any:

        renderers = self.retrieve_renderers_for_source_target_combination(
            source_type, target_type
        )
        if not renderers:
            raise Exception(
                f"No renderer(s) available for rendering '{source_type}' to '{target_type}'."
            )

        if len(renderers) > 1:
            raise Exception(
                f"Multiple renderers available for rendering '{source_type}' to '{target_type}': {renderers}. This is not implemented yet."
            )

        renderer_instance = next(iter(renderers))
        if render_config is None:
            render_config = {}
        rc = renderer_instance.__class__._inputs_schema(**render_config)

        return renderer_instance.render(item, render_config=rc)

    @property
    def template_loader(self) -> PrefixLoader:

        if self._template_loader is not None:
            return self._template_loader

        loaders: Dict[str, BaseLoader] = dict(self.template_loaders)
        loaders.update(self.template_folders)

        self._template_loader = PrefixLoader(loaders)
        return self._template_loader

    def register_template_folder(self, alias: str, path: str):

        if alias in self.template_folders.keys():
            raise Exception(f"Duplicate template alias: {alias}")
        if alias in self.template_loaders.keys():
            raise Exception(f"Duplicate template alias: {alias}")
        if not os.path.isdir(path):
            raise Exception(f"Template path doesn't exist or is not a folder: {path}")

        self.template_folders[alias] = FileSystemLoader(path)  # type: ignore
        self._template_loader = None

    def register_template_pkg_location(self, alias: str, pkg_name: str, path: str):

        if alias in self.template_loaders.keys():
            raise Exception(f"Duplicate template alias: {alias}")
        if alias in self.template_folders.keys():
            raise Exception(f"Duplicate template alias: {alias}")

        self.template_loaders[alias] = PackageLoader(  # type: ignore
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
        result: List[str] = env.list_templates()
        return result
