# -*- coding: utf-8 -*-

import importlib
import os
import pkgutil
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, List, Mapping, Union

import mistune
import structlog
from jinja2 import (
    Environment,
    FileSystemLoader,
    PrefixLoader,
    Template,
    select_autoescape,
)

from kiara.defaults import SpecialValue
from kiara.utils import log_exception

if TYPE_CHECKING:
    from kiara.models import KiaraModel

logger = structlog.getLogger()


def render_model_filter(template_registry: "TemplateRegistry", instance: "KiaraModel"):

    template = template_registry.get_template("kiara/render/models/model_data.html")
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


class TemplateRegistry(object):
    """
    A registry collecting all the (jinja) templates that are available in the current environment.

    Packages can register templates by specifying an entrypoint under 'kiara.templates', pointing to a Python module
    that has template files
    """

    _instance = None

    @classmethod
    def instance(cls) -> "TemplateRegistry":
        """
        The default *kiara* TemplateRegistry instance.

        Can be a simgleton because it only contains data that is determined by the current Python environment.
        """
        if cls._instance is None:
            cls._instance = TemplateRegistry()
        return cls._instance

    def __init__(self) -> None:

        self._template_dirs: Union[None, Mapping[str, Path]] = None
        self._template_loader: Union[None, PrefixLoader] = None
        self._environment: Union[None, Environment] = None

    @property
    def environment(self) -> Environment:

        if self._environment is not None:
            return self._environment

        self._environment = Environment(
            loader=self.template_loader, autoescape=select_autoescape()
        )
        self._environment.filters["render_model"] = partial(render_model_filter, self)
        self._environment.filters["render_bool"] = boolean_filter
        self._environment.filters["render_default"] = default_filter
        try:
            markdown = mistune.create_markdown()
        except Exception:
            markdown = mistune.Markdown()
        self._environment.filters["markdown"] = partial(render_markdown, markdown)
        return self._environment

    @property
    def template_dirs(self) -> Mapping[str, Path]:

        if self._template_dirs is not None:
            return self._template_dirs

        discovered_plugins = {}

        try:
            import kiara_plugin  # type: ignore

            plugin_modules_available = True
        except Exception:
            plugin_modules_available = False
            plugin_modules = []

        if plugin_modules_available:
            plugin_modules = [
                name
                for finder, name, ispkg in pkgutil.iter_modules(
                    kiara_plugin.__path__, kiara_plugin.__name__ + "."  # type: ignore
                )
            ] + [
                name
                for finder, name, ispkg in pkgutil.iter_modules()
                if name.startswith("kiara")
            ]

        for module_name in plugin_modules:  # type: ignore

            try:
                module = importlib.import_module(module_name)
                discovered_plugins[module_name] = module
            except Exception as e:
                log_exception(e)

        all_template_dirs = {}
        for plugin_name, module in discovered_plugins.items():
            if not module.__file__:
                logger.warning(
                    "skip.discovered_plugin", plugin_name=plugin_name, module=module
                )
                continue
            templates_folder = os.path.join(
                os.path.dirname(module.__file__), "resources", "templates"
            )
            if not os.path.isdir(templates_folder):
                continue
            all_template_dirs[plugin_name] = Path(templates_folder)
            logger.debug(
                "registered.templates_dir", package=plugin_name, path=templates_folder
            )

        self._template_dirs = all_template_dirs
        return self._template_dirs

    @property
    def template_loader(self) -> PrefixLoader:

        if self._template_loader is not None:
            return self._template_loader

        loaders = {}
        for plugin_name, path in self.template_dirs.items():
            loaders[plugin_name] = FileSystemLoader(searchpath=path)

        self._template_loader = PrefixLoader(loaders)
        return self._template_loader

    def get_template(self, name: str) -> Template:

        return self.environment.get_template(name=name)

    @property
    def template_names(self) -> List[str]:
        """List all available template names."""

        templates: List[str] = self.environment.list_templates()
        return templates

    def get_template_for_model_type(
        self,
        model_type: str,
        template_format: str = "html",
        use_generic_if_none: bool = False,
    ) -> Union[Template, None]:

        matches = [
            template_name
            for template_name in self.template_names
            if template_name.endswith(f"{model_type}.{template_format}")
        ]

        if not matches and use_generic_if_none:
            matches = [
                template_name
                for template_name in self.template_names
                if template_name.endswith(f"generic_model_info.{template_format}")
            ]

        if not matches:
            return None
        elif len(matches) > 1:
            raise Exception(
                f"Multiple templates found for model type '{model_type}' and format '{template_format}'. This is not supported yet."
            )

        return self.get_template(matches[0])
