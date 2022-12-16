# -*- coding: utf-8 -*-

#  Copyright (c) 2022, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import os
from functools import partial
from jinja2 import BaseLoader, Environment, FileSystemLoader
from pathlib import Path
from pydantic import Field
from typing import TYPE_CHECKING, Any, Generic, Mapping, Type, TypeVar, Union

from kiara.models import KiaraModel
from kiara.models.module.pipeline.pipeline import Pipeline
from kiara.utils import log_message
from kiara.utils.values import extract_raw_value

if TYPE_CHECKING:
    from kiara.context import Kiara


class KiaraRendererConfig(KiaraModel):

    pass


RENDER_CONFIG = TypeVar("RENDER_CONFIG", bound=KiaraRendererConfig)
RENDER_SOURCE_TYPE = TypeVar("RENDER_SOURCE_TYPE")


class KiaraRenderer(abc.ABC, Generic[RENDER_SOURCE_TYPE, RENDER_CONFIG]):

    _render_config_cls: Type[RENDER_CONFIG] = KiaraRendererConfig  # type: ignore

    def __init__(
        self,
        config: Union[None, Mapping[str, Any], KiaraRendererConfig] = None,
        kiara: Union[None, "Kiara"] = None,
    ):

        if kiara is None:
            from kiara.context import Kiara

            kiara = Kiara.instance()
        self._kiara: "Kiara" = kiara
        if config is None:
            self._config: RENDER_CONFIG = self.__class__._render_config_cls()
        elif isinstance(config, Mapping):
            self._config = self.__class__._render_config_cls(**config)
        elif not isinstance(config, self.__class__._render_config_cls):
            raise Exception(
                f"Can't create renderer instance, invalid config type: {type(config)}, must be: {self.__class__._render_config_cls.__name__}"
            )
        else:
            self._config = config

    @property
    def config(self) -> RENDER_CONFIG:
        return self._config

    @abc.abstractmethod
    def get_render_source_type(self) -> Type[RENDER_SOURCE_TYPE]:
        pass

    @abc.abstractmethod
    def _render_object(self, object: RENDER_SOURCE_TYPE) -> Any:
        pass

    def _post_process(self, rendered: Any) -> Any:
        return rendered

    def render(self, object: RENDER_SOURCE_TYPE):

        rendered = self._render_object(object=object)
        post_processed = self._post_process(rendered=rendered)
        return post_processed


class JinjaPipelineRenderConfig(KiaraRendererConfig):

    template: str = Field(
        description="The template to use to render the pipeline. Either a path to a template file, or the template string directly."
    )


class JinjaPipelineRenderer(KiaraRenderer[Pipeline, JinjaPipelineRenderConfig]):

    _render_config_cls = JinjaPipelineRenderConfig

    def get_render_source_type(self) -> Type[Pipeline]:
        return Pipeline

    def _render_object(self, object: Pipeline) -> Any:

        template = self.config.template

        if os.path.isfile(template):
            path = Path(template)
            loader = FileSystemLoader(path.parent)
            env: Environment = Environment(loader=loader)
            env.filters["extract_raw_data"] = partial(extract_raw_value, self._kiara)
            _template = env.get_template(path.name)
        else:
            env = Environment(loader=BaseLoader())
            _template = env.from_string(template)

        rendered = _template.render(pipeline=object, config=self.config)
        return rendered

    # def _augment_inputs(self, **inputs: Any) -> Mapping[str, Any]:
    #
    #     # pipeline_input = inputs.get("inputs", None)
    #     module = inputs.get("module")
    #     module_config = inputs.get("module_config", None)
    #
    #     module_obj: "PipelineModule" = self._kiara.create_module(  # type: ignore
    #         module_type=module, module_config=module_config  # type: ignore
    #     )
    #
    #     if not module_obj.is_pipeline():
    #         raise Exception("Only pipeline modules supported (for now).")
    #
    #     step_inputs: Dict[str, Dict[str, Any]] = {}
    #     # for k, v in pipeline_input.items():
    #     #     pi = module_obj.structure.pipeline_inputs.get(k)
    #     #     assert pi
    #     #     if len(pi.connected_inputs) != 1:
    #     #         raise NotImplementedError()
    #     #
    #     #     ci = pi.connected_inputs[0]
    #     #     if isinstance(v, str):
    #     #         v = f'"{v}"'
    #     #     step_inputs.setdefault(ci.step_id, {})[ci.value_name] = v
    #
    #     result = {"structure": module_obj.config.structure, "input_values": step_inputs}
    #     if "template" in inputs.keys():
    #         template = inputs["template"]
    #     else:
    #         template = "notebook"
    #
    #     if template in ["notebook", "python-script"]:
    #         template = os.path.join(
    #             KIARA_RESOURCES_FOLDER, "templates", f"{template}.j2"
    #         )
    #
    #     result["template"] = template
    #     return result

    def _post_process(self, rendered: Any) -> Any:

        is_notebook = True
        if is_notebook:
            import jupytext

            notebook = jupytext.reads(rendered, fmt="py:percent")
            converted = jupytext.writes(notebook, fmt="notebook")
            return converted
        else:
            try:
                import black
                from black import Mode

                cleaned = black.format_str(rendered, mode=Mode())
                return cleaned

            except Exception as e:
                log_message(
                    f"Could not format python code, 'black' not in virtual environment: {e}."
                )
                return rendered
