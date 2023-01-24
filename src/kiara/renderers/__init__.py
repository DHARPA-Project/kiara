# -*- coding: utf-8 -*-

#  Copyright (c) 2022, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
from typing import TYPE_CHECKING, Any, Generic, Iterable, Mapping, Type, TypeVar, Union

from jinja2 import TemplateNotFound

from kiara.exceptions import KiaraException
from kiara.models import KiaraModel

if TYPE_CHECKING:
    from kiara.context import Kiara


class KiaraRendererConfig(KiaraModel):
    pass


class RenderInputsSchema(KiaraModel):
    pass


RENDERER_CONFIG = TypeVar("RENDERER_CONFIG", bound=KiaraRendererConfig)
SOURCE_TYPE = TypeVar("SOURCE_TYPE", bound=Type)
INPUTS_SCHEMA = TypeVar("INPUTS_SCHEMA", bound=RenderInputsSchema)
TARGET_TYPE = TypeVar("TARGET_TYPE")


class KiaraRenderer(
    abc.ABC, Generic[SOURCE_TYPE, INPUTS_SCHEMA, TARGET_TYPE, RENDERER_CONFIG]
):

    _renderer_config_cls: Type[RENDERER_CONFIG] = KiaraRendererConfig  # type: ignore
    _inputs_schema: Type[INPUTS_SCHEMA] = RenderInputsSchema  # type: ignore

    @classmethod
    @abc.abstractmethod
    def retrieve_supported_source_types(self) -> Iterable[Type]:
        pass

    def __init__(
        self,
        kiara: "Kiara",
        renderer_config: Union[None, Mapping[str, Any], KiaraRendererConfig] = None,
    ):

        self._kiara: "Kiara" = kiara
        if renderer_config is None:
            self._config: RENDERER_CONFIG = self.__class__._renderer_config_cls()
        elif isinstance(renderer_config, Mapping):
            self._config = self.__class__._renderer_config_cls(**renderer_config)
        elif not isinstance(renderer_config, self.__class__._renderer_config_cls):
            raise Exception(
                f"Can't create renderer instance, invalid config type: {type(renderer_config)}, must be: {self.__class__._renderer_config_cls.__name__}"
            )
        else:
            self._config = renderer_config

    @property
    def renderer_config(self) -> RENDERER_CONFIG:
        return self._config

    def transform_source(self, source_obj: Any) -> SOURCE_TYPE:
        return source_obj

    @abc.abstractmethod
    def _render(
        self, instance: SOURCE_TYPE, render_config: INPUTS_SCHEMA
    ) -> TARGET_TYPE:
        pass

    def _post_process(self, rendered: Any) -> TARGET_TYPE:
        return rendered

    def render(self, object: SOURCE_TYPE, render_config: INPUTS_SCHEMA) -> Any:

        try:
            transformed = self.transform_source(object)
        except Exception as e:
            raise KiaraException("Error transforming source object.", parent=e)
        try:
            rendered: TARGET_TYPE = self._render(
                instance=transformed, render_config=render_config
            )
        except Exception as e:

            if isinstance(e, TemplateNotFound):
                details = f"Template not found: {e}"
                raise KiaraException("Error while rendering item.", details=details)
            else:
                raise e

        try:
            post_processed: TARGET_TYPE = self._post_process(rendered=rendered)
        except Exception as e:
            raise KiaraException("Error post-processing rendered item.", parent=e)

        return post_processed
