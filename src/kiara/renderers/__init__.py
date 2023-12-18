# -*- coding: utf-8 -*-

#  Copyright (c) 2022, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import inspect
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Iterable,
    List,
    Mapping,
    Set,
    Type,
    TypeVar,
    Union,
)

from jinja2 import TemplateNotFound

from kiara.exceptions import KiaraException
from kiara.models import KiaraModel
from kiara.models.documentation import DocumentationMetadataModel

if TYPE_CHECKING:
    from kiara.context import Kiara


class KiaraRendererConfig(KiaraModel):
    pass


class RenderInputsSchema(KiaraModel):
    pass


RENDERER_CONFIG = TypeVar("RENDERER_CONFIG", bound=KiaraRendererConfig)
SOURCE_TYPE = TypeVar("SOURCE_TYPE")
INPUTS_SCHEMA = TypeVar("INPUTS_SCHEMA", bound=RenderInputsSchema)
TARGET_TYPE = TypeVar("TARGET_TYPE")


class SourceTransformer(Generic[SOURCE_TYPE]):
    def __init__(self) -> None:
        self._doc: Union[DocumentationMetadataModel, None] = None

    @abc.abstractmethod
    def retrieve_supported_python_classes(self) -> Iterable[Type]:
        pass

    @abc.abstractmethod
    def validate_and_transform(self, source: Any) -> Union[SOURCE_TYPE, None]:
        pass

    @abc.abstractmethod
    def retrieve_supported_inputs_descs(self) -> Union[str, Iterable[str]]:
        pass


class NoOpSourceTransformer(SourceTransformer):
    def retrieve_supported_python_classes(self) -> Iterable[Type]:
        return [object]

    def validate_and_transform(self, source: Any) -> Any:
        return source

    def retrieve_supported_inputs_descs(self) -> Union[str, Iterable[str]]:
        return "any Python input, unchecked"


class KiaraRenderer(
    abc.ABC, Generic[SOURCE_TYPE, INPUTS_SCHEMA, TARGET_TYPE, RENDERER_CONFIG]
):

    _renderer_config_cls: Type[RENDERER_CONFIG] = KiaraRendererConfig  # type: ignore
    _inputs_schema: Type[INPUTS_SCHEMA] = RenderInputsSchema  # type: ignore

    def __init__(
        self,
        kiara: "Kiara",
        renderer_config: Union[None, Mapping[str, Any], KiaraRendererConfig] = None,
    ):

        self._kiara: "Kiara" = kiara
        self._source_transformers: Union[None, Iterable[SourceTransformer]] = None
        self._doc: Union[DocumentationMetadataModel, None] = None
        self._supported_inputs_desc: Union[None, Iterable[str]] = None

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

    @property
    def supported_inputs_descs(self) -> Iterable[str]:

        if self._supported_inputs_desc is not None:
            return self._supported_inputs_desc

        transformers: List[str] = []
        for transformer in self.source_transformers:
            descs = transformer.retrieve_supported_inputs_descs()
            if isinstance(descs, str):
                descs = [descs]
            transformers.extend(descs)
        return transformers

    def retrieve_doc(self) -> Union[str, None]:
        return None

    @property
    def doc(self) -> DocumentationMetadataModel:
        if self._doc is not None:
            return self._doc

        doc = self.retrieve_doc()
        if doc is None:
            doc = self.__class__.__doc__
            if not doc:
                doc = ""
            doc = f"{inspect.cleandoc(doc)}\n\n"

        transformers_list = (
            "## Supported inputs:\n\nThis renderer supports the following inputs:\n\n"
        )
        for transformer in self.supported_inputs_descs:
            transformers_list += f"- {transformer}\n"

        doc = f"{doc}\n\n{transformers_list}"

        self._doc = DocumentationMetadataModel.create(doc)
        return self._doc

    @property
    def source_transformers(self) -> Iterable[SourceTransformer]:
        if self._source_transformers is None:
            self._source_transformers = self.retrieve_source_transformers()
        return self._source_transformers

    def retrieve_source_transformers(self) -> Iterable[SourceTransformer]:
        return [NoOpSourceTransformer()]

    def retrieve_supported_python_classes(self) -> Set[Type]:
        """Retrieve the set of Python classes that this renderer supports as inputs."""
        result: Set[Type] = set()
        for x in self.source_transformers:
            result.update(x.retrieve_supported_python_classes())
        return result

    def get_renderer_alias(self) -> str:
        return self.__class__._renderer_name  # type: ignore

    @abc.abstractmethod
    def retrieve_supported_render_sources(self) -> Union[Iterable[str], str]:
        pass

    @abc.abstractmethod
    def retrieve_supported_render_targets(self) -> Union[Iterable[str], str]:
        pass

    @abc.abstractmethod
    def _render(
        self, instance: SOURCE_TYPE, render_config: INPUTS_SCHEMA
    ) -> TARGET_TYPE:
        pass

    def _post_process(self, rendered: TARGET_TYPE) -> TARGET_TYPE:
        return rendered

    def render(self, instance: SOURCE_TYPE, render_config: INPUTS_SCHEMA) -> Any:

        transformed = None
        for transformer in self.source_transformers:
            try:

                for cls in transformer.retrieve_supported_python_classes():
                    if isinstance(instance, cls):
                        transformed = transformer.validate_and_transform(instance)
                        if transformed is not None:
                            break
            except Exception as e:
                raise KiaraException("Error transforming source object.", parent=e)

        if not transformed:
            raise Exception(f"Can't transform input object: {instance}.")

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
