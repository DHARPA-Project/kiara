# -*- coding: utf-8 -*-
import abc
from pydantic import Field
from typing import Any, Dict, Iterable, NamedTuple

from kiara.models import KiaraModel


class RenderInstruction(KiaraModel):
    @classmethod
    @abc.abstractmethod
    def retrieve_source_type(cls) -> str:
        pass

    @classmethod
    def retrieve_supported_target_types(cls) -> Iterable[str]:

        result = []
        for attr in dir(cls):
            if len(attr) <= 11 or not attr.startswith("render_as__"):
                continue

            attr = attr[11:]
            target_type = attr[0:]
            result.append(target_type)

        return result


class RenderMetadata(KiaraModel):

    related_instructions: Dict[str, RenderInstruction] = Field(
        description="Related instructions, to be used by implementing frontends as hints.",
        default_factory=dict,
    )


class RenderValueResult(NamedTuple):

    rendered: Any
    metadata: RenderMetadata
