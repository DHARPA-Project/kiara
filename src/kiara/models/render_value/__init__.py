# -*- coding: utf-8 -*-
import abc
from pydantic import Field
from typing import Any, Dict, Iterable, Mapping, NamedTuple, Union

from kiara.models import KiaraModel


class RenderScene(KiaraModel):

    parent_scene: Union[None, "RenderScene"] = Field(
        description="The parent of this scene (if applicable).", default=None
    )

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

    scene_name: Union[None, str] = Field(
        description="The name of the current scene to be rendered.", default=None
    )
    scenes: Union[None, Mapping[str, "RenderScene"]] = Field(
        description="Child scenes, in case this itself is not going to be rendered.",
        default=None,
    )

    def get_render_parameters(self) -> Dict[str, Any]:
        if self.scene_name:
            if not self.scenes or self.scene_name not in self.scenes.keys():
                if self.scenes:
                    _msg = "Available names: " + ", ".join(self.scenes.keys())
                else:
                    _msg = "No child scenes available"

                raise Exception(
                    f"Can't render scene, no scene named '{self.scene_name}' available. {_msg}."
                )
            else:
                return self.scenes[self.scene_name].get_render_parameters()
        else:
            return self.dict(exclude={"scene_name", "scenes"})


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

    related_instructions: Dict[str, Union[RenderInstruction, None]] = Field(
        description="Related instructions, to be used by implementing frontends as hints.",
        default_factory=dict,
    )


class RenderValueResult(NamedTuple):

    rendered: Any
    metadata: RenderMetadata
