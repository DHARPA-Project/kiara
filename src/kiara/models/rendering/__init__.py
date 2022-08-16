# -*- coding: utf-8 -*-
import orjson
from pydantic import Extra, Field, validator
from pydantic.generics import GenericModel
from typing import TYPE_CHECKING, Any, Dict, Generic, Mapping, TypeVar, Union

from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.models import KiaraModel
from kiara.models.module.manifest import Manifest
from kiara.utils.json import orjson_dumps

if TYPE_CHECKING:
    pass

DataT = TypeVar("DataT")


class RenderScene(KiaraModel):

    _kiara_model_id = "instance.render_scene"

    title: str = Field(description="The title of this scene.")
    disabled: bool = Field(
        description="Whether this scene should be displayed as 'disabled' in a UI.",
        default=False,
    )
    description: str = Field(
        description="Description of what this scene renders.",
        default=DEFAULT_NO_DESC_VALUE,
    )
    manifest_hash: str = Field(
        description="The hash of the manifest of the referenced render scene."
    )
    render_config: Mapping[str, Any] = Field(
        description="The inputs used with the referenced manifest.",
        default_factory=dict,
    )
    related_scenes: Mapping[str, Union[None, "RenderScene"]] = Field(
        description="Other render scenes, related to this one.", default_factory=dict
    )

    @validator("manifest_hash", pre=True)
    def validate_manifest_hash(cls, value):

        if hasattr(value, "manifest_hash"):
            return value.manifest_hash  # type: ignore
        else:
            return value

    # @validator("description", pre=True)
    # def validate_desc(cls, value):
    #     return DocumentationMetadataModel.create(value)


class RenderValueResult(GenericModel, Generic[DataT]):
    class Config(object):
        json_loads = orjson.loads
        json_dumps = orjson_dumps
        extra = Extra.forbid

    # @classmethod
    # def create(cls, this_scene: RenderScene, this_manifest: Manifest, related_scenes: Union[Mapping[str, RenderScene]]=None, manifest_lookup: Union[Mapping[str, Manifest]]=None, description: Any=None):
    #
    #     if related_scenes is None:
    #         related_scenes = {}
    #     if manifest_lookup is None:
    #         manifest_lookup = {}
    #
    #     assert this_scene.title not in related_scenes.keys()
    #     related_scenes[this_scene.title] = this_scene
    #     manifest_lookup[this_scene.manifest_hash] = this_manifest.manifest_hash
    #
    #     return RenderSceneResult.construct(
    #         related_scenes=related_scenes,
    #         manifest_lookup=manifest_lookup,
    #         description=description,
    #         this_scene=this_scene.title
    #     )

    rendered: DataT = Field(description="The rendered object.")
    related_scenes: Mapping[str, Union[None, RenderScene]] = Field(
        description="Other render scenes, related to this one.", default_factory=dict
    )
    manifest_lookup: Dict[str, Manifest] = Field(
        description="The manifests referenced in this model, indexed by the hashes.",
        default_factory=dict,
    )


# class ValueRenderSceneString(RenderScene[str]):
#
#     pass
#
# class ValueRenderSceneTerminal(RenderScene[RenderableType]):
#
#     class Config:
#         arbitrary_types_allowed = True
#
# class ValueRenderSceneHtml(RenderScene[str]):
#
#     pass
