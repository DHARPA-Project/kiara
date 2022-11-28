# -*- coding: utf-8 -*-
import uuid
from dag_cbor.encoding import EncodableType
from pydantic import Field, validator
from typing import TYPE_CHECKING, Any, Dict, Mapping, TypeVar, Union

from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.models import KiaraModel
from kiara.models.module.manifest import Manifest

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


class RenderValueResult(KiaraModel):
    """Object containing all the result properties of a 'render_value' operation."""

    value_id: uuid.UUID = Field(description="The value that was rendered.")
    render_config: Mapping[str, Any] = Field(
        description="The config that was used to render this.", default_factory=dict
    )
    render_manifest: str = Field(
        description="The id of the manifest that was used to render this."
    )
    related_scenes: Mapping[str, Union[None, RenderScene]] = Field(
        description="Other render scenes, related to this one.", default_factory=dict
    )
    manifest_lookup: Dict[str, Manifest] = Field(
        description="The manifests referenced in this model, indexed by the hashes.",
        default_factory=dict,
    )
    rendered: Any = Field(description="The rendered object.")

    def _retrieve_data_to_hash(self) -> EncodableType:
        return {
            "value_id": str(self.value_id),
            "render_config": self.render_config,
            "render_manifest": self.render_manifest,
        }


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
