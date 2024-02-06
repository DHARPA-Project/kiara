# -*- coding: utf-8 -*-
import uuid
from typing import Any, ClassVar, Dict, Mapping, TypeVar, Union

import orjson
from dag_cbor import IPLDKind
from pydantic import Field, field_validator
from rich.console import RenderableType
from rich.syntax import Syntax
from rich.table import Table

from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.models import KiaraModel
from kiara.models.module.manifest import Manifest
from kiara.utils.json import orjson_dumps
from kiara.utils.output import extract_renderable

DataT = TypeVar("DataT")


class RenderScene(KiaraModel):

    _kiara_model_id: ClassVar = "instance.render_scene"

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

    @field_validator("manifest_hash", mode="before")
    @classmethod
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

    def _retrieve_data_to_hash(self) -> IPLDKind:
        return {
            "value_id": str(self.value_id),
            "render_config": self.render_config,  # type: ignore
            "render_manifest": self.render_manifest,
        }

    def create_renderable(self, **config: Any) -> RenderableType:

        show_render_result = config.get("show_render_result", True)
        show_render_metadata = config.get("show_render_metadata", False)
        if show_render_metadata:

            table: Table = Table(show_header=False)
            table.add_column("key")
            table.add_column("value")

            table.add_row("value_id", str(self.value_id))

            rc_data = orjson_dumps(self.render_config, option=orjson.OPT_INDENT_2)
            render_config = Syntax(rc_data, "json", background_color="default")
            table.add_row("applied render config", render_config)

            applied_module = self.manifest_lookup[self.render_manifest]
            table.add_row("applied module", applied_module.create_renderable(**config))  # type: ignore

            related_scenes: Dict[str, Union[str, Dict[str, Any]]] = {}
            for k, v in self.related_scenes.items():
                if v is None:
                    related_scenes[k] = "-- disabled --"
                else:
                    related_scenes[k] = v.model_dump()
            rel_scenes_json = orjson_dumps(related_scenes, option=orjson.OPT_INDENT_2)
            table.add_row(
                "related scenes",
                Syntax(rel_scenes_json, "json", background_color="default"),
            )
            if show_render_result:
                table.add_row(
                    "rendered", extract_renderable(self.rendered, render_config=config)
                )

            result: RenderableType = table
        else:
            result = extract_renderable(self.rendered, render_config=config)

        return result


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
