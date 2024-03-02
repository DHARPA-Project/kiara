# -*- coding: utf-8 -*-
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Mapping,
    Set,
    Type,
    Union,
)

from jinja2 import Template

from kiara.interfaces.python_api.models.archive import KiArchive
from kiara.models.module.pipeline.pipeline import Pipeline
from kiara.renderers import (
    RenderInputsSchema,
    SourceTransformer,
)
from kiara.renderers.jinja import BaseJinjaRenderer, JinjaEnv

if TYPE_CHECKING:
    from kiara.context import Kiara


class KiArchiveTransformer(SourceTransformer):
    def __init__(self, kiara: "Kiara"):
        self._kiara: "Kiara" = kiara

        super().__init__()

    def retrieve_supported_python_classes(self) -> Iterable[Type]:
        return [KiArchive, str, Path]

    def retrieve_supported_inputs_descs(self) -> Union[str, Iterable[str]]:
        return [
            "a KiaraArchive instance",
            "a path to a a kiara archive file",
        ]

    def validate_and_transform(self, source: Any) -> Union["KiArchive", None]:

        if isinstance(source, (str, Path)):
            archive: Union[KiArchive, None] = KiArchive.load_kiarchive(
                kiara=self._kiara, path=source
            )
        elif isinstance(source, KiArchive):
            archive = source
        else:
            archive = None

        return archive


class ArchiveRendererHtml(BaseJinjaRenderer[Type[Pipeline], RenderInputsSchema]):
    """Renders archive information as a static html page.

    This is a placeholder for now.
    """

    _renderer_name = "archive_html"

    # _render_profiles: Mapping[str, Mapping[str, Any]] = {"html": {}}

    def retrieve_supported_render_sources(cls) -> str:
        return "archive"

    def retrieve_supported_render_targets(self) -> Union[Set[str], str]:
        return "html"

    def retrieve_source_transformers(self) -> Iterable[SourceTransformer]:
        return [KiArchiveTransformer(kiara=self._kiara)]

    def retrieve_jinja_env(self) -> JinjaEnv:

        jinja_env = JinjaEnv(template_base="kiara")
        return jinja_env

    def get_template(self, render_config: RenderInputsSchema) -> Template:

        return self.get_jinja_env().get_template("archive/static_page/page.html.j2")

    def assemble_render_inputs(
        self, instance: Any, render_config: RenderInputsSchema
    ) -> Mapping[str, Any]:

        inputs = {
            "archive": instance,
        }
        return inputs
