# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import orjson
from pydantic import Field
from rich import box
from rich.console import RenderableType
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from typing import TYPE_CHECKING, Any, Dict, Iterable, Literal, Mapping, Type, Union

from kiara.interfaces.python_api.models.info import TypeInfo, TypeInfoItemGroup
from kiara.models import KiaraModel
from kiara.models.documentation import (
    AuthorsMetadataModel,
    ContextMetadataModel,
    DocumentationMetadataModel,
)

# from kiara.models.info import TypeInfo
from kiara.models.python_class import PythonClass
from kiara.utils.json import orjson_dumps

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.models.values.value import Value


class ValueMetadata(KiaraModel):
    @classmethod
    @abc.abstractmethod
    def retrieve_supported_data_types(cls) -> Iterable[str]:
        pass

    @classmethod
    @abc.abstractmethod
    def create_value_metadata(
        cls, value: "Value"
    ) -> Union["ValueMetadata", Dict[str, Any]]:
        pass

    # @property
    # def metadata_key(self) -> str:
    #     return self._metadata_key  # type: ignore  # this is added by the kiara class loading functionality

    def _retrieve_id(self) -> str:
        return self._metadata_key  # type: ignore

    def _retrieve_data_to_hash(self) -> Any:
        return {"metadata": self.dict(), "schema": self.schema_json()}


class MetadataTypeInfo(TypeInfo):

    _kiara_model_id = "info.metadata_type"

    @classmethod
    def create_from_type_class(
        self, type_cls: Type[ValueMetadata], kiara: "Kiara"
    ) -> "MetadataTypeInfo":

        authors_md = AuthorsMetadataModel.from_class(type_cls)
        doc = DocumentationMetadataModel.from_class_doc(type_cls)
        python_class = PythonClass.from_class(type_cls)
        properties_md = ContextMetadataModel.from_class(type_cls)
        type_name = type_cls._metadata_key  # type: ignore
        schema = type_cls.schema()

        return MetadataTypeInfo.construct(
            type_name=type_name,
            documentation=doc,
            authors=authors_md,
            context=properties_md,
            python_class=python_class,
            metadata_schema=schema,
        )

    @classmethod
    def base_class(self) -> Type[ValueMetadata]:
        return ValueMetadata

    @classmethod
    def category_name(cls) -> str:
        return "value_metadata"

    metadata_schema: Dict[str, Any] = Field(
        description="The (json) schema for this metadata value."
    )

    def create_renderable(self, **config: Any) -> RenderableType:

        include_doc = config.get("include_doc", True)
        include_schema = config.get("include_schema", True)

        table = Table(box=box.SIMPLE, show_header=False, padding=(0, 0, 0, 0))
        table.add_column("property", style="i")
        table.add_column("value")

        if include_doc:
            table.add_row(
                "Documentation",
                Panel(self.documentation.create_renderable(), box=box.SIMPLE),
            )
        table.add_row("Author(s)", self.authors.create_renderable())
        table.add_row("Context", self.context.create_renderable())

        if hasattr(self, "python_class"):
            table.add_row("Python class", self.python_class.create_renderable())

        if include_schema:
            schema = Syntax(
                orjson_dumps(self.metadata_schema, option=orjson.OPT_INDENT_2),
                "json",
                background_color="default",
            )
            table.add_row("metadata_schema", schema)

        return table


class MetadataTypeClassesInfo(TypeInfoItemGroup):

    _kiara_model_id = "info.metadata_types"

    @classmethod
    def base_info_class(cls) -> Type[TypeInfo]:
        return MetadataTypeInfo

    type_name: Literal["value_metadata"] = "value_metadata"
    item_infos: Mapping[str, MetadataTypeInfo] = Field(  # type: ignore
        description="The value metadata info instances for each type."
    )
