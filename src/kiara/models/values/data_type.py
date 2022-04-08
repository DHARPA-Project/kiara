# -*- coding: utf-8 -*-
from pydantic.fields import Field
from rich import box
from rich.console import RenderableType
from rich.panel import Panel
from rich.table import Table
from typing import Any, Literal, Mapping, Type

from kiara.data_types import DataType
from kiara.defaults import DATA_TYPE_CLASS_CATEGORY_ID
from kiara.models.documentation import (
    AuthorsMetadataModel,
    ContextMetadataModel,
    DocumentationMetadataModel,
)
from kiara.models.info import InfoModelGroupMixin, KiaraInfoModel
from kiara.models.python_class import PythonClass


class DataTypeClassInfo(KiaraInfoModel[DataType]):
    @classmethod
    def create_from_type_class(self, type_cls: Type[DataType]) -> "DataTypeClassInfo":

        authors = AuthorsMetadataModel.from_class(type_cls)
        doc = DocumentationMetadataModel.from_class_doc(type_cls)
        properties_md = ContextMetadataModel.from_class(type_cls)

        try:
            return DataTypeClassInfo.construct(
                type_name=type_cls._data_type_name,  # type: ignore
                python_class=PythonClass.from_class(type_cls),
                value_cls=PythonClass.from_class(type_cls.python_class()),
                data_type_config_cls=PythonClass.from_class(
                    type_cls.data_type_config_class()
                ),
                documentation=doc,
                authors=authors,
                context=properties_md,
            )
        except Exception as e:
            if isinstance(
                e, TypeError
            ) and "missing 1 required positional argument: 'cls'" in str(e):
                raise Exception(
                    f"Invalid implementation of TypeValue subclass '{type_cls.__name__}': 'python_class' method must be marked as a '@classmethod'. This is a bug."
                )
            raise e

    @classmethod
    def base_class(self) -> Type[DataType]:
        return DataType

    @classmethod
    def category_name(cls) -> str:
        return "data_type"

    value_cls: PythonClass = Field(description="The python class of the value itself.")
    data_type_config_cls: PythonClass = Field(
        description="The python class holding the schema for configuring this type."
    )

    def _retrieve_id(self) -> str:
        return self.type_name

    def _retrieve_category_id(self) -> str:
        return DATA_TYPE_CLASS_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return self.type_name

    def create_renderable(self, **config: Any) -> RenderableType:

        include_doc = config.get("include_doc", True)

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

        table.add_row("Python class", self.python_class.create_renderable())
        table.add_row("Config class", self.data_type_config_cls.create_renderable())
        table.add_row("Value class", self.value_cls.create_renderable())

        return table


class DataTypeClassesInfo(InfoModelGroupMixin):
    @classmethod
    def base_info_class(cls) -> Type[KiaraInfoModel]:
        return DataTypeClassInfo

    type_name: Literal["data_type"] = "data_type"
    type_infos: Mapping[str, DataTypeClassInfo] = Field(
        description="The data_type info instances for each type."
    )
