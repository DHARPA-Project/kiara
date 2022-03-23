# -*- coding: utf-8 -*-
import uuid
from pydantic.fields import Field, PrivateAttr
from rich import box
from rich.console import RenderableType
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table
from typing import Any, Iterable, Mapping, Optional, Type

from kiara.data_types import DataType
from kiara.defaults import DATA_TYPE_CLASS_CATEGORY_ID
from kiara.models import KiaraModel
from kiara.models.documentation import (
    AuthorsMetadataModel,
    ContextMetadataModel,
    DocumentationMetadataModel,
)
from kiara.models.python_class import PythonClass


class ValueTypeClassInfo(KiaraModel):
    @classmethod
    def create_from_data_type(self, data_type_cls: Type[DataType]) -> "ValueTypeInfo":

        authors = AuthorsMetadataModel.from_class(data_type_cls)
        doc = DocumentationMetadataModel.from_class_doc(data_type_cls)
        properties_md = ContextMetadataModel.from_class(data_type_cls)

        try:
            return ValueTypeClassInfo(
                type_name=data_type_cls._data_type_name,  # type: ignore
                data_type_cls=PythonClass.from_class(data_type_cls),
                value_cls=PythonClass.from_class(data_type_cls.python_class()),
                data_type_config_cls=PythonClass.from_class(
                    data_type_cls.data_type_config_class()
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
                    f"Invalid implementation of TypeValue subclass '{data_type_cls.__name__}': 'python_class' method must be marked as a '@classmethod'. This is a bug."
                )
            raise e

    type_name: str = Field(description="The registered name for this value type.")
    data_type_cls: PythonClass = Field(
        description="The python class of the value type."
    )
    value_cls: PythonClass = Field(description="The python class of the value itself.")
    data_type_config_cls: PythonClass = Field(
        description="The python class holding the schema for configuring this type."
    )
    documentation: DocumentationMetadataModel = Field(
        description="Documentation for the value type."
    )
    authors: AuthorsMetadataModel = Field(
        description="Information about the authros of this value type."
    )
    context: ContextMetadataModel = Field(
        description="Generic properties of this value type."
    )

    def _retrieve_id(self) -> str:
        return self.type_name

    def _retrieve_category_id(self) -> str:
        return DATA_TYPE_CLASS_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return {
            "type_name": self.type_name,
            "data_type_cls": self.data_type_cls.model_data_hash,
            "value_cls": self.value_cls.model_data_hash,
            "value_config_cls": self.data_type_config_cls,
            "documentation": self.documentation.model_data_hash,
            "authors": self.authors.model_data_hash,
            "context": self.context.model_data_hash,
        }


class DataTypeClassesInfo(KiaraModel):

    _data_type_classes: Mapping[str, Type[DataType]] = PrivateAttr()
    _group_id = PrivateAttr()
    _details: bool = PrivateAttr()

    def __init__(
        self,
        data_type_classes: Mapping[str, Type[DataType]],
        id: Optional[str] = None,
        details: bool = False,
    ):

        super().__init__()
        self._data_type_classes = data_type_classes
        if id is None:
            id = str(uuid.uuid4())
        self._group_id = id
        self._details = details

    def _retrieve_id(self) -> str:
        return self._id

    def _retrieve_category_id(self) -> str:
        return DATA_TYPES_CATEGORY_ID

    def _retrieve_subcomponent_keys(self) -> Optional[Iterable[str]]:
        return self._data_type_classes.keys()

    def _retrieve_data_to_hash(self) -> Any:
        return self._data_type_classes

    def create_renderable(self, **config: Any) -> RenderableType:

        table = Table(show_header=True, box=box.SIMPLE, show_lines=False)
        table.add_column("Type name", style="i")
        table.add_column("Description")

        for type_name in sorted(self._data_type_classes.keys()):
            t_md = ValueTypeClassInfo.create_from_data_type(
                self._data_type_classes[type_name]
            )
            if self._details:
                md = Markdown(t_md.documentation.full_doc)
            else:
                md = Markdown(t_md.documentation.description)
            table.add_row(type_name, md)

        panel = Panel(table, title="Available value types", title_align="left")
        return panel
