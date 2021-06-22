# -*- coding: utf-8 -*-
import importlib
import typing
from pydantic import AnyUrl, BaseModel, EmailStr, Field
from rich import box
from rich.table import Table

from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.metadata import MetadataModel
from kiara.utils.global_metadata import get_metadata_for_python_module_or_class


class PythonClassMetadata(MetadataModel):
    @classmethod
    def from_class(cls, item_cls: typing.Type):

        conf: typing.Dict[str, typing.Any] = {
            "class_name": item_cls.__name__,
            "module_name": item_cls.__module__,
            "full_name": f"{item_cls.__module__}.{item_cls.__name__}",
        }
        return PythonClassMetadata(**conf)

    class_name: str = Field(description="The name of the Python class")
    module_name: str = Field(
        description="The name of the Python module this class lives in."
    )
    full_name: str = Field(description="The full class namespace.")

    def get_class(self) -> typing.Type:
        m = importlib.import_module(self.module_name)
        return getattr(m, self.class_name)


class LinkModel(BaseModel):

    url: AnyUrl = Field(description="The url.")
    desc: typing.Optional[str] = Field(
        description="A short description of the link content.",
        default=DEFAULT_NO_DESC_VALUE,
    )


class AuthorModel(BaseModel):

    name: str = Field(description="The full name of the author.")
    email: typing.Optional[EmailStr] = Field(
        description="The email address of the author", default=None
    )


class CommonMetadataModel(MetadataModel):

    _metadata_key = "metadata"

    @classmethod
    def from_class(cls, item_cls: typing.Type):

        data = get_metadata_for_python_module_or_class(item_cls)  # type: ignore
        return cls.from_dicts(*data)

    authors: typing.List[AuthorModel] = Field(
        description="The authors/creators of this item.", default_factory=list
    )
    description: str = Field(
        description="Description of the item (in markdown).",
        default=DEFAULT_NO_DESC_VALUE,
    )
    references: typing.Dict[str, LinkModel] = Field(
        description="References for the item.", default_factory=dict
    )
    tags: typing.Set[str] = Field(
        description="A list of tags for the item.", default_factory=set
    )
    labels: typing.Dict[str, str] = Field(
        description="A list of labels for the item.", default_factory=list
    )

    def create_table(self):

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")

        table.add_row("Description", self.description)
        if self.tags:
            table.add_row("Tags", ", ".join(self.tags))
        if self.labels:
            labels = []
            for k, v in self.labels.items():
                labels.append(f"[i]{k}[/i]: {v}")
            table.add_row("Labels", "\n".join(labels))
        authors = []
        for author in self.authors:
            if author.email:
                authors.append(f"{author.name} ({author.email})")
            else:
                authors.append(author.name)
        table.add_row("Authors", "\n".join(authors))

        if self.references:
            references = []
            for k, v in self.references.items():
                references.append(f"[i]{k}[/i]: {v.url}")
            table.add_row("References", "\n".join(references))

        return table
