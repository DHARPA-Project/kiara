# -*- coding: utf-8 -*-
import importlib
import inspect
import typing
from pydantic import AnyUrl, BaseModel, EmailStr, Field
from rich import box
from rich.console import RenderableType
from rich.markdown import Markdown
from rich.table import Table

from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.metadata import MetadataModel
from kiara.utils import merge_dicts
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


class ContextMetadataModel(MetadataModel):
    @classmethod
    def from_class(cls, item_cls: typing.Type):

        data = get_metadata_for_python_module_or_class(item_cls)  # type: ignore
        merged = merge_dicts(*data)
        return cls.parse_obj(merged)

    _metadata_key = "properties"

    references: typing.Dict[str, LinkModel] = Field(
        description="References for the item.", default_factory=dict
    )
    tags: typing.Set[str] = Field(
        description="A list of tags for the item.", default_factory=set
    )
    labels: typing.Dict[str, str] = Field(
        description="A list of labels for the item.", default_factory=list
    )

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")

        if self.tags:
            table.add_row("Tags", ", ".join(self.tags))
        if self.labels:
            labels = []
            for k, v in self.labels.items():
                labels.append(f"[i]{k}[/i]: {v}")
            table.add_row("Labels", "\n".join(labels))

        if self.references:
            references = []
            for _k, _v in self.references.items():
                references.append(f"[i]{_k}[/i]: {_v.url}")
            table.add_row("References", "\n".join(references))

        return table


class DocumentationMetadataModel(MetadataModel):

    _metadata_key = "documentation"

    @classmethod
    def from_class_doc(cls, item_cls: typing.Type):

        doc = item_cls.__doc__

        if not doc:
            doc = DEFAULT_NO_DESC_VALUE

        doc = inspect.cleandoc(doc)
        if "\n" in doc:
            desc, doc = doc.split("\n", maxsplit=1)
        else:
            desc = doc
            doc = None

        if doc:
            doc = doc.strip()

        return cls(description=desc.strip(), doc=doc)

    description: str = Field(
        description="Short description of the item.", default=DEFAULT_NO_DESC_VALUE
    )
    doc: typing.Optional[str] = Field(
        description="Detailed documentation of the item (in markdown).", default=None
    )

    @property
    def full_doc(self):

        if self.doc:
            return f"{self.description}\n\n{self.doc}"
        else:
            return self.description

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        return Markdown(self.full_doc)


class OriginMetadataModel(MetadataModel):

    _metadata_key = "origin"

    @classmethod
    def from_class(cls, item_cls: typing.Type):

        data = get_metadata_for_python_module_or_class(item_cls)  # type: ignore
        merged = merge_dicts(*data)
        return cls.parse_obj(merged)

    authors: typing.List[AuthorModel] = Field(
        description="The authors/creators of this item.", default_factory=list
    )

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")

        authors = []
        for author in self.authors:
            if author.email:
                authors.append(f"{author.name} ({author.email})")
            else:
                authors.append(author.name)
        table.add_row("Authors", "\n".join(authors))

        return table
