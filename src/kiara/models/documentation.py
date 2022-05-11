# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import inspect
from pydantic import Extra
from pydantic.fields import Field
from pydantic.main import BaseModel
from pydantic.networks import AnyUrl, EmailStr
from rich import box
from rich.console import RenderableType
from rich.markdown import Markdown
from rich.table import Table
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple, Type, Union

from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.models import KiaraModel
from kiara.utils import merge_dicts
from kiara.utils.global_metadata import get_metadata_for_python_module_or_class


class AuthorModel(BaseModel):

    name: str = Field(description="The full name of the author.")
    email: Optional[EmailStr] = Field(
        description="The email address of the author", default=None
    )


class LinkModel(BaseModel):

    url: AnyUrl = Field(description="The url.")
    desc: Optional[str] = Field(
        description="A short description of the link content.",
        default=DEFAULT_NO_DESC_VALUE,
    )


class AuthorsMetadataModel(KiaraModel):

    _kiara_model_id = "metadata.authors"

    class Config:
        extra = Extra.ignore

    _metadata_key = "origin"

    @classmethod
    def from_class(cls, item_cls: Type):

        data = get_metadata_for_python_module_or_class(item_cls)  # type: ignore
        merged = merge_dicts(*data)
        return cls.parse_obj(merged)

    authors: List[AuthorModel] = Field(
        description="The authors/creators of this item.", default_factory=list
    )

    def create_renderable(self, **config: Any) -> RenderableType:

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Name")
        table.add_column("Email", style="i")

        for author in reversed(self.authors):
            if author.email:
                authors: Tuple[str, Union[str, EmailStr]] = (author.name, author.email)
            else:
                authors = (author.name, "")
            table.add_row(*authors)

        return table


class ContextMetadataModel(KiaraModel):

    _kiara_model_id = "metadata.context"

    class Config:
        extra = Extra.ignore

    @classmethod
    def from_class(cls, item_cls: Type):

        data = get_metadata_for_python_module_or_class(item_cls)  # type: ignore
        merged = merge_dicts(*data)
        return cls.parse_obj(merged)

    _metadata_key = "properties"

    references: Dict[str, LinkModel] = Field(
        description="References for the item.", default_factory=dict
    )
    tags: List[str] = Field(
        description="A list of tags for the item.", default_factory=list
    )
    labels: Dict[str, str] = Field(
        description="A list of labels for the item.", default_factory=list
    )

    def create_renderable(self, **config: Any) -> RenderableType:

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
                link = f"[link={_v.url}]{_v.url}[/link]"
                references.append(f"[i]{_k}[/i]: {link}")
            table.add_row("References", "\n".join(references))

        return table

    def add_reference(
        self,
        ref_type: str,
        url: str,
        desc: Optional[str] = None,
        force: bool = False,
    ):

        if ref_type in self.references.keys() and not force:
            raise Exception(f"Reference of type '{ref_type}' already present.")
        link = LinkModel(url=url, desc=desc)
        self.references[ref_type] = link

    def get_url_for_reference(self, ref: str) -> Optional[str]:

        link = self.references.get(ref, None)
        if not link:
            return None

        return link.url


class DocumentationMetadataModel(KiaraModel):

    _kiara_model_id = "metadata.documentation"

    _metadata_key = "documentation"

    @classmethod
    def from_class_doc(cls, item_cls: Type):

        doc = item_cls.__doc__

        if not doc:
            doc = DEFAULT_NO_DESC_VALUE

        doc = inspect.cleandoc(doc)
        return cls.from_string(doc)

    @classmethod
    def from_function(cls, func: Callable):

        doc = func.__doc__

        if not doc:
            doc = DEFAULT_NO_DESC_VALUE

        doc = inspect.cleandoc(doc)
        return cls.from_string(doc)

    @classmethod
    def from_string(cls, doc: Optional[str]):

        if not doc:
            doc = DEFAULT_NO_DESC_VALUE

        if "\n" in doc:
            desc, doc = doc.split("\n", maxsplit=1)
        else:
            desc = doc
            doc = None

        if doc:
            doc = doc.strip()

        return cls(description=desc.strip(), doc=doc)

    @classmethod
    def from_dict(cls, data: Mapping):

        doc = data.get("doc", None)
        desc = data.get("description", None)
        if desc is None:
            desc = data.get("desc", None)

        if not doc and not desc:
            return cls.from_string(DEFAULT_NO_DESC_VALUE)
        elif doc and not desc:
            return cls.from_string(doc)
        elif desc and not doc:
            return cls.from_string(desc)
        else:
            return cls(description=desc, doc=doc)

    @classmethod
    def create(cls, item: Any):

        if not item:
            return cls.from_string(DEFAULT_NO_DESC_VALUE)
        elif isinstance(item, DocumentationMetadataModel):
            return item
        elif isinstance(item, Mapping):
            return cls.from_dict(item)
        if isinstance(item, type):
            return cls.from_class_doc(item)
        elif isinstance(item, str):
            return cls.from_string(item)
        else:
            raise TypeError(f"Can't create documentation from type '{type(item)}'.")

    description: str = Field(
        description="Short description of the item.", default=DEFAULT_NO_DESC_VALUE
    )
    doc: Optional[str] = Field(
        description="Detailed documentation of the item (in markdown).", default=None
    )

    @property
    def is_set(self) -> bool:
        if self.description and self.description != DEFAULT_NO_DESC_VALUE:
            return True
        else:
            return False

    def _retrieve_data_to_hash(self) -> Any:
        return self.full_doc

    @property
    def full_doc(self):

        if self.doc:
            return f"{self.description}\n\n{self.doc}"
        else:
            return self.description

    def create_renderable(self, **config: Any) -> RenderableType:

        return Markdown(self.full_doc)
