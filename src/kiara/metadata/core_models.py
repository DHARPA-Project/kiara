# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import abc
import importlib
import inspect
import typing
from deepdiff import DeepHash
from pydantic import AnyUrl, BaseModel, EmailStr, Field, PrivateAttr
from rich import box
from rich.console import RenderableType
from rich.markdown import Markdown
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from types import ModuleType

from kiara.defaults import DEFAULT_NO_DESC_VALUE, KIARA_HASH_FUNCTION
from kiara.metadata import MetadataModel
from kiara.utils import merge_dicts
from kiara.utils.global_metadata import get_metadata_for_python_module_or_class


class PythonClassMetadata(MetadataModel):
    """Python class and module information."""

    _metadata_key: typing.ClassVar[str] = "python_class"

    @classmethod
    def from_class(cls, item_cls: typing.Type):

        conf: typing.Dict[str, typing.Any] = {
            "class_name": item_cls.__name__,
            "module_name": item_cls.__module__,
            "full_name": f"{item_cls.__module__}.{item_cls.__name__}",
        }
        return PythonClassMetadata(**conf)

    class_name: str = Field(description="The name of the Python class.")
    module_name: str = Field(
        description="The name of the Python module this class lives in."
    )
    full_name: str = Field(description="The full class namespace.")

    def get_id(self) -> str:
        return self.full_name

    def get_category_alias(self) -> str:
        return "metadata.python_class"

    def get_class(self) -> typing.Type:
        m = self.get_module()
        return getattr(m, self.class_name)

    def get_module(self) -> ModuleType:
        m = importlib.import_module(self.module_name)
        return m


class WrapperMetadataModel(MetadataModel):

    python_class: PythonClassMetadata = Field(
        description="Information about the Python class for this module type."
    )

    def get_id(self) -> str:
        return self.python_class.get_id()


class HashedMetadataModel(MetadataModel):

    _hash_cache: typing.Optional[str] = PrivateAttr(default=None)

    @abc.abstractmethod
    def _obj_to_hash(self) -> typing.Any:
        pass

    def get_id(self) -> str:
        return self.module_config_hash

    def get_category_alias(self) -> str:
        return "metadata"

    @property
    def module_config_hash(self):
        if self._hash_cache is not None:
            return self._hash_cache

        obj = self._obj_to_hash()
        h = DeepHash(obj, hasher=KIARA_HASH_FUNCTION)
        self._hash_cache = h[obj]
        return self._hash_cache


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


class ContextMetadataModel(HashedMetadataModel):
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

    def _obj_to_hash(self) -> typing.Any:
        return self.dict()

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
                link = f"[link={_v.url}]{_v.url}[/link]"
                references.append(f"[i]{_k}[/i]: {link}")
            table.add_row("References", "\n".join(references))

        return table

    def add_reference(
        self,
        ref_type: str,
        url: str,
        desc: typing.Optional[str] = None,
        force: bool = False,
    ):

        if ref_type in self.references.keys() and not force:
            raise Exception(f"Reference of type '{ref_type}' already present.")
        link = LinkModel(url=url, desc=desc)
        self.references[ref_type] = link

    def get_url_for_reference(self, ref: str) -> typing.Optional[str]:

        link = self.references.get(ref, None)
        if not link:
            return None

        return link.url


class DocumentationMetadataModel(HashedMetadataModel):

    _metadata_key = "documentation"

    @classmethod
    def from_class_doc(cls, item_cls: typing.Type):

        doc = item_cls.__doc__

        if not doc:
            doc = DEFAULT_NO_DESC_VALUE

        doc = inspect.cleandoc(doc)
        return cls.from_string(doc)

    @classmethod
    def from_string(cls, doc: typing.Optional[str]):

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
    def from_dict(cls, data: typing.Mapping):

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
            return DocumentationMetadataModel(description=desc, doc=doc)

    @classmethod
    def create(cls, item: typing.Any):

        if not item:
            return cls.from_string(DEFAULT_NO_DESC_VALUE)
        elif isinstance(item, DocumentationMetadataModel):
            return item
        elif isinstance(item, typing.Mapping):
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
    doc: typing.Optional[str] = Field(
        description="Detailed documentation of the item (in markdown).", default=None
    )

    def _obj_to_hash(self) -> typing.Any:
        return self.full_doc

    @property
    def full_doc(self):

        if self.doc:
            return f"{self.description}\n\n{self.doc}"
        else:
            return self.description

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        return Markdown(self.full_doc)


class OriginMetadataModel(HashedMetadataModel):

    _metadata_key = "origin"

    @classmethod
    def from_class(cls, item_cls: typing.Type):

        data = get_metadata_for_python_module_or_class(item_cls)  # type: ignore
        merged = merge_dicts(*data)
        return cls.parse_obj(merged)

    authors: typing.List[AuthorModel] = Field(
        description="The authors/creators of this item.", default_factory=list
    )

    def _obj_to_hash(self) -> typing.Any:
        return self.authors

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")

        authors = []
        for author in reversed(self.authors):
            if author.email:
                authors.append(f"{author.name} ({author.email})")
            else:
                authors.append(author.name)
        table.add_row("Authors", "\n".join(authors))

        return table


class MetadataModelMetadata(WrapperMetadataModel):
    @classmethod
    def from_model_class(cls, model_cls: typing.Type[MetadataModel]):

        origin_md = OriginMetadataModel.from_class(model_cls)
        doc = DocumentationMetadataModel.from_class_doc(model_cls)
        python_class = PythonClassMetadata.from_class(model_cls)
        properties_md = ContextMetadataModel.from_class(model_cls)

        return MetadataModelMetadata(
            type_name=model_cls._metadata_key,  # type: ignore
            documentation=doc,
            origin=origin_md,
            context=properties_md,
            python_class=python_class,
        )

    type_name: str = Field(description="The registered name for this value type.")
    documentation: DocumentationMetadataModel = Field(
        description="Documentation for the value type."
    )
    origin: OriginMetadataModel = Field(
        description="Information about the creator of this value type."
    )
    context: ContextMetadataModel = Field(
        description="Generic properties of this value type."
    )

    def get_category_alias(self) -> str:
        return "metadata.model"

    def create_fields_table(
        self, show_header: bool = True, show_required: bool = True
    ) -> Table:

        type_cls = self.python_class.get_class()

        fields_table = Table(show_header=show_header, box=box.SIMPLE)
        fields_table.add_column("Field name", style="i")
        fields_table.add_column("Type")
        if show_required:
            fields_table.add_column("Required")
        fields_table.add_column("Description")
        for field_name, details in type_cls.__fields__.items():
            field_type = type_cls.schema()["properties"][field_name]["type"]
            info = details.field_info.description
            if show_required:
                req = "yes" if details.required else "no"
                fields_table.add_row(field_name, field_type, req, info)
            else:
                fields_table.add_row(field_name, field_type, info)

        return fields_table

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        include_schema = config.get("display_schema", False)
        include_doc = config.get("include_doc", True)

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Property", style="i")
        table.add_column("Value")

        if include_doc:
            table.add_row(
                "Documentation",
                Panel(self.documentation.create_renderable(), box=box.SIMPLE),
            )
        table.add_row("Origin", self.origin.create_renderable())
        table.add_row("Context", self.context.create_renderable())

        table.add_row("Python class", self.python_class.create_renderable())

        fields_table = self.create_fields_table()
        table.add_row("Fields", fields_table)

        if include_schema:
            json_str = Syntax(
                self.python_class.get_class().schema_json(indent=2),
                "json",
                background_color="default",
            )
            table.add_row("Json Schema", json_str)

        return table
