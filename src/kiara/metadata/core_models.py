# -*- coding: utf-8 -*-
import importlib
import inspect
import typing
from pydantic import AnyUrl, BaseModel, EmailStr, Field
from rich import box
from rich.console import RenderableType
from rich.markdown import Markdown
from rich.table import Table
from types import ModuleType

from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.metadata import MetadataModel
from kiara.module_config import OperationConfig
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

    class_name: str = Field(description="The name of the Python class.")
    module_name: str = Field(
        description="The name of the Python module this class lives in."
    )
    full_name: str = Field(description="The full class namespace.")

    def get_class(self) -> typing.Type:
        m = self.get_module()
        return getattr(m, self.class_name)

    def get_module(self) -> ModuleType:
        m = importlib.import_module(self.module_name)
        return m


# class HashMetadata(MetadataModel):
#
#     hash: str = Field(description="The hash for the value.")
#     hash_desc: typing.Optional[str] = Field(
#         description="A description how the hash was calculated and other details.",
#         default=None,
#     )


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


class MetadataModelMetadata(MetadataModel):
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
    python_class: PythonClassMetadata = Field(
        description="The Python class for this value type."
    )


class ValueHash(BaseModel):

    hash: str = Field(description="The value hash.")
    hash_type: str = Field(description="The value hash method.")


class ValueInfo(BaseModel):

    type: str = Field(description="The value type.")
    hashes: typing.Dict[str, ValueHash] = Field(
        description="All available hashes for this value."
    )


class SnapshotMetadata(BaseModel):

    value_type: str = Field(description="The value type.")
    value_id: str = Field(description="The value id after the snapshot.")
    value_id_orig: str = Field(description="The value id before the snapshot.")
    snapshot_time: str = Field(description="The time the data was saved.")


class LoadConfig(OperationConfig):

    value_id: str = Field(description="The id of the value.")
    base_path_input_name: str = Field(
        description="The base path where the value is stored.", default="base_path"
    )
    inputs: typing.Dict[str, typing.Any] = Field(
        description="The inputs to use when running this module.", default_factory=dict
    )
    output_name: str = Field(description="The name of the output field for the value.")


class SaveConfig(OperationConfig):

    inputs: typing.Dict[str, typing.Any] = Field(
        description="The inputs to use when running this module.", default_factory=dict
    )
    load_config_output: str = Field(
        description="The output name that will contain the load config output value."
    )
