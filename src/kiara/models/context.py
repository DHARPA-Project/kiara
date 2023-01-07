# -*- coding: utf-8 -*-
import humanfriendly
import uuid
from humanfriendly import format_size
from pydantic import BaseModel, Field, PrivateAttr
from rich import box
from rich.console import Group, RenderableType
from rich.markdown import Markdown
from rich.table import Table
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Union

from kiara.context import KiaraConfig, KiaraContextConfig, KiaraRuntimeConfig
from kiara.models import KiaraModel
from kiara.models.archives import ArchiveGroupInfo

if TYPE_CHECKING:
    from kiara.context import Kiara


class ContextInfo(KiaraModel):
    @classmethod
    def create_from_context_config(
        cls,
        config: KiaraContextConfig,
        context_name: Union[str, None] = None,
        runtime_config: Union[KiaraRuntimeConfig, None] = None,
    ):

        from kiara.context import Kiara

        kiara = Kiara(config=config, runtime_config=runtime_config)
        return cls.create_from_context(kiara=kiara, context_name=context_name)

    @classmethod
    def create_from_context(cls, kiara: "Kiara", context_name: Union[str, None] = None):

        errors = {}
        try:
            value_ids = list(kiara.data_registry.retrieve_all_available_value_ids())
        except Exception as e:
            errors["values"] = e
            value_ids = []

        try:
            aliases = {
                a.full_alias: a.value_id for a in kiara.alias_registry.aliases.values()
            }
        except Exception as e:
            errors["aliases"] = e
            aliases = {}

        try:
            archives_info = ArchiveGroupInfo.create_from_context(kiara=kiara)
        except Exception as e:
            errors["archives"] = e
            archives_info = ArchiveGroupInfo(item_infos={})

        comment = None
        invalid = False
        if errors:
            invalid = True
            comment = "Errors creating this context, this is most likely a bug:\n\n"
            for k, err in errors.items():
                comment = f"{comment}  - {k}: {err}"

        result = ContextInfo.construct(
            kiara_id=kiara.id,
            value_ids=value_ids,
            aliases=aliases,
            context_name=context_name,
            archives=archives_info,
            invalid=invalid,
            comment=comment,
        )
        result._kiara = kiara
        return result

    kiara_id: uuid.UUID = Field(
        description="The (globally unique) id of the kiara context."
    )
    context_name: Union[str, None] = Field(
        description="The local alias for this context."
    )
    value_ids: List[uuid.UUID] = Field(
        description="The ids of all stored values in this context."
    )
    aliases: Dict[str, uuid.UUID] = Field(
        description="All available aliases within this context (and the value ids they refer to)."
    )
    archives: ArchiveGroupInfo = Field(
        description="The archives registered in this context."
    )
    invalid: bool = Field(description="Whether this context has errors.", bool=False)
    comment: Union[str, None] = Field(
        description="(Optional) comment about this context.", default=None
    )

    _kiara: Union["Kiara", None] = PrivateAttr()

    @property
    def kiara_context(self) -> "Kiara":
        if self._kiara is None:
            raise Exception("Kiara context object not set.")
        return self._kiara

    def value_summary(self) -> Dict[str, Any]:

        sum_size = 0
        types: Dict[str, int] = {}
        internal_types: Dict[str, int] = {}
        no_of_values = len(self.value_ids)

        for value_id in self.value_ids:
            value = self.kiara_context.data_registry.get_value(value=value_id)
            sum_size = sum_size + value.value_size
            if self.kiara_context.type_registry.is_internal_type(value.data_type_name):
                if value.data_type_name not in internal_types.keys():
                    internal_types[value.data_type_name] = 1
                else:
                    internal_types[value.data_type_name] += 1
            else:
                if value.data_type_name not in types.keys():
                    types[value.data_type_name] = 1
                else:
                    types[value.data_type_name] += 1

            types.setdefault(value.data_type_name, 0)

        return {
            "size": sum_size,
            "no_values": no_of_values,
            "types": types,
            "internal_types": internal_types,
        }

    def alias_summary(self) -> Dict[str, Any]:

        sum_size = 0
        types: Dict[str, int] = {}
        internal_types: Dict[str, int] = {}
        no_of_values = len(self.value_ids)

        for alias, value_id in self.aliases.items():
            value = self.kiara_context.data_registry.get_value(value=value_id)
            sum_size = sum_size + value.value_size
            if self.kiara_context.type_registry.is_internal_type(value.data_type_name):
                if value.data_type_name not in internal_types.keys():
                    internal_types[value.data_type_name] = 1
                else:
                    internal_types[value.data_type_name] += 1
            else:
                if value.data_type_name not in types.keys():
                    types[value.data_type_name] = 1
                else:
                    types[value.data_type_name] += 1

            types.setdefault(value.data_type_name, 0)

        return {
            "size": sum_size,
            "no_values": no_of_values,
            "types": types,
            "internal_types": internal_types,
        }

    def create_renderable(self, **config: Any) -> RenderableType:

        full_details = config.get("full_details", False)
        show_value_ids = config.get("show_value_ids", False)
        show_archive_info = config.get("show_archive_info", True)

        table = Table(box=box.SIMPLE, show_header=False)

        table.add_column("Property", style="i")
        table.add_column("Value")

        if self.invalid:
            msg = "[red]This context is invalid![/red]\n"
            cmt = self.comment if self.comment else "-- no details --"
            md = Markdown(cmt)
            table.add_row(
                "",
                Group(msg, md, ""),
            )

        if self.context_name:
            table.add_row("context name", self.context_name)
        table.add_row("kiara_id", str(self.kiara_id))

        size_on_disk = humanfriendly.format_size(self.archives.combined_size)
        table.add_row("size on disk", size_on_disk)

        value_sum = self.value_summary()
        v_table = Table(box=box.SIMPLE, show_header=False)
        v_table.add_column("Property")
        v_table.add_column("Value")
        v_table.add_row("no. values", str(value_sum["no_values"]))
        v_table.add_row("combined size", format_size(value_sum["size"]))
        if full_details and show_value_ids:
            if self.value_ids:
                value_ids = sorted((str(v) for v in self.value_ids))
                v_table.add_row("value_ids", value_ids[0])
                for v_id in value_ids[1:]:
                    v_table.add_row("", v_id)
            else:
                v_table.add_row("value_ids", "")
        table.add_row("values", v_table)

        alias_sum = self.alias_summary()
        a_table = Table(box=box.SIMPLE, show_header=False)
        a_table.add_column("Property")
        a_table.add_column("Value")
        a_table.add_row("no. aliases", str(len(self.aliases)))
        a_table.add_row("combined size", format_size(alias_sum["size"]))
        if full_details:
            if self.aliases:
                aliases = sorted(self.aliases.keys())
                a_table.add_row(
                    "aliases", f"{aliases[0]} -> {self.aliases[aliases[0]]}"
                )
                for alias in aliases[1:]:
                    a_table.add_row("", f"{alias} -> {self.aliases[alias]}")
            else:
                a_table.add_row("aliases", "")
        table.add_row("aliases", a_table)

        if show_archive_info:
            table.add_row("archives", self.archives)

        return table


class ContextInfos(BaseModel):
    __root__: Dict[str, ContextInfo]

    @classmethod
    def create_context_infos(
        cls, contexts: Union[Mapping[str, "KiaraContextConfig"], None] = None
    ):

        if not contexts:
            kc = KiaraConfig()
            contexts = kc.context_configs

        return ContextInfos(
            __root__={
                a: ContextInfo.create_from_context_config(c, context_name=a)
                for a, c in contexts.items()
            }
        )

    def create_renderable(self, **config: Any) -> RenderableType:

        full_details = config.get("full_details", False)

        if not full_details:
            table = Table(box=box.SIMPLE, show_header=True, show_lines=False)
            table.add_column("context name", style="i")
            table.add_column("context id", style="i")
            table.add_column("size on disk")
            table.add_column("size of all values")
            table.add_column("no. values")
            table.add_column("no. aliaes")
            for context_name, context_summary in self.__root__.items():
                size_on_disk = context_summary.archives.combined_size
                value_summary = context_summary.value_summary()
                size = humanfriendly.format_size(value_summary["size"])
                no_values = str(value_summary["no_values"])
                no_aliases = str(len(context_summary.aliases))
                table.add_row(
                    context_name,
                    str(context_summary.kiara_id),
                    humanfriendly.format_size(size_on_disk),
                    size,
                    no_values,
                    no_aliases,
                )
        else:

            table = Table(box=box.MINIMAL, show_header=True, show_lines=True)
            table.add_column("context_name", style="i")
            table.add_column("details")

            for context_name, context_summary in self.__root__.items():

                table.add_row(context_name, context_summary.create_renderable(**config))

        return table
