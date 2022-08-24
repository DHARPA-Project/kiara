# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
# import orjson
# from pydantic.fields import Field, PrivateAttr
# from rich import box
# from rich.console import RenderableType
# from rich.markdown import Markdown
# from rich.panel import Panel
# from rich.syntax import Syntax
# from rich.table import Table
# from typing import TYPE_CHECKING, Any, List, Literal, Mapping, Type, Union
#
# from kiara.data_types import DataType
# from kiara.interfaces.python_api.models.info import TypeInfo
# from kiara.models.documentation import (
#     AuthorsMetadataModel,
#     ContextMetadataModel,
#     DocumentationMetadataModel,
# )
# # from kiara.models.info import TypeInfo, TypeInfoModelGroup
# from kiara.models.python_class import PythonClass
# from kiara.utils.json import orjson_dumps
#
# if TYPE_CHECKING:
#     from kiara.context import Kiara
#

# class DataTypeClassInfo(TypeInfo[DataType]):
#
#     _kiara_model_id = "info.data_type"
#
#     @classmethod
#     def create_from_type_class(
#         self, type_cls: Type[DataType], kiara: Union["Kiara", None] = None
#     ) -> "DataTypeClassInfo":
#
#         authors = AuthorsMetadataModel.from_class(type_cls)
#         doc = DocumentationMetadataModel.from_class_doc(type_cls)
#         properties_md = ContextMetadataModel.from_class(type_cls)
#
#         if kiara is not None:
#             qual_profiles = kiara.type_registry.get_associated_profiles(type_cls._data_type_name)  # type: ignore
#             lineage = kiara.type_registry.get_type_lineage(type_cls._data_type_name)  # type: ignore
#         else:
#             qual_profiles = None
#             lineage = None
#
#         try:
#             result = DataTypeClassInfo.construct(
#                 type_name=type_cls._data_type_name,  # type: ignore
#                 python_class=PythonClass.from_class(type_cls),
#                 value_cls=PythonClass.from_class(type_cls.python_class()),
#                 data_type_config_cls=PythonClass.from_class(
#                     type_cls.data_type_config_class()
#                 ),
#                 lineage=lineage,  # type: ignore
#                 qualifier_profiles=qual_profiles,
#                 documentation=doc,
#                 authors=authors,
#                 context=properties_md,
#             )
#         except Exception as e:
#             if isinstance(
#                 e, TypeError
#             ) and "missing 1 required positional argument: 'cls'" in str(e):
#                 raise Exception(
#                     f"Invalid implementation of TypeValue subclass '{type_cls.__name__}': 'python_class' method must be marked as a '@classmethod'. This is a bug."
#                 )
#             raise e
#
#         result._kiara = kiara
#         return result
#
#     @classmethod
#     def base_class(self) -> Type[DataType]:
#         return DataType
#
#     @classmethod
#     def category_name(cls) -> str:
#         return "data_type"
#
#     value_cls: PythonClass = Field(description="The python class of the value itself.")
#     data_type_config_cls: PythonClass = Field(
#         description="The python class holding the schema for configuring this type."
#     )
#     lineage: Union[List[str], None] = Field(description="This types lineage.")
#     qualifier_profiles: Union[Mapping[str, Mapping[str, Any]], None] = Field(
#         description="A map of qualifier profiles for this data types."
#     )
#     _kiara: Union["Kiara", None] = PrivateAttr(default=None)
#
#     def _retrieve_id(self) -> str:
#         return self.type_name
#
#     def _retrieve_data_to_hash(self) -> Any:
#         return self.type_name
#
#     def create_renderable(self, **config: Any) -> RenderableType:
#
#         include_doc = config.get("include_doc", True)
#
#         table = Table(box=box.SIMPLE, show_header=False, padding=(0, 0, 0, 0))
#         table.add_column("property", style="i")
#         table.add_column("value")
#
#         if self.lineage:
#             table.add_row("lineage", "\n".join(self.lineage[0:]))
#         else:
#             table.add_row("lineage", "-- n/a --")
#
#         if self.qualifier_profiles:
#             qual_table = Table(show_header=False, box=box.SIMPLE)
#             qual_table.add_column("name")
#             qual_table.add_column("config")
#             for name, details in self.qualifier_profiles.items():
#                 json_details = orjson_dumps(details, option=orjson.OPT_INDENT_2)
#                 qual_table.add_row(
#                     name, Syntax(json_details, "json", background_color="default")
#                 )
#             table.add_row("qualifier profile(s)", qual_table)
#         else:
#             table.add_row("qualifier profile(s)", "-- n/a --")
#
#         if include_doc:
#             table.add_row(
#                 "Documentation",
#                 Panel(self.documentation.create_renderable(), box=box.SIMPLE),
#             )
#
#         table.add_row("Author(s)", self.authors.create_renderable())
#         table.add_row("Context", self.context.create_renderable())
#
#         table.add_row("Python class", self.python_class.create_renderable())
#         table.add_row("Config class", self.data_type_config_cls.create_renderable())
#         table.add_row("Value class", self.value_cls.create_renderable())
#
#         return table


# class DataTypeClassesInfo(TypeInfoModelGroup):
#
#     _kiara_model_id = "info.data_types"
#
#     @classmethod
#     def create_from_type_items(
#         cls,
#         group_title: Union[str, None] = None,
#         **items: Type,
#     ) -> "TypeInfoModelGroup":
#
#         type_infos = {
#             k: cls.base_info_class().create_from_type_class(v) for k, v in items.items()  # type: ignore
#         }
#         data_types_info = cls.construct(group_alias=group_title, item_infos=type_infos)  # type: ignore
#         return data_types_info
#
#     @classmethod
#     def create_augmented_from_type_items(
#         cls,
#         kiara: Union["Kiara", None] = None,
#         group_alias: Union[str, None] = None,
#         **items: Type,
#     ) -> "TypeInfoModelGroup":
#
#         type_infos = {
#             k: cls.base_info_class().create_from_type_class(v, kiara=kiara) for k, v in items.items()  # type: ignore
#         }
#         data_types_info = cls.construct(group_alias=group_alias, item_infos=type_infos)  # type: ignore
#         data_types_info._kiara = kiara
#         return data_types_info
#
#     @classmethod
#     def base_info_class(cls) -> Type[DataTypeClassInfo]:
#         return DataTypeClassInfo
#
#     type_name: Literal["data_type"] = "data_type"
#     item_infos: Mapping[str, DataTypeClassInfo] = Field(
#         description="The data_type info instances for each type."
#     )
#     _kiara: Union["Kiara", None] = PrivateAttr(default=None)
#
#     def create_renderable(self, **config: Any) -> RenderableType:
#
#         full_doc = config.get("full_doc", False)
#         show_subtypes_inline = config.get("show_qualifier_profiles_inline", True)
#         show_lineage = config.get("show_type_lineage", True)
#
#         show_lines = full_doc or show_subtypes_inline or show_lineage
#
#         table = Table(show_header=True, box=box.SIMPLE, show_lines=show_lines)
#         table.add_column("type name", style="i")
#
#         if show_lineage:
#             table.add_column("type lineage")
#
#         if show_subtypes_inline:
#             table.add_column("(qualifier) profiles")
#
#         if full_doc:
#             table.add_column("documentation")
#         else:
#             table.add_column("description")
#
#         all_types = self.item_infos.keys()
#
#         for type_name in sorted(all_types):  # type: ignore
#
#             t_md = self.item_infos[type_name]  # type: ignore
#             row: List[Any] = [type_name]
#
#             if show_lineage:
#                 if self._kiara is None:
#                     lineage_str = "-- n/a --"
#                 else:
#                     lineage = list(
#                         self._kiara.type_registry.get_type_lineage(type_name)
#                     )
#                     lineage_str = ", ".join(reversed(lineage[1:]))
#                 row.append(lineage_str)
#             if show_subtypes_inline:
#                 if self._kiara is None:
#                     qual_profiles = "-- n/a --"
#                 else:
#                     qual_p = self._kiara.type_registry.get_associated_profiles(
#                         data_type_name=type_name
#                     ).keys()
#                     if qual_p:
#                         qual_profiles = "\n".join(qual_p)
#                     else:
#                         qual_profiles = "-- n/a --"
#                 row.append(qual_profiles)
#
#             if full_doc:
#                 md = Markdown(t_md.documentation.full_doc)
#             else:
#                 md = Markdown(t_md.documentation.description)
#             row.append(md)
#             table.add_row(*row)
#
#         return table
