# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


"""A module that contains value-related classes for *Kiara*.

A value in Kiara-speak is a pointer to actual data (aka 'bytes'). It contains metadata about that data (like whether it's
valid/set, what type/schema it has, when it was last modified, ...), but it does not contain the data itself. The reason for
that is that such data can be fairly large, and in a lot of cases it is not necessary for the code involved to have
access to it, access to the metadata is enough.

Each ValueOrm has a unique id, which can be used to retrieve the data (whole, or parts of it) from a [DataRegistry][kiara.data.registry.DataRegistry].
In addition, that id can be used to subscribe to change events for a value (published whenever the data that is associated with a value was changed).
"""

# import json
# from pydantic import Field
# from rich import box
# from rich.console import RenderableType
# from rich.syntax import Syntax
# from rich.table import Table
# from typing import Any, Dict, Iterable, List
#
# from kiara.models import KiaraModel
# from kiara.models.values.value import Value, yaml
# from kiara.models.values.value_schema import ValueSchema

# NO_ID_YET_MARKER = "__no_id_yet__"


# class ValueInfo(KiaraModel):
#     @classmethod
#     def from_value(cls, value: Value, include_deserialization_config: bool = False):
#
#         if value.id not in value._registry.value_ids:
#             raise Exception("ValueOrm not registered (yet).")
#
#         # aliases = value._registry.find_aliases_for_value(value)
#         # hashes = value.get_hashes()
#         hashes = []
#         metadata = value.get_metadata(also_return_schema=True)
#         # metadata = value.metadata
#         value_lineage = value.get_lineage()
#
#         if include_deserialization_config:
#             # serialize_operation: SerializeValueOperationType = (  # type: ignore
#             #     value._kiara.operation_mgmt.get_operation("serialize")  # type: ignore
#             # )
#             raise NotImplementedError()
#
#         return ValueInfo(
#             value_id=value.id,
#             value_schema=value.value_schema,
#             hashes=hashes,
#             metadata=metadata,
#             lineage=value_lineage,
#             is_valid=value.item_is_valid(),
#         )
#
#     def get_id(self):
#         return self.value_id
#
#     def get_category_alias(self) -> str:
#         return "value"
#
#     value_id: str = Field(description="The value id.")
#     value_schema: ValueSchema = Field(description="The value schema.")
#     # aliases: typing.List[ValueAlias] = Field(
#     #     description="All aliases for this value.", default_factory=list
#     # )
#     # tags: typing.List[str] = Field(
#     #     description="All tags for this value.", default_factory=list
#     # )
#     # created: str = Field(description="The time the data was created.")
#     is_valid: bool = Field(
#         description="Whether the item is valid (in the context of its schema)."
#     )
#     hashes: List[ValueHash] = Field(
#         description="All available hashes for this value.", default_factory=list
#     )
#     metadata: Dict[str, Dict[str, Any]] = Field(
#         description="The metadata associated with this value."
#     )
#
#     def get_metadata_items(self, *keys: str) -> Dict[str, Any]:
#
#         if not keys:
#             _keys: Iterable[str] = self.metadata.keys()
#         else:
#             _keys = keys
#
#         result = {}
#         for k in _keys:
#
#             md = self.metadata.get(k)
#             if md is None:
#                 raise Exception(f"No metadata for key '{k}' available.")
#
#             result[k] = md["metadata_item"]
#
#         return result
#
#     def get_metadata_schemas(self, *keys: str) -> Dict[str, Any]:
#
#         if not keys:
#             _keys: Iterable[str] = self.metadata.keys()
#         else:
#             _keys = keys
#
#         result = {}
#         for k in _keys:
#             md = self.metadata.get(k)
#             if md is None:
#                 raise Exception(f"No metadata for key '{k}' available.")
#             result[k] = md["metadata_schema"]
#
#         return result
#
#     def create_renderable(self, **render_config: Any) -> RenderableType:
#
#         padding = render_config.get("padding", (0, 1))
#         skip_metadata = render_config.get("skip_metadata", False)
#         skip_value_lineage = render_config.get("skip_lineage", True)
#         include_ids = render_config.get("include_ids", False)
#
#         table = Table(box=box.SIMPLE, show_header=False, padding=padding)
#         table.add_column("property", style="i")
#         table.add_column("value")
#
#         table.add_row("id", self.value_id)  # type: ignore
#         table.add_row("type", self.value_schema.type)
#         if self.value_schema.type_config:
#             json_data = json.dumps(self.value_schema.type_config)
#             tc_content = Syntax(json_data, "json")
#             table.add_row("type render_config", tc_content)
#         table.add_row("desc", self.value_schema.doc)
#         table.add_row("is set", "yes" if self.is_valid else "no")
#         # table.add_row("is constant", "yes" if self.is_constant else "no")
#
#         # if isinstance(self.value_hash, int):
#         #     vh = str(self.value_hash)
#         # else:
#         #     vh = self.value_hash.value
#         # table.add_row("hash", vh)
#
#         if self.hashes:
#             hashes_dict = {hs.hash_type: hs.hash for hs in self.hashes}
#             yaml_string = yaml.dump(hashes_dict)
#             hases_str = Syntax(yaml_string, "yaml", background_color="default")
#             table.add_row("", "")
#             table.add_row("hashes", hases_str)
#
#         if not skip_metadata:
#             if self.metadata:
#                 yaml_string = yaml.dump(data=self.get_metadata_items())
#                 # json_string = json.dumps(self.get_metadata_items(), indent=2)
#                 metadata = Syntax(yaml_string, "yaml", background_color="default")
#                 table.add_row("metadata", metadata)
#             else:
#                 table.add_row("metadata", "-- no metadata --")
#
#         if not skip_value_lineage and self.lineage:
#             if self.metadata:
#                 table.add_row("", "")
#             # json_string = self.lineage.json(indent=2)
#             # seed_content = Syntax(json_string, "json")
#             table.add_row(
#                 "lineage", self.lineage.create_renderable(include_ids=include_ids)
#             )
#
#         return table
