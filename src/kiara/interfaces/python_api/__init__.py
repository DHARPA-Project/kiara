# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import structlog
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

logger = structlog.getLogger()


# class ValueResolver(object):
#
#     def __init__(self, kiara: "Kiara"):
#
#         self._kiara: Kiara = kiara
#
#     def resolve(self, alias: str, schema: Optional[ValueSchema]) -> Value:
#
#         if ":" not in alias:
#             raise Exception(
#                 f"Can't retrieve value for '{alias}': can't determine reference type."
#             )
#
#         ref_type, rest = alias.split(":", maxsplit=1)
#
#         if ref_type == "value":
#             _value_id = uuid.UUID(rest)
#         elif ref_type == "alias":
#             _value_id = self._kiara.alias_registry.find_value_id_for_alias(
#                 alias=rest
#             )
#             if _value_id is None:
#                 raise NoSuchValueAliasException(
#                     alias=rest,
#                     msg=f"Can't retrive value for alias '{rest}': no such alias registered.",
#                 )
#         else:
#             raise Exception(
#                 f"Can't retrieve value for alias '{alias}': invalid reference type '{ref_type}'."
#             )
#
#
# class KiaraContext(object):
#
#     def __init__(self, kiara: "Kiara"):
#
#         self._kiara: Kiara = kiara
#         self._value_resolver: ValueResolver = ValueResolver(kiara=kiara)
#
#     def get_value(self, value: Union[str, uuid.UUID, Value], schema: Optional[ValueSchema]=None) -> Value:
#
#         if isinstance(value, (uuid.UUID, Value)):
#             return self._kiara.data_registry.get_value(value_id=value)
#
#         try:
#             _value_id = uuid.UUID(
#                 value_id  # type: ignore
#             )  # this should fail if not string or wrong string format
#             return self._kiara.data_registry.get_value(value_id=_value_id)
#         except ValueError:
#             _value_id = None
#
#         if not isinstance(value, str):
#             raise Exception(
#                 f"Can't retrieve value for '{value}': invalid type '{type(value)}'."
#             )
#
#         return self._value_resolver.resolve(alias=value, schema=schema)
#
#
#
#
#     def save_values(
#         self, values: ValueMap, alias_map: Mapping[str, Iterable[str]]
#     ) -> StoreValuesResult:
#
#         return self._kiara.save_values(values=values, alias_map=alias_map)
