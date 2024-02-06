# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import copy
import datetime
import uuid
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Mapping, Union

import structlog
from pydantic import Field, PrivateAttr
from rich.tree import Tree

from kiara.defaults import NONE_VALUE_ID, SpecialValue
from kiara.models.values.value import Value, ValueMap
from kiara.models.values.value_schema import ValueSchema
from kiara.utils.cli import terminal_print

if TYPE_CHECKING:
    from kiara.context import DataRegistry


logger = structlog.getLogger()

VALUE_ALIAS_SEPARATOR = "."


class AliasValueMap(ValueMap):
    """A model class that holds a tree of values and their schemas."""

    _kiara_model_id: ClassVar = "instance.value_map.aliases"

    alias: Union[str, None] = Field(description="This maps own (full) alias.")
    version: int = Field(description="The version of this map (in this maps parent).")
    created: Union[datetime.datetime, None] = Field(
        description="The time this map was created.", default=None
    )
    assoc_schema: Union[ValueSchema, None] = Field(
        description="The schema for this maps associated value."
    )
    assoc_value: Union[uuid.UUID, None] = Field(
        description="The value that is associated with this map."
    )

    value_items: Dict[str, Dict[int, "AliasValueMap"]] = Field(
        description="The values contained in this set.", default_factory=dict
    )

    _data_registry: "DataRegistry" = PrivateAttr(default=None)
    _schema_locked: bool = PrivateAttr(default=False)
    _auto_schema: bool = PrivateAttr(default=True)
    _is_stored: bool = PrivateAttr(default=False)

    def _retrieve_data_to_hash(self) -> Any:
        raise NotImplementedError()

    @property
    def is_stored(self) -> bool:
        return self._is_stored

    def get_child_map(
        self, field_name: str, version: Union[str, None] = None
    ) -> Union["AliasValueMap", None]:
        """
        Get the child map for the specified field / version combination.

        Raises an error if the child field does not exist. Returns 'None' if not value is set yet (but schema is).
        """
        if version is not None:
            raise NotImplementedError()

        if VALUE_ALIAS_SEPARATOR not in field_name:

            if self.values_schema.get(field_name, None) is None:
                if not self.values_schema:
                    msg = "No available fields"
                else:
                    msg = "Available fields: " + ", ".join(self.values_schema.keys())
                raise KeyError(f"No field name '{field_name}'. {msg}")

            field_items = self.value_items[field_name]
            if not field_items:
                return None

            max_version = max(field_items.keys())

            item = field_items[max_version]
            return item

        else:
            child, rest = field_name.split(VALUE_ALIAS_SEPARATOR, maxsplit=1)
            if child not in self.values_schema.keys():
                raise Exception(
                    f"No field name '{child}'. Available fields: {', '.join(self.values_schema.keys())}"
                )
            child_map = self.get_child_map(child)
            assert child_map is not None
            return child_map.get_child_map(rest)

    def get_value_obj(self, field_name: str) -> Value:

        item = self.get_child_map(field_name=field_name)
        if item is None:
            return self._data_registry.NONE_VALUE
        if item.assoc_value is None:
            raise Exception(f"No value associated for field '{field_name}'.")

        return self._data_registry.get_value(value=item.assoc_value)

    def get_value_id(self, field_name: str) -> uuid.UUID:

        item = self.get_child_map(field_name=field_name)
        if item is None:
            result = NONE_VALUE_ID
        else:
            result = item.assoc_value if item.assoc_value is not None else NONE_VALUE_ID

        return result

    def get_all_value_ids(
        self,
    ) -> Dict[str, uuid.UUID]:

        result: Dict[str, uuid.UUID] = {}
        for k in self.values_schema.keys():
            v_id = self.get_value_id(field_name=k)
            if v_id is None:
                v_id = NONE_VALUE_ID
            result[k] = v_id
        return result

    def set_alias_schema(self, alias: str, schema: ValueSchema):

        if self._schema_locked:
            raise Exception(f"Can't add schema for alias '{alias}': schema locked.")

        if VALUE_ALIAS_SEPARATOR not in alias:

            self._set_local_field_schema(field_name=alias, schema=schema)
        else:
            child, rest = alias.split(VALUE_ALIAS_SEPARATOR, maxsplit=1)

            if child in self.values_schema.keys():
                child_map = self.get_child_map(child)
            else:
                self._set_local_field_schema(
                    field_name=child, schema=ValueSchema(type="none")
                )
                child_map = self._set_alias(alias=child, data=None)

            assert child_map is not None

            child_map.set_alias_schema(alias=rest, schema=schema)

    def _set_local_field_schema(self, field_name: str, schema: ValueSchema):

        assert field_name is not None
        if VALUE_ALIAS_SEPARATOR in field_name:
            raise Exception(
                f"Can't add schema, field name has alias separator in name: {field_name}. This is most likely a bug."
            )

        if field_name in self.values_schema.keys():
            raise Exception(
                f"Can't set alias schema for '{field_name}' to map: schema already set."
            )

        try:
            items = self.get_child_map(field_name)
            if items is not None:
                raise Exception(
                    f"Can't set schema for field '{field_name}': already at least one child set for this field."
                )
        except KeyError:
            pass

        self.values_schema[field_name] = schema
        self.value_items[field_name] = {}

    def get_alias(self, alias: str) -> Union["AliasValueMap", None]:

        if VALUE_ALIAS_SEPARATOR not in alias:
            if "@" in alias:
                raise NotImplementedError()

            child_map = self.get_child_map(alias)
            if child_map is None:
                return None

            return child_map

        else:
            child, rest = alias.split(VALUE_ALIAS_SEPARATOR, maxsplit=1)
            if "@" in child:
                raise NotImplementedError()

            child_map = self.get_child_map(field_name=child)

            if child_map is None:
                return None

            return child_map.get_alias(rest)

    def set_value(self, field_name: str, data: Any) -> None:

        assert VALUE_ALIAS_SEPARATOR not in field_name

        self._set_alias(alias=field_name, data=data)

    def _set_aliases(self, **aliases: Any) -> Mapping[str, "AliasValueMap"]:

        result = {}
        for k, v in aliases.items():
            r = self._set_alias(alias=k, data=v)
            result[k] = r

        return result

    def _set_alias(self, alias: str, data: Any) -> "AliasValueMap":

        if VALUE_ALIAS_SEPARATOR not in alias:
            field_name: Union[str, None] = alias

            # means we are setting the alias in this map
            assert field_name is not None

            vs = self.values_schema[alias]
            if vs.type == "none":
                assert data is None
                value_id = None
            else:

                if data in [None, SpecialValue.NO_VALUE, SpecialValue.NOT_SET]:
                    if vs.default:
                        if callable(vs.default):
                            data = vs.default()
                        else:
                            data = copy.deepcopy(vs.default)

                value = self._data_registry.register_data(data=data, schema=vs)
                value_id = value.value_id

            new_map = self._set_local_value_item(
                field_name=field_name, value_id=value_id
            )
            return new_map

        else:
            child, rest = alias.split(VALUE_ALIAS_SEPARATOR, maxsplit=1)
            field_name = None

            # means we are dealing with an intermediate alias map
            assert rest is not None
            assert child is not None
            assert field_name is None
            if child not in self.value_items.keys():
                if not self._auto_schema:
                    raise Exception(
                        f"Can't set alias '{alias}', no schema set for field: '{child}'."
                    )
                else:
                    self.set_alias_schema(alias=child, schema=ValueSchema(type="any"))

            field_item: Union[AliasValueMap, None] = None
            try:
                field_item = self.get_child_map(field_name=child)
            except KeyError:
                pass

            if self.alias:
                new_alias = f"{self.alias}.{child}"
            else:
                new_alias = child

            if field_item is None:
                new_version = 0
                schemas = {}
                self.value_items[child] = {}
            else:
                max_version = len(field_item.keys())
                new_version = max_version + 1
                assert field_item.alias == new_alias
                assert field_item.version == max_version
                schemas = field_item.values_schema

            new_map = AliasValueMap(
                alias=new_alias,
                version=new_version,
                assoc_schema=self.values_schema[child],
                assoc_value=None,
                values_schema=schemas,
            )
            new_map._data_registry = self._data_registry
            self.value_items[child][new_version] = new_map

            new_map._set_alias(alias=rest, data=data)

        return new_map

    def _set_local_value_item(
        self, field_name: str, value_id: Union[uuid.UUID, None] = None
    ) -> "AliasValueMap":

        assert VALUE_ALIAS_SEPARATOR not in field_name

        value: Union[Value, None] = None
        if value_id is not None:
            value = self._data_registry.get_value(value=value_id)
            assert value is not None
            assert value.value_id == value_id

        if field_name not in self.values_schema.keys():
            if not self._auto_schema:
                raise Exception(
                    f"Can't add value for field '{field_name}': field not in schema."
                )
            else:
                if value_id is None:
                    value_schema = ValueSchema(type="none")
                else:
                    value_schema = value.value_schema  # type: ignore
                self.set_alias_schema(alias=field_name, schema=value_schema)

        field_items = self.value_items.get(field_name, None)
        if not field_items:
            assert field_items is not None
            new_version = 0
            values_schema = {}
        else:
            max_version = max(field_items.keys())
            current_map = field_items[max_version]

            if value_id == current_map.assoc_value:
                logger.debug(
                    "set_field.skip",
                    value_id=None,
                    reason=f"Same value id: {value_id}",
                )
                return current_map

            # TODO: check schema
            new_version = max(field_items.keys()) + 1
            values_schema = current_map.values_schema

        if self.alias:
            new_alias = f"{self.alias}.{field_name}"
        else:
            new_alias = field_name
        new_map = AliasValueMap(
            alias=new_alias,
            version=new_version,
            assoc_schema=self.values_schema[field_name],
            assoc_value=value_id,
            values_schema=values_schema,
        )
        new_map._data_registry = self._data_registry
        self.value_items[field_name][new_version] = new_map
        return new_map

    def print_tree(self):

        t = self.get_tree("base")
        terminal_print(t)

    def get_tree(self, base_name: str) -> Tree:

        if self.assoc_schema:
            type_name = self.assoc_schema.type
        else:
            type_name = "none"

        if type_name == "none":
            type_str = ""
        else:
            type_str = f" ({type_name})"

        tree = Tree(f"{base_name}{type_str}")
        if self.assoc_value:
            data = tree.add("__data__")
            value = self._data_registry.get_value(self.assoc_value)
            data.add(str(value.data))

        for field_name, schema in self.values_schema.items():

            alias = self.get_alias(alias=field_name)
            if alias is not None:
                tree.add(alias.get_tree(base_name=field_name))
            else:
                if schema.type == "none":
                    type_str = ""
                else:
                    type_str = f" ({schema.type})"

                tree.add(f"{field_name}{type_str}")

        return tree

    def __repr__(self):

        return f"AliasMap(assoc_value={self.assoc_value}, field_names={self.value_items.keys()})"

    def __str__(self):
        return self.__repr__()
