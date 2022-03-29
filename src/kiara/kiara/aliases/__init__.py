# -*- coding: utf-8 -*-
import datetime
import structlog
import uuid
from pydantic import Field, PrivateAttr, root_validator
from rich.tree import Tree
from sqlalchemy import and_, func
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, aliased
from typing import Any, Dict, Mapping, Optional, TYPE_CHECKING

from kiara.defaults import VALUES_CATEGORY_ID
from kiara.kiara.aliases.aliases_orm import AliasOrm
from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.values.value import Value, ValueSet
from kiara.models.values.value_schema import ValueSchema
from kiara.utils import rich_print

if TYPE_CHECKING:
    from kiara.kiara import DataRegistry


logger = structlog.getLogger()

VALUE_ALIAS_SEPARATOR = "."


class AliasValueMap(ValueSet):

    alias: Optional[str] = Field(description="This maps own (full) alias.")
    version: int = Field(description="The version of this map (in this maps parent).")
    created: Optional[datetime.datetime] = Field(
        description="The time this map was created."
    )
    assoc_schema: Optional[ValueSchema] = Field(
        description="The schema for this maps associated value."
    )
    assoc_value: Optional[uuid.UUID] = Field(
        description="The value that is associated with this map."
    )

    value_items: Dict[str, Dict[int, "AliasValueMap"]] = Field(
        description="The values contained in this set.", default_factory=dict
    )

    _data_registry: "DataRegistry" = PrivateAttr(default=None)
    _schema_locked: bool = PrivateAttr(default=False)
    _auto_schema: bool = PrivateAttr(default=True)
    _is_stored: bool = PrivateAttr(default=False)

    def _retrieve_id(self) -> str:
        return str(uuid.uuid4())

    def _retrieve_category_id(self) -> str:
        return VALUES_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        raise NotImplementedError()

    @property
    def is_stored(self) -> bool:
        return self._is_stored

    def get_child_map(
        self, field_name: str, version: Optional[str] = None
    ) -> Optional["AliasValueMap"]:
        """Get the child map for the specified field / version combination.

        Raises an error if the child field does not exist. Returns 'None' if not value is set yet (but schema is).
        """

        if version is not None:
            raise NotImplementedError()

        if VALUE_ALIAS_SEPARATOR not in field_name:

            if self.values_schema.get(field_name, None) is None:
                raise KeyError(
                    f"No field name '{field_name}'. Available fields: {', '.join(self.values_schema.keys())}"
                )

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
            return child_map.get_child_map(rest)

    def get_value_obj(self, field_name: str) -> Value:

        item = self.get_child_map(field_name=field_name)
        if item.assoc_value is None:
            raise Exception(f"No value associated for field '{field_name}'.")

        return self._data_registry.get_value(value=item.assoc_value)

    def get_value_id(self, field_name: str) -> Optional[uuid.UUID]:

        item = self.get_child_map(field_name=field_name)
        if item is None:
            return item
        else:
            return item.assoc_value

    def get_all_value_ids(
        self,
    ) -> Dict[str, Optional[uuid.UUID]]:

        result = {}
        for k in self.values_schema.keys():
            v_id = self.get_value_id(field_name=k)
            result[k] = v_id
        return result

    def set_value(self, field_name: str, data: Any) -> None:

        assert VALUE_ALIAS_SEPARATOR not in field_name

        value = self._data_registry.register_data(data)
        self.set_alias(alias=field_name, value_id=value.value_id)

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
                child_map = self.set_alias(alias=child, value_id=None)

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

    def get_alias(self, alias: str) -> Optional["AliasValueMap"]:

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

    def set_aliases(self, **aliases) -> Mapping[str, "AliasValueMap"]:

        result = {}
        for k, v in aliases.items():
            r = self.set_alias(alias=k, value_id=v)
            result[k] = r

        return result

    def set_alias(self, alias: str, value_id: Optional[uuid.UUID]) -> "AliasValueMap":

        if VALUE_ALIAS_SEPARATOR not in alias:
            child = None
            field_name = alias
            rest = None
        else:
            child, rest = alias.split(VALUE_ALIAS_SEPARATOR, maxsplit=1)
            field_name = None

        if child is None:
            # means we are setting the alias in this map
            assert field_name is not None
            new_map = self._set_local_value_item(
                field_name=field_name, value_id=value_id
            )
            return new_map
        else:
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
                    print(f"ADDING: {child}")
                    self.set_alias_schema(alias=child, schema=ValueSchema(type="any"))

            field_item = None
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
                max_version = max(field_item.keys())
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

            new_map.set_alias(alias=rest, value_id=value_id)

        return new_map

    def _set_local_value_item(
        self, field_name: str, value_id: Optional[uuid.UUID] = None
    ) -> "AliasValueMap":

        assert VALUE_ALIAS_SEPARATOR not in field_name

        value: Optional[Value] = None
        if value_id is not None:
            value = self._data_registry.get_value(value=value_id)
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
                    value_schema = value.value_schema
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
        rich_print(t)

    def get_tree(self, base_name: str) -> Tree:

        dbg(self.__dict__)
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


# class AliasMap(object):
#     def __init__(
#         self,
#         alias: str,
#         version: int,
#         value: Optional[Value],
#         values_schema: Optional[Mapping[str, ValueSchema]] = None,
#         is_stored: bool = False,
#     ):
#
#         self._alias: str = alias
#         self._version: int = version
#         self._is_stored: bool = is_stored
#         self._value: Optional[Value] = value
#         self._created: datetime.datetime = datetime.datetime.now(
#             tz=datetime.timezone.utc
#         )
#
#         self._schemas: Dict[str, Optional[ValueSchema]] = {}
#
#         self._aliases: Dict[str, Dict[int, AliasMap]] = {}
#         self._schema_locked: bool = False
#         self._schema_autoadd: bool = True
#
#         if values_schema:
#             for field_name, schema in values_schema.items():
#                 self.set_alias_schema(alias=field_name, schema=schema)
#
#     @property
#     def value(self) -> Optional[Value]:
#         return self._value
#
#     @property
#     def is_stored(self) -> bool:
#         return self._is_stored
#
#     @property
#     def current_field_names(self) -> Iterable[str]:
#         return self._schemas.keys()
#
#     @property
#     def created(self) -> datetime.datetime:
#         return self._created
#
#     def latest_version_nr(self, field_name: str) -> int:
#
#         alias = self._aliases.get(field_name, None)
#         if not alias:
#             return -1
#         return max(alias.keys())
#
#     @property
#     def data(self) -> Any:
#         if self._value is None:
#             return None
#         else:
#             return self._value.data
#
#     def get_alias(self, alias: str) -> "AliasMap":
#
#         if "." in alias:
#             child, rest = alias.split(".", maxsplit=1)
#         else:
#             child = alias
#             rest = None
#
#         if "@" in child:
#             raise NotImplementedError()
#
#         child_map = self._aliases[child]
#         child_version = child_map[max(child_map.keys())]
#         if rest is None:
#             return child_version
#         else:
#             return child_version.get_alias(rest)
#
#     def set_alias_schema(self, alias: str, schema: Optional[ValueSchema] = None):
#
#         if self._schema_locked:
#             raise Exception(f"Can't add schema for alias '{alias}': schema locked.")
#
#         if "." not in alias:
#             child = None
#             field_name = alias
#             rest = None
#         else:
#             child, rest = alias.split(".", maxsplit=1)
#             field_name = None
#
#         if child is None:
#             assert field_name is not None
#
#             if alias in self._schemas.keys():
#                 raise Exception(
#                     f"Can't set alias schema for '{alias}' to map: schema already set."
#                 )
#             self._schemas[alias] = schema
#         else:
#             assert rest is not None
#             if child not in self._aliases.keys():
#                 self._set_local_field_alias(field_name=child, value=None)
#             child_maps = self._aliases[child]
#             max_version = max(child_maps.keys())
#             child_maps[max_version].set_alias_schema(alias=rest, schema=schema)
#
#     def set_alias(self, alias: str, value: Value):
#
#         if "." not in alias:
#             child = None
#             field_name = alias
#             rest = None
#         else:
#             child, rest = alias.split(".", maxsplit=1)
#             field_name = None
#
#         if child is None:
#             assert field_name is not None
#             self._set_local_field_alias(field_name=field_name, value=value)
#         else:
#             assert rest is not None
#             if child not in self._aliases.keys():
#                 self._set_local_field_alias(field_name=child, value=None)
#             max_version = max(self._aliases[child].keys())
#             self._aliases[child][max_version].set_alias(alias=rest, value=value)
#
#
#
#     def _set_local_field_alias(self, field_name: str, value: Optional[Value] = None) -> "AliasMap":
#
#         if not field_name in self._schemas.keys():
#             if not self._schema_autoadd:
#                 raise Exception(
#                     f"Invalid field name '{field_name}', allowed names: {', '.join(self._schemas.keys())}"
#                 )
#
#             self.set_alias_schema(alias=field_name)
#
#         field_items = self._aliases.setdefault(field_name, {})
#         if field_items:
#             max_version = max(field_items.keys())
#             current = field_items[max_version]
#
#             if value is None:
#                 if current is None:
#                     logger.debug(
#                         "set_field.skip",
#                         value_id=None,
#                         reason="last value was also 'None'",
#                     )
#                     return current
#
#             if current.value is not None:
#                 if value is not None and current.value.value_id == value.value_id:
#                     logger.debug(
#                         "set_field.skip",
#                         value_id=value.value_id,
#                         reason="identical to last value",
#                     )
#                     return current
#
#         if (
#             self._schemas[field_name]
#             and value.value_schema.type != self._schemas[field_name].type  # type: ignore
#         ):
#             raise Exception(
#                 f"Invalid value type '{value.value_schema.type}': must be '{self._schemas[field_name].type}'"  # type: ignore
#             )
#
#         if not field_items:
#             version = 1
#         else:
#             version = max(field_items.keys()) + 1
#         alias = f"{self._alias}.{field_name}"
#         new_map = AliasMap(alias=alias, version=version, value=value)
#         field_items[version] = new_map
#
#         return new_map
#
#     def print_tree(self):
#
#         t = self.get_tree("base")
#         rich_print(t)
#
#     def get_tree(self, base_name: str) -> Tree:
#
#         tree = Tree(base_name)
#         if self.value:
#             data = tree.add("__data__")
#             data.add(self.value.render_data())
#
#         if self._aliases:
#
#             for field_name, aliases in self._aliases.items():
#                 max_version = max(aliases.keys())
#                 alias = aliases[max_version]
#                 tree.add(alias.get_tree(base_name=field_name))
#
#         return tree
#
#     def __repr__(self):
#
#         return f"AliasMap(value={self.value}, field_names={self._aliases.keys()})"
#
#     def __str__(self):
#         return self.__repr__()


class AliasRegistry(AliasValueMap):
    # def __init__(self, data_registry: "DataRegistry", engine: Engine, doc: Any = None):
    #
    #     self._data_registry: DataRegistry = data_registry
    #     self._engine: Engine = engine
    #     doc = DocumentationMetadataModel.create(doc)
    #     v_doc = self._data_registry.register_data(
    #         doc, schema=ValueSchema(type="doc"), pedigree=ORPHAN
    #     )
    #     super().__init__(alias="", version=0, value=v_doc)
    #
    #     self._load_all_aliases()
    doc: Optional[DocumentationMetadataModel] = Field(
        description="Description of the values this map contains."
    )
    _engine: Engine = PrivateAttr(default=None)

    @root_validator(pre=True)
    def _fill_defaults(cls, values):
        if "values_schema" not in values.keys():
            values["values_schema"] = {}

        if "version" not in values.keys():
            values["version"] = 0
        else:
            assert values["version"] == 0

        return values

    def _load_all_aliases(self):

        with Session(bind=self._engine, future=True) as session:  # type: ignore

            alias_a = aliased(AliasOrm)
            alias_b = aliased(AliasOrm)

            result = (
                session.query(alias_b)
                .join(
                    alias_a,
                    and_(
                        alias_a.alias == alias_b.alias,
                        alias_a.version < alias_b.version,
                    ),
                )
                .where(alias_b.value_id != None)
                .order_by(func.length(alias_b.alias), alias_b.alias)
            )

            for r in result:
                value = self._data_registry.get_value(r.value_id)
                self.set_alias(r.alias, value=value)

    def save(self, *aliases):

        for alias in aliases:
            print(f"ALIAS: {alias}")
            self._persist(alias)

    def _persist(self, alias: str):

        return

        with Session(bind=self._engine, future=True) as session:  # type: ignore

            current = []
            tokens = alias.split(".")
            for token in tokens:
                current.append(token)
                current_path = ".".join(current)
                alias_map = self.get_alias(current_path)
                if alias_map.is_stored:
                    continue

                value_id = None
                if alias_map.assoc_value:
                    value_id = alias_map.assoc_value

                if value_id is None:
                    continue
                alias_map_orm = AliasOrm(
                    value_id=value_id,
                    created=alias_map.created,
                    version=alias_map.version,
                    alias=current_path,
                )
                session.add(alias_map_orm)

            session.commit()
