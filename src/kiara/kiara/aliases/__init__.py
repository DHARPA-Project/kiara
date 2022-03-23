import datetime
from typing import Optional, Mapping, Dict, List, Any, TYPE_CHECKING, Iterable

import structlog
from rich.tree import Tree
from sqlalchemy import desc, and_, func
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, aliased

from kiara.kiara.aliases.aliases_orm import AliasOrm
from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.values.value import Value, ORPHAN
from kiara.models.values.value_schema import ValueSchema
from kiara.utils.output import rich_print

if TYPE_CHECKING:
    from kiara.kiara import DataRegistry


logger = structlog.getLogger()


class AliasMap(object):

    def __init__(self, alias: str, version: int, value: Optional[Value], values_schema: Optional[Mapping[str, ValueSchema]]=None, is_stored: bool=False):

        self._alias: str = alias
        self._version: int = version
        self._is_stored: Optional[bool] = is_stored
        self._value: Optional[Value] = value
        self._created: datetime.datetime = datetime.datetime.now(tz=datetime.timezone.utc)

        self._schemas: Dict[str, Optional[ValueSchema]] = {}

        self._aliases: Dict[str, Dict[int, AliasMap]] = {}
        self._schema_locked: bool = False
        self._schema_autoadd: bool = True

        if values_schema:
            for field_name, schema in values_schema.items():
                self.add_field(field_name=field_name, schema=schema)

    @property
    def value(self) -> Optional[Value]:
        return self._value

    @property
    def is_stored(self) -> bool:
        return self._is_stored

    @property
    def current_field_names(self) -> Iterable[str]:
        return self._schemas.keys()

    @property
    def created(self) -> datetime.datetime:
        return self._created

    def latest_version_nr(self, field_name: str) -> int:

        alias = self._aliases.get(field_name, None)
        if not alias:
            return -1
        return max(alias.keys())

    @property
    def data(self) -> Any:
        if self._value is None:
            return None
        else:
            return self.value.data

    def get_alias(self, alias: str) -> "AliasMap":

        if "." in alias:
            child, rest = alias.split(".", maxsplit=1)
        else:
            child = alias
            rest = None

        if "@" in child:
            raise NotImplementedError()

        child_map = self._aliases[child]
        child = child_map[max(child_map.keys())]
        if rest is None:
            return child
        else:
            return child.get_alias(rest)

    def set_alias(self, alias: str, value: Value):

        if "." not in alias:
            child = None
            field_name = alias
            rest = None
        else:
            child, rest = alias.split(".", maxsplit=1)
            field_name = None

        if child is None:
            self.set_field_value(field_name=field_name, value=value)
        else:
            if child not in self._aliases.keys():
                self.set_field_value(field_name=child, value=None)
            max_version = max(self._aliases[child].keys())
            self._aliases[child][max_version].set_alias(alias=rest, value=value)


    def add_field(self, field_name: str, schema: Optional[ValueSchema]=None):

        if self._schema_locked:
            raise Exception(f"Can't add field '{field_name}': schema locked.")
        if field_name in self._schemas.keys():
            raise Exception(f"Can't add field '{field_name}' to map: field already exists.")

        if "." in field_name:
            raise Exception(f"Can't add field '{field_name}': invalid field name, '.' characters not allowed in name.")

        self._schemas[field_name] = schema

    def set_field_value(self, field_name: str, value: Optional[Value]=None):

        if not field_name in self._schemas.keys():
            if not self._schema_autoadd:
                raise Exception(f"Invalid field name '{field_name}', allowed names: {', '.join(self._schemas.keys())}")

            self.add_field(field_name=field_name)

        field_items = self._aliases.setdefault(field_name, {})
        if field_items:
            max_version = max(field_items.keys())
            current = field_items[max_version]

            if value is None:
                if current is None:
                    logger.debug("set_field.skip", value_id=None, reason="last value was also 'None'")
                    return
            if current.value is not None:
                if current.value.value_id == value.value_id:
                    logger.debug("set_field.skip", value_id=value.value_id, reason="identical to last value")
                    return

        if self._schemas[field_name] and value.value_schema.type != self._schemas[field_name].type:
            raise Exception(
                f"Invalid value type '{value.value_schema.type}': must be '{self._schemas[field_name].type}'")

        if not field_items:
            version = 1
        else:
            version = max(field_items.keys()) + 1
        alias = f"{self._alias}.{field_name}"
        new_map = AliasMap(alias=alias, version=version, value=value)
        field_items[version] = new_map

    def print_tree(self):

        t = self.get_tree("base")
        rich_print(t)

    def get_tree(self, base_name: str) -> Tree:

        tree = Tree(base_name)
        if self.value:
            data = tree.add("data")
            data.add(self.value.render_data())

        if self._aliases:
            childs = tree.add("childs")

            for field_name, aliases in self._aliases.items():
                max_version = max(aliases.keys())
                alias = aliases[max_version]
                childs.add(alias.get_tree(base_name=field_name))

        return tree


    def __repr__(self):

        return f"AliasMap(value={self.value}, field_names={self._aliases.keys()})"

    def __str__(self):
        return self.__repr__()


class AliasRegistry(AliasMap):

    def __init__(self, data_registry: "DataRegistry", engine: Engine, doc: Any=None):

        self._data_registry: DataRegistry = data_registry
        self._engine: Engine = engine
        doc = DocumentationMetadataModel.create(doc)
        v_doc = self._data_registry.register_data(doc, schema=ValueSchema(type="doc"), pedigree=ORPHAN)
        super().__init__(alias="", version=0, value=v_doc)

        self._load_all_aliases()

    def _load_all_aliases(self):

        with Session(bind=self._engine, future=True) as session:

            alias_a = aliased(AliasOrm)
            alias_b = aliased(AliasOrm)


            result = session.query(
                alias_b
            ).join(
                alias_a, and_(
                    alias_a.alias == alias_b.alias,
                    alias_a.version < alias_b.version
                )
            ).where(alias_b.value_id != None).order_by(func.length(alias_b.alias), alias_b.alias)

            for r in result:
                value = self._data_registry.get_value(r.value_id)
                self.set_alias(r.alias, value=value)

    def save(self, *aliases):

        for alias in aliases:
            print(f"ALIAS: {alias}")
            self._persist(alias)

    def _persist(self, alias: str):

        with Session(bind=self._engine, future=True) as session:

            current = []
            tokens = alias.split(".")
            for token in tokens:
                current.append(token)
                current_path = ".".join(current)
                alias_map = self.get_alias(current_path)
                if alias_map.is_stored:
                    continue

                value_id = None
                if alias_map.value:
                    value_id = alias_map.value.value_id

                if value_id is None:
                    continue
                alias_map_orm = AliasOrm(value_id=value_id, created=alias_map.created, version=alias_map._version, alias=current_path)
                session.add(alias_map_orm)

            session.commit()

