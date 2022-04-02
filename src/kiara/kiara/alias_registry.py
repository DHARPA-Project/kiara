# -*- coding: utf-8 -*-
import structlog
from pydantic import Field, PrivateAttr, root_validator
from sqlalchemy import and_, func
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, aliased
from typing import Optional, TYPE_CHECKING

from kiara.models.aliases import AliasValueMap
from kiara.models.documentation import DocumentationMetadataModel

if TYPE_CHECKING:
    from kiara.kiara import Kiara

logger = structlog.getLogger()


class AliasRegistry(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara


class PersistentValueAliasMap(AliasValueMap):
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
