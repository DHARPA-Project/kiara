# -*- coding: utf-8 -*-
import uuid
from pydantic import BaseModel, Field, PrivateAttr
from rich import box
from rich.console import RenderableType
from rich.table import Table
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Union

from kiara.models.module.persistence import LoadConfig
from kiara.models.values.value import ORPHAN, Value, ValuePedigree
from kiara.models.values.value_schema import ValueSchema
from kiara.utils import is_debug, log_message
from kiara.utils.output import create_table_from_model_object

if TYPE_CHECKING:
    from kiara.kiara import Kiara


def render_value_data(value: Value):

    try:
        renderable = value._data_registry.render_data(
            value.value_id, target_type="terminal_renderable"
        )
    except Exception as e:

        if is_debug():
            import traceback

            traceback.print_exc()
        log_message("error.render_value", value=value.value_id, error=e)
        renderable = [str(value.data)]

    return renderable


RENDER_FIELDS = {
    "value_id": {"show_default": True, "render": {"terminal": str}},
    "aliases": {"show_default": True, "render": {"terminal": lambda x: ", ".join(x)}},
    "type": {
        "show_default": True,
        "render": {"terminal": lambda x: x.value_schema.type},
    },
    "value_schema": {"show_default": False},
    "is_stored": {
        "show_default": False,
        "render": {"terminal": lambda x: "yes" if x else "no"},
    },
    "data": {"show_default": False, "render": {"terminal": render_value_data}},
    "pedigree": {
        "show_default": False,
        "render": {"terminal": lambda p: "-- external data -- " if p == ORPHAN else p},
    },
    "load_config": {"show_default": False},
}


class ValueInfo(Value):
    @classmethod
    def create_from_value(cls, kiara: "Kiara", value: Value):

        aliases = kiara.alias_registry.find_aliases_for_value_id(value.value_id)
        if value.is_stored:
            load_config = kiara.data_registry.retrieve_load_config(
                value_id=value.value_id
            )
        else:
            load_config = None

        is_internal = "internal" in kiara.type_registry.get_type_lineage(
            value.data_type_name
        )

        model = ValueInfo.construct(
            value_id=value.value_id,
            kiara_id=value.kiara_id,
            value_schema=value.value_schema,
            value_status=value.value_status,
            value_size=value.value_size,
            value_hash=value.value_hash,
            pedigree=value.pedigree,
            pedigree_output_name=value.pedigree_output_name,
            data_type_class=value.data_type_class,
            property_refs=value.property_refs,
            destiny_details=value.destiny_details,
            aliases=list(aliases),
            load_config=load_config,
            properties={},
        )
        model._set_registry(value._data_registry)
        model._is_stored = value._is_stored
        model._data_type = value._data_type
        model._value_data = value._value_data
        model._data_retrieved = value._data_retrieved
        model._is_internal = is_internal
        return model

    value_id: uuid.UUID = Field(description="The value id.")
    value_schema: ValueSchema = Field(description="The data schema of this value.")
    aliases: List[str] = Field(
        description="The aliases that are registered for this value."
    )
    pedigree: Optional[ValuePedigree] = Field(description="This values' pedigree.")
    load_config: Optional[LoadConfig] = Field(
        description="The load config associated with this value."
    )
    properties: Mapping[str, Any] = Field(description="The values' properties.")

    _is_internal: bool = PrivateAttr(default=False)

    def _retrieve_id(self) -> str:
        return str(self.value_id)

    def _retrieve_category_id(self) -> str:
        return "instance.value_info"

    def _retrieve_data_to_hash(self) -> Any:
        return self.value_id

    def create_renderable(self, **config: Any) -> RenderableType:
        return create_table_from_model_object(self)


class ValuesInfo(BaseModel):
    @classmethod
    def create_from_values(cls, kiara: "Kiara", *values: Union[Value, uuid.UUID]):

        v = [
            ValueInfo.create_from_value(
                kiara=kiara,
                value=v if isinstance(v, Value) else kiara.data_registry.get_value(v),
            )
            for v in values
        ]
        return ValuesInfo(__root__=v)

    __root__: List[ValueInfo]

    def create_render_map(self, render_type: str = "terminal", **render_config):

        list_by_alias = render_config.get("list_by_alias", True)
        show_internal = render_config.get("show_internal", False)

        render_fields = render_config.get("render_fields", None)
        if not render_fields:
            render_fields = [k for k, v in RENDER_FIELDS.items() if v["show_default"]]
            if list_by_alias:
                render_fields[0] = "aliases"
                render_fields[1] = "value_id"

        render_map: Dict[uuid.UUID, Dict[str, Any]] = {}

        lookup = {}
        for value in self.__root__:
            if not show_internal and value._is_internal:
                continue
            lookup[value.value_id] = value
            details = {}
            for property in render_fields:

                if hasattr(value, property) and property != "data":
                    attr = getattr(value, property)
                else:
                    attr = value
                render_func = (
                    RENDER_FIELDS.get(property, {})
                    .get("render", {})
                    .get(render_type, None)
                )
                if render_func is None:
                    rendered = attr
                else:
                    rendered = render_func(attr)
                details[property] = rendered
            render_map[value.value_id] = details

        if not list_by_alias:
            return {str(k): v for k, v in render_map.items()}
        else:
            result: Dict[str, Dict[str, Any]] = {}
            for value_id, render_details in render_map.items():
                value_aliases = lookup[value_id].aliases
                if value_aliases:
                    for alias in value_aliases:
                        assert alias not in result.keys()
                        render_details = dict(render_details)
                        render_details["alias"] = alias
                        result[alias] = render_details
                else:
                    render_details["alias"] = ""
                    result[f"no_aliases_{value_id}"] = render_details
            return result

    def create_renderable(self, render_type: str = "terminal", **render_config: Any):

        render_map = self.create_render_map(render_type=render_type, **render_config)

        list_by_alias = render_config.get("list_by_alias", True)

        render_fields = render_config.get("render_fields", None)
        if not render_fields:
            render_fields = [k for k, v in RENDER_FIELDS.items() if v["show_default"]]
            if list_by_alias:
                render_fields.insert(0, "alias")
                render_fields.remove("aliases")

        table = Table(box=box.SIMPLE)
        for property in render_fields:
            if property == "aliases" and list_by_alias:
                table.add_column("alias")
            else:
                table.add_column(property)

        for item_id, details in render_map.items():
            row = []
            for field in render_fields:
                value = details[field]
                row.append(value)
            table.add_row(*row)

        return table