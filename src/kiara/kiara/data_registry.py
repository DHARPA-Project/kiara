# -*- coding: utf-8 -*-
import copy
import json
import uuid

from orjson import orjson
from rich import box
from rich.console import RenderableType
from rich.table import Table
from sqlalchemy.engine import Engine
from typing import TYPE_CHECKING, Any, Dict, Mapping, Optional, Union, Set

import structlog
from kiara.defaults import STRICT_CHECKS, SpecialValue, NOT_SET_VALUE_ID, NONE_VALUE_ID
from kiara.models.values import ValueStatus
from kiara.models.values.value import (
    ORPHAN,
    Value,
    ValuePedigree,
    ValueSet,
    ValueSetReadOnly, StaticValue,
)
from kiara.models.values.value_metadata import ValueMetadata
from kiara.models.values.value_schema import ValueSchema
from kiara.utils import log_message
from kiara.value_types.included_core_types.persistence import LoadConfig

if TYPE_CHECKING:
    from kiara.kiara import Kiara


logger = structlog.getLogger()

class DataRegistry(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._engine: Engine = self._kiara._engine

        self._registered_values: Dict[uuid.UUID, Value] = {}
        self._value_by_hash: Dict[int, Dict[int, Set[Value]]] = {}

        # initialize special values
        self._not_set_value: Value = StaticValue(value_id=NOT_SET_VALUE_ID, kiara_id=self._kiara.id, value_schema=ValueSchema(type="special_type", default=SpecialValue.NOT_SET, is_constant=True, doc="Special value, indicating a field is not set."), value_status=ValueStatus.NOT_SET, value_size=0, value_hash=-1, pedigree=ORPHAN, pedigree_output_name="__void__")
        self._not_set_value.init_data(data=SpecialValue.NOT_SET)
        self._none_value: Value = StaticValue(value_id=NONE_VALUE_ID, kiara_id=self._kiara.id, value_schema=ValueSchema(type="special_type", default=SpecialValue.NO_VALUE, is_constant=True, doc="Special value, indicating a field is set with a 'none' value."), value_status=ValueStatus.NONE, value_size=0, value_hash=-2, pedigree=ORPHAN, pedigree_output_name="__void__")
        self._none_value.init_data(data=SpecialValue.NO_VALUE)

    def get_value(self, value: Union[uuid.UUID, str, Value]) -> Value:

        if isinstance(value, str):
            value_id = uuid.UUID(value)
        elif isinstance(value, Value):
            value_id = value.value_id
            # TODO: check hash is the same as registered one?
        else:
            value_id = value

        if value_id in self._registered_values.keys():
            return self._registered_values[value_id]

        try:
            value = self._kiara.data_store.retrieve_value(value_id=value_id)
            self._registered_values[value_id] = value
            return self._registered_values[value_id]
        except Exception as e:
            raise e
            # raise Exception(f"No value registered with id: {value}")



    def load_valueset(self, values: Mapping[str, uuid.UUID]):

        value_items = {}
        schemas = {}
        for field_name, value_id in values.items():
            value = self.get_value(value=value_id)
            value_items[field_name] = value
            schemas[field_name] = value.value_schema

        return ValueSetReadOnly(value_items=value_items, values_schema=schemas)

    def create_valueset(
        self, data: Mapping[str, Any], schema: Mapping[str, ValueSchema]
    ) -> ValueSet:
        """Extract a set of [Value][kiara.data.values.Value] from Python data and ValueSchemas."""

        input_details = {}
        for input_name, value_schema in schema.items():

            input_details[input_name] = {"schema": value_schema}

        leftover = set(data.keys())
        leftover.difference_update(input_details.keys())
        if leftover:
            if not STRICT_CHECKS:
                log_message("unused.inputs", input_names=leftover)
            else:
                raise Exception(
                    f"Can't register job, inputs contain unused/invalid fields: {', '.join(leftover)}"
                )

        values = {}
        for input_name, details in input_details.items():

            value_schema = details["schema"]

            if input_name not in data.keys():
                value_data = SpecialValue.NOT_SET
            else:
                value_data = data[input_name]

            if isinstance(value_data, Value):
                value = value_data
            elif isinstance(value_data, uuid.UUID):
                raise NotImplementedError()
            else:
                value = self.register_data(
                    data=value_data, schema=value_schema, pedigree=ORPHAN, pedigree_output_name="__void__"
                )
            values[input_name] = value

        return ValueSetReadOnly(value_items=values, values_schema=schema)  # type: ignore

    def find_values_for_hash(self, value_hash: int, value_type: Optional[str]=None) -> Set[Value]:

        if value_type:
            raise NotImplementedError()

        cached = self._value_by_hash.get(value_hash, None)
        if cached is not None:
            return cached

        stored = self._kiara.data_store.find_values_for_hash(value_hash=value_hash, value_type=value_type)
        if stored:
            self._value_by_hash[value_hash] = stored
            return stored
        return set()

    def register_data(
        self,
        data: Any,
        schema: Optional[ValueSchema] = None,
        pedigree: Optional[ValuePedigree] = None,
        pedigree_output_name: str = None,
        reuse_existing: bool = True
    ) -> Value:

        if schema is None:
            raise NotImplementedError()

        if pedigree is None:
            raise NotImplementedError()

        if pedigree_output_name is None:
            raise NotImplementedError()

        if data == SpecialValue.NOT_SET:
            return self._not_set_value
        elif data == SpecialValue.NO_VALUE:
            return self._none_value

        if isinstance(data, Value):
            if data.value_id in self._registered_values.keys():
                raise Exception(f"Can't register value '{data.value_id}: already registered")

            self._registered_values[data.value_id] = data
            return data
        else:
            value_type = self._kiara.get_value_type(
                value_type=schema.type, value_type_config=schema.type_config
            )
            # import traceback
            # traceback.print_stack()
            data, status, value_hash = value_type._pre_examine_data(data=data, schema=schema)

            if reuse_existing:
                existing = self.find_values_for_hash(value_hash=value_hash)
                if existing:
                    if len(existing) != 1:
                        raise NotImplementedError()
                    return next(iter(existing))

            v_id = uuid.uuid4()
            value = value_type.assemble_static_value(value_id=v_id, data=data, schema=schema, status=status, value_hash=value_hash, pedigree=pedigree, kiara_id=self._kiara.id, pedigree_output_name=pedigree_output_name)

            self._value_by_hash.setdefault(value_type.value_type_hash, {})[
                value.value_hash
            ] = set(value)
            self._registered_values[value.value_id] = value
        return value

    def create_renderable(self, config: Optional["RenderConfig"]=None) -> RenderableType:
        """Create a renderable for this module configuration."""

        from kiara.utils.output import RenderConfig, create_renderable_from_values

        table = create_renderable_from_values({str(i): v for i, v in self._registered_values.items()}, config=config)
        return table
