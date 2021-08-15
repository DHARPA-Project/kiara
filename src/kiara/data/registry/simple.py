# -*- coding: utf-8 -*-
import logging
import typing
import uuid

from kiara.data.registry import DataRegistry
from kiara.data.values import Value, ValueMetadata, ValueSchema
from kiara.defaults import SpecialValue
from kiara.pipeline.values import ValueField, ValueUpdateHandler

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara

log = logging.getLogger("kiara")


def generate_random_value_id():

    return str(uuid.uuid4())


class SimpleRegistry(DataRegistry):
    def __init__(self, kiara: Kiara):

        super().__init__(kiara=kiara)
        self._all_values: typing.Dict[str, Value] = {}

    def register_value(
        self,
        value_schema: ValueSchema,
        value_fields: typing.Union[
            ValueField, typing.Iterable[ValueField], None
        ] = None,
        callbacks: typing.Optional[typing.Iterable[ValueUpdateHandler]] = None,
        initial_value: typing.Any = SpecialValue.NOT_SET,
        is_constant: bool = False,
        value_metadata: typing.Union[
            None, typing.Mapping[str, typing.Any], ValueMetadata
        ] = None,
    ) -> Value:

        pass
