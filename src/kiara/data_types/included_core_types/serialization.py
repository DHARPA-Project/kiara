# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


from typing import Any, Mapping, Type

from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types.internal import InternalType
from kiara.defaults import INVALID_HASH_MARKER, INVALID_SIZE_MARKER
from kiara.models.values.value import SerializedData, Value


class PythonObjectType(InternalType[object, DataTypeConfig]):
    """
    A 'plain' Python object.

    This data type is mostly used internally, for hading over data in (de-)serialization operations.
    """

    @classmethod
    def python_class(cls) -> Type:
        return object

    def parse_python_obj(self, data: Any) -> object:
        return data

    def calculate_hash(self, data: SerializedData) -> str:
        """Calculate the hash of the value."""
        return INVALID_HASH_MARKER

    def calculate_size(self, data: SerializedData) -> int:
        return INVALID_SIZE_MARKER

    def _pretty_print_as__terminal_renderable(
        self, value: Value, render_config: Mapping[str, Any]
    ):

        return str(value.data)
