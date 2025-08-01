# -*- coding: utf-8 -*-

"""
This module contains the metadata (and other) models that are used in the ``kiara_plugin.core_types`` package.

Those models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata -- but also
other type of models -- that is attached to data, as well as *kiara* modules.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel]. Other models usually
sub-class a pydantic BaseModel or implement custom base classes.
"""

from typing import Any, Dict, Mapping, Union

from pydantic import BaseModel, Field, PrivateAttr

from kiara.models.python_class import PythonClass
from kiara.utils.hashing import compute_cid


class KiaraDict(BaseModel, Mapping):
    dict_data: Dict[str, Any] = Field(description="The data.")
    data_schema: Dict[str, Any] = Field(description="The schema.")
    python_class: PythonClass = Field(
        description="The python class of which model instances are created. This is mostly meant as a hint for client applications."
    )

    _size_cache: Union[int, None] = PrivateAttr(default=None)
    _hash_cache: Union[int, None] = PrivateAttr(default=None)
    _data_hash: Union[int, None] = PrivateAttr(default=None)
    _schema_hash: Union[int, None] = PrivateAttr(default=None)
    _value_hash: Union[int, None] = PrivateAttr(default=None)

    @property
    def size(self):
        if self._size_cache is not None:
            return self._size_cache

        self._size_cache = len(self.dict_data) + len(self.data_schema)
        return self._size_cache

    @property
    def data_hash(self) -> int:
        if self._data_hash is not None:
            return self._data_hash

        _data_hash, _ = compute_cid(self.dict_data)
        self._data_hash = int.from_bytes(_data_hash, byteorder="big")
        return self._data_hash

    @property
    def schema_hash(self) -> int:
        if self._schema_hash is not None:
            return self._schema_hash

        _schema_hash, _ = compute_cid(self.data_schema)
        self._schema_hash = int.from_bytes(_schema_hash, byteorder="big")
        return self._schema_hash

    @property
    def value_hash(self) -> int:
        if self._value_hash is not None:
            return self._value_hash

        obj = {"data": self.data_hash, "data_schema": self.schema_hash}
        _value_hash, _ = compute_cid(obj)  # type: ignore
        self._value_hash = int.from_bytes(_value_hash, byteorder="big")
        return self._value_hash

    def __getitem__(self, item):
        return self.dict_data.__getitem__(item)

    def __iter__(self):
        return self.dict_data.__iter__()

    def __len__(self):
        return self.dict_data.__len__()

    def __repr__(self):
        full = {
            "dict_data": self.dict_data,
            "data_schema": self.data_schema,
        }
        return full.__repr__()
