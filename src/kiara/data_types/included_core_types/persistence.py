# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

# from pydantic import Field
# from typing import Type
#
# from kiara.data_types import DataTypeConfig
# from kiara.data_types.included_core_types.internal import InternalType
# from kiara.models.module.persistence import LoadConfig
#
#
# class LoadConfigSchema(DataTypeConfig):
#
#     persistence_target: str = Field(
#         description="A hint as to the persistence target (e.g. disk)."
#     )
#     persistence_format: str = Field(
#         description="A hint as to the persistence format (e.g. pickle)."
#     )
#
#
# class LoadConfigValueType(InternalType[LoadConfig, LoadConfigSchema]):
#     """A value type that contains data that describes how to (re-)load a value from disk.
#
#     This is mostly used internally in kiara, but might be exposed to users in certain cases (for example when exporting
#     a value in the native kiara file format).
#     """
#
#     _data_type_name = "load_config"
#
#     @classmethod
#     def python_class(cls) -> Type:
#         return LoadConfig
#
#     @classmethod
#     def data_type_config_class(cls) -> Type[LoadConfigSchema]:
#         return LoadConfigSchema
#
#     def is_immutable(self) -> bool:
#         return True
#
#     def calculate_hash(self, data: LoadConfig) -> int:
#         """Calculate the hash of the value."""
#
#         return data.model_data_hash
#
#     def calculate_size(self, data: LoadConfig) -> int:
#         return data.model_size
#
#     @property
#     def persistence_target(self) -> str:
#         return self.type_config.persistence_target
#
#     @property
#     def persistence_format(self) -> str:
#         return self.type_config.persistence_format
