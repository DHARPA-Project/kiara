# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import typing
from deepdiff import DeepHash
from pydantic import Field, PrivateAttr
from rich import box
from rich.console import RenderableType
from rich.table import Table

from kiara import Kiara
from kiara.defaults import KIARA_HASH_FUNCTION
from kiara.info import KiaraInfoModel
from kiara.metadata.operation_models import OperationsMetadata
from kiara.operations import Operation, OperationType


class OperationsInfo(KiaraInfoModel):
    @classmethod
    def create_all(self, kiara: Kiara) -> typing.Dict[str, "OperationsInfo"]:

        op_types = kiara.operation_mgmt.operation_types
        return {
            op_name: OperationsInfo.create(operations=op_types[op_name])
            for op_name in sorted(op_types.keys())
        }

    @classmethod
    def create(cls, operations: OperationType):

        info = OperationsMetadata.from_operations_class(operations.__class__)
        return OperationsInfo(info=info, operation_configs=dict(operations.operations))

    @classmethod
    def create_renderable_from_operations_map(
        cls, op_map: typing.Mapping[str, "OperationsInfo"]
    ):

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")
        for op_name, ops in op_map.items():
            table.add_row(op_name, ops)

        return table

    info: OperationsMetadata = Field(
        description="Details about the type of operations contained in this collection."
    )
    operation_configs: typing.Dict[str, Operation] = Field(
        description="All available operation ids and their configurations."
    )
    _hash_cache: typing.Optional[str] = PrivateAttr(default=None)

    @property
    def module_config_hash(self):
        if self._hash_cache is not None:
            return self._hash_cache

        obj = {
            "info": self.info.get_id(),
            "operation_configs": {
                k: v.get_id() for k, v in self.operation_configs.items()
            },
        }
        h = DeepHash(obj, hasher=KIARA_HASH_FUNCTION)
        self._hash_cache = h[obj]
        return self._hash_cache

    def get_id(self) -> str:
        return self.module_config_hash

    def get_category_alias(self) -> str:
        return "all_operations_info"

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        return self.info.create_renderable(
            operations=self.operation_configs, omit_type_name=True
        )


# class OperationsGroupInfo(KiaraInfoModel):
#     @classmethod
#     def create(cls, kiara: "Kiara", ignore_errors: bool = False):
#
#         operation_types = OperationsInfo.create_all(kiara=kiara)
#         operation_configs = operation_types.pop("all")
#
#         return OperationsGroupInfo(
#             operation_types=operation_types,
#             operation_configs=operation_configs.operation_configs,
#         )
#
#     operation_types: typing.Dict[str, OperationsInfo] = Field(
#         description="The available operation types and their details."
#     )
#     operation_configs: typing.Dict[str, Operation] = Field(
#         description="The available operation ids and module_configs."
#     )
#
#     def create_renderable(self, **config: typing.Any) -> RenderableType:
#
#         table = Table(show_header=False, box=box.SIMPLE)
#         table.add_column("Key", style="i")
#         table.add_column("Value")
#
#         op_map_table = OperationsInfo.create_renderable_from_operations_map(
#             self.operation_types
#         )
#         table.add_row("operation types", op_map_table)
#         configs = ModuleConfig.create_renderable_from_module_instance_configs(
#             self.operation_configs
#         )
#         table.add_row("operations", configs)
#         return table
