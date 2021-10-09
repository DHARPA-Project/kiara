# -*- coding: utf-8 -*-
import typing
from pydantic import Field
from rich import box
from rich.console import RenderableType
from rich.table import Table

from kiara import Kiara
from kiara.info import KiaraInfoModel
from kiara.metadata.operation_models import OperationsMetadata
from kiara.module_config import ModuleConfig
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
        return OperationsInfo(
            info=info, operation_configs=dict(operations.operation_configs)
        )

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

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        return self.info.create_renderable(
            operations=self.operation_configs, omit_type_name=True
        )


class OperationsGroupInfo(KiaraInfoModel):
    @classmethod
    def create(cls, kiara: "Kiara", ignore_errors: bool = False):

        operation_types = OperationsInfo.create_all(kiara=kiara)
        operation_configs = operation_types.pop("all")

        return OperationsGroupInfo(
            operation_types=operation_types,
            operation_configs=operation_configs.operation_configs,
        )

    operation_types: typing.Dict[str, OperationsInfo] = Field(
        description="The available operation types and their details."
    )
    operation_configs: typing.Dict[str, Operation] = Field(
        description="The available operation ids and module_configs."
    )

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")

        op_map_table = OperationsInfo.create_renderable_from_operations_map(
            self.operation_types
        )
        table.add_row("operation types", op_map_table)
        configs = ModuleConfig.create_renderable_from_module_instance_configs(
            self.operation_configs
        )
        table.add_row("operations", configs)
        return table
