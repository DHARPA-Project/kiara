# -*- coding: utf-8 -*-
import typing
from pydantic import Field
from rich import box
from rich.console import RenderableType
from rich.table import Table

from kiara.info import KiaraInfoModel
from kiara.info.operations import OperationsInfo
from kiara.module_config import ModuleInstanceConfig
from kiara.operations import OperationConfig

if typing.TYPE_CHECKING:
    from kiara import Kiara


class OperationsGroupInfo(KiaraInfoModel):
    @classmethod
    def create(cls, kiara: "Kiara"):
        operation_types = OperationsInfo.create_all(kiara=kiara)
        operation_configs = operation_types.pop("all")
        return OperationsGroupInfo(
            operation_types=operation_types,
            operation_configs=operation_configs.operation_configs,
        )

    operation_types: typing.Dict[str, OperationsInfo] = Field(
        description="The available operation types and their details."
    )
    operation_configs: typing.Dict[str, OperationConfig] = Field(
        description="The available operation ids and configs."
    )

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")

        op_map_table = OperationsInfo.create_renderable_from_operations_map(
            self.operation_types
        )
        table.add_row("operation types", op_map_table)
        configs = ModuleInstanceConfig.create_renderable_from_module_instance_configs(
            self.operation_configs
        )
        table.add_row("operations", configs)
        return table


class KiaraInfo(KiaraInfoModel):
    @classmethod
    def create(cls, kiara: "Kiara"):

        op_group = OperationsGroupInfo.create(kiara=kiara)
        return KiaraInfo(operations=op_group)

    operations: OperationsGroupInfo = Field(
        "Information about supported operations in this kiara context."
    )
