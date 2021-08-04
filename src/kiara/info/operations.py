# -*- coding: utf-8 -*-
import typing
from pydantic import Field
from rich import box
from rich.console import RenderableType
from rich.table import Table

from kiara import Kiara
from kiara.info import KiaraInfoModel
from kiara.metadata.operation_models import OperationsMetadata
from kiara.operations import OperationConfig, Operations


class OperationsInfo(KiaraInfoModel):
    @classmethod
    def create_all(self, kiara: Kiara) -> typing.Dict[str, "OperationsInfo"]:

        op_types = kiara.operation_mgmt.operation_types
        return {
            op_name: OperationsInfo.create(operations=op_types[op_name])
            for op_name in sorted(op_types.keys())
        }

    @classmethod
    def create(cls, operations: Operations):

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
    operation_configs: typing.Dict[str, OperationConfig] = Field(
        description="All available operation ids and their configurations."
    )

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        return self.info.create_renderable(
            operations=self.operation_configs, omit_type_name=True
        )
