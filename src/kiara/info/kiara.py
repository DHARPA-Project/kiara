# -*- coding: utf-8 -*-
import typing
from pydantic import Field
from rich import box
from rich.console import RenderableType
from rich.table import Table

from kiara.info import KiaraInfoModel
from kiara.info.modules import ModuleTypesGroupInfo
from kiara.info.operations import OperationsGroupInfo
from kiara.info.pipelines import PipelineTypesGroupInfo

if typing.TYPE_CHECKING:
    from kiara import Kiara


class ModulesGroup(KiaraInfoModel):
    @classmethod
    def create(cls, kiara: "Kiara", ignore_errors: bool=False):

        module_types = ModuleTypesGroupInfo.from_type_names(
            kiara=kiara, ignore_pipeline_modules=True
        )
        pipeline_types = PipelineTypesGroupInfo.create(kiara=kiara, ignore_errors=ignore_errors)

        return ModulesGroup(module_types=module_types, pipelines=pipeline_types)

    module_types: ModuleTypesGroupInfo = Field(
        description="The available module types."
    )
    pipelines: PipelineTypesGroupInfo = Field(description="The available pipelines.")

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        mod_table = self.module_types.create_renderable()
        pipe_table = self.pipelines.create_renderable()

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Type", style="b")
        table.add_column("Modules")

        table.add_row("Module types", mod_table)
        table.add_row("Pipelines", pipe_table)
        return table


class KiaraContext(KiaraInfoModel):
    @classmethod
    def create(cls, kiara: "Kiara", ignore_errors: bool = False):

        modules = ModulesGroup.create(kiara=kiara, ignore_errors=ignore_errors)
        op_group = OperationsGroupInfo.create(kiara=kiara, ignore_errors=ignore_errors)
        return KiaraContext(
            modules=modules,
            operations=op_group,
        )

    modules: ModulesGroup = Field(
        description="Information about the available modules."
    )
    operations: OperationsGroupInfo = Field(
        description="Information about supported operations in this kiara context."
    )

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")

        module_table = self.modules.create_renderable()
        table.add_row("module types", module_table)

        operations_table = self.operations.create_renderable()
        table.add_row("operations", operations_table)

        return table
