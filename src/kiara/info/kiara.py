# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import typing
from pydantic import Field
from rich import box
from rich.console import RenderableType
from rich.table import Table

from kiara.info import KiaraDynamicInfoModel, KiaraInfoModel
from kiara.info.modules import ModuleTypesGroupInfo
from kiara.info.operations import OperationsInfo
from kiara.info.pipelines import PipelineTypesGroupInfo
from kiara.metadata.operation_models import OperationsMetadata
from kiara.metadata.type_models import ValueTypeMetadata

if typing.TYPE_CHECKING:
    from kiara import Kiara


# class ModulesGroup(KiaraInfoModel):
#     @classmethod
#     def create(cls, kiara: "Kiara", ignore_errors: bool = False):
#
#         module_types = ModuleTypesGroupInfo.from_type_names(
#             kiara=kiara, ignore_pipeline_modules=True
#         )
#         pipeline_types = PipelineTypesGroupInfo.create(
#             kiara=kiara, ignore_errors=ignore_errors
#         )
#
#         return ModulesGroup(module_types=module_types, pipelines=pipeline_types)
#
#     module_types: ModuleTypesGroupInfo = Field(
#         description="The available module types."
#     )
#     pipelines: PipelineTypesGroupInfo = Field(description="The available pipelines.")
#
#     def create_renderable(self, **config: typing.Any) -> RenderableType:
#
#         mod_table = self.module_types.create_renderable()
#         pipe_table = self.pipelines.create_renderable()
#
#         table = Table(show_header=False, box=box.SIMPLE)
#         table.add_column("Type", style="b")
#         table.add_column("Modules")
#
#         table.add_row("Module types", mod_table)
#         table.add_row("Pipelines", pipe_table)
#         return table


class KiaraContext(KiaraInfoModel):

    available_categories: typing.ClassVar = [
        "value_types",
        "modules",
        "pipelines",
        "operations",
        "operation_types",
    ]
    _info_cache: typing.ClassVar = {}

    @classmethod
    def get_info_for_category(
        cls, kiara: "Kiara", category_name: str, ignore_errors: bool = False
    ) -> KiaraInfoModel:

        if category_name not in cls.available_categories:
            raise Exception(
                f"Can't provide information for category '{category_name}': invalid category name. Valid names: {', '.join(cls.available_categories)}"
            )

        cache = (
            cls._info_cache.get(kiara._id, {})
            .get(category_name, {})
            .get(ignore_errors, None)
        )
        if cache is not None:
            return cache

        if category_name == "value_types":
            all_types = ValueTypeMetadata.create_all(kiara=kiara)
            info = KiaraDynamicInfoModel.create_from_child_models(**all_types)
        elif category_name == "modules":
            info = ModuleTypesGroupInfo.from_type_names(
                kiara=kiara, ignore_pipeline_modules=True
            )
        elif category_name == "pipelines":
            info = PipelineTypesGroupInfo.create(
                kiara=kiara, ignore_errors=ignore_errors
            )
        elif category_name == "operation_types":
            all_op_types = OperationsMetadata.create_all(kiara=kiara)
            info = KiaraDynamicInfoModel.create_from_child_models(**all_op_types)
        elif category_name == "operations":
            ops_infos = KiaraDynamicInfoModel.create_from_child_models(
                **OperationsInfo.create_all(kiara=kiara)
            )
            operation_configs = {}
            for v in ops_infos.__root__.values():
                configs = v.operation_configs
                operation_configs.update(configs)

            info = KiaraDynamicInfoModel.create_from_child_models(**operation_configs)
        else:
            raise NotImplementedError(f"Category not available: {category_name}")

        cls._info_cache.setdefault(kiara._id, {}).setdefault(ignore_errors, {})[
            category_name
        ] = info
        return info

    @classmethod
    def get_info(
        cls,
        kiara: "Kiara",
        sub_type: typing.Optional[str] = None,
        ignore_errors: bool = False,
    ):

        if sub_type is None:

            module_types = cls.get_info_for_category(
                kiara=kiara, category_name="modules", ignore_errors=ignore_errors
            )
            value_types = cls.get_info_for_category(
                kiara=kiara, category_name="value_types", ignore_errors=ignore_errors
            )
            pipeline_types = cls.get_info_for_category(
                kiara=kiara, category_name="pipelines", ignore_errors=ignore_errors
            )
            op_group = cls.get_info_for_category(
                kiara=kiara, category_name="operations", ignore_errors=ignore_errors
            )
            op_types = cls.get_info_for_category(
                kiara=kiara,
                category_name="operation_types",
                ignore_errors=ignore_errors,
            )

            return KiaraContext(
                value_types=value_types,
                modules=module_types,
                pipelines=pipeline_types,
                operations=op_group,
                operation_types=op_types,
            )
        else:
            tokens = sub_type.split(".")
            current = cls.get_info_for_category(
                kiara=kiara, category_name=tokens[0], ignore_errors=ignore_errors
            )

            if len(tokens) == 1:
                return current

            path = ".".join(tokens[1:])
            try:
                current = current.get_subcomponent(path)
            except Exception as e:
                raise Exception(
                    f"Can't get '{path}' information for '{tokens[0]}': {e}"
                )
            return current

    value_types: KiaraDynamicInfoModel = Field(
        description="Information about available value types."
    )
    modules: ModuleTypesGroupInfo = Field(
        description="Information about the available modules."
    )
    pipelines: PipelineTypesGroupInfo = Field(
        description="Information about available pipelines."
    )
    operation_types: KiaraDynamicInfoModel = Field(
        description="Information about operation types contained in the current kiara context."
    )
    operations: KiaraDynamicInfoModel = Field(description="Available operations.")

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")

        value_type_table = self.value_types.create_renderable()
        table.add_row("value_types", value_type_table)

        module_table = self.modules.create_renderable()
        table.add_row("modules", module_table)

        pipelines_table = self.pipelines.create_renderable()
        table.add_row("pipelines", pipelines_table)

        operation_types_table = self.operation_types.create_renderable()
        table.add_row("operation_types", operation_types_table)

        operations_table = self.operations.create_renderable()
        table.add_row("operations", operations_table)

        return table
