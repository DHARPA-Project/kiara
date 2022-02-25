# -*- coding: utf-8 -*-
"""The 'run' subcommand for the cli."""
#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import copy
import os
import rich_click as click
import typing
from pydantic import Field, PrivateAttr
from rich import box
from rich.console import RenderableType
from rich.panel import Panel
from rich.table import Table

from kiara import Kiara
from kiara.data import Value
from kiara.data.values import ValueAlias, ValueInfo
from kiara.info import KiaraInfoModel
from kiara.metadata.module_models import KiaraModuleTypeMetadata
from kiara.metadata.operation_models import OperationsMetadata
from kiara.module import KiaraModule
from kiara.operations import Operation, OperationType
from kiara.utils import create_uuid4_string, log_message
from kiara.utils.output import rich_print


def find_operation_types(
    item: str, kiara: typing.Optional[Kiara] = None
) -> typing.Dict[str, OperationsMetadata]:

    if kiara is None:
        kiara = Kiara.instance()

    all_operations_types = kiara.operation_mgmt.operation_types

    matches = {}
    for operation_name in sorted(all_operations_types.keys()):
        if operation_name == "all":
            continue

        operation_details: OperationType = all_operations_types[operation_name]
        if item in operation_name:
            matches[operation_name] = OperationsMetadata.from_operations_class(
                operation_details.__class__
            )
            continue

        desc = operation_details.get_type_metadata().documentation.description
        if item in desc:
            matches[operation_name] = OperationsMetadata.from_operations_class(
                operation_details.__class__
            )

    return matches


def find_operations(
    item: str,
    search_desc: bool = True,
    search_full_doc: bool = False,
    kiara: typing.Optional[Kiara] = None,
) -> typing.Dict[str, Operation]:

    if kiara is None:
        kiara = Kiara.instance()

    all_operations_types = kiara.operation_mgmt.operation_types

    matches = {}
    for operation_name in all_operations_types.keys():
        if operation_name == "all":
            continue

        operation_details: OperationType = all_operations_types[operation_name]

        for op_id, op_config in sorted(operation_details.operations.items()):

            if item in op_id:
                matches[op_id] = op_config
                continue

            if not search_desc:
                continue

            if search_full_doc:
                desc = op_config.doc.full_doc
            else:
                desc = op_config.doc.description

            if item in desc:
                matches[op_id] = op_config

    return matches


def find_module_types(
    item: str,
    search_desc: bool = True,
    search_full_doc: bool = False,
    kiara: typing.Optional[Kiara] = None,
) -> typing.Dict[str, KiaraModuleTypeMetadata]:

    if kiara is None:
        kiara = Kiara.instance()

    matches = {}
    for module_id in kiara.available_module_types:
        if item in module_id:
            module_cls = kiara.get_module_class(module_id)
            matches[module_id] = KiaraModuleTypeMetadata.from_module_class(module_cls)
            continue

        if not search_desc:
            continue

        module_cls = kiara.get_module_class(module_id)
        if search_full_doc:
            desc = module_cls.get_type_metadata().documentation.full_doc
        else:
            desc = module_cls.get_type_metadata().documentation.description

        if item in desc:
            matches[module_id] = KiaraModuleTypeMetadata.from_module_class(module_cls)

    return matches


def find_values(
    item: str,
    search_ids: bool = True,
    search_aliases: bool = True,
    kiara: typing.Optional[Kiara] = None,
) -> typing.Dict[str, Value]:

    if kiara is None:
        kiara = Kiara.instance()

    matches = {}
    if search_ids:
        for value_id in kiara.data_store.value_ids:
            if item in value_id:
                value_obj = kiara.data_store.get_value_obj(value_id)
                matches[value_id] = ValueInfo.from_value(value_obj)  # type: ignore

    if search_aliases:
        _alias_obj = ValueAlias.from_string(item)
        if _alias_obj.tag or _alias_obj.version:
            match = kiara.data_store.get_value_obj(_alias_obj)
            if match:
                matches[_alias_obj.full_alias] = ValueInfo.from_value(match)
        else:
            for alias in kiara.data_store.alias_names:
                if item in alias:
                    value_slot = kiara.data_store.get_value_slot(alias)
                    assert value_slot
                    value = value_slot.get_latest_value()
                    assert value
                    matches[alias] = ValueInfo.from_value(value)

    return matches


class KiaraEntityMatches(KiaraInfoModel):
    @classmethod
    def search(
        self,
        search_term: str,
        search_ids: bool = True,
        search_aliases: bool = True,
        search_desc: bool = True,
        search_full_doc: bool = False,
        kiara: typing.Optional[Kiara] = None,
    ):

        if kiara is None:
            kiara = Kiara.instance()

        module_types = find_module_types(
            item=search_term,
            search_desc=search_desc,
            search_full_doc=search_full_doc,
            kiara=kiara,
        )
        operation_types = find_operation_types(item=search_term, kiara=kiara)
        operations = find_operations(
            item=search_term,
            search_desc=search_desc,
            search_full_doc=search_full_doc,
            kiara=kiara,
        )
        values = find_values(
            item=search_term,
            search_ids=search_ids,
            search_aliases=search_aliases,
            kiara=kiara,
        )

        return KiaraEntityMatches(
            module_types=module_types,
            operation_types=operation_types,
            operations=operations,
            values=values,
            kiara=kiara,
        )

    def __init__(
        self,
        module_types: typing.Optional[
            typing.Dict[str, typing.Type[KiaraModule]]
        ] = None,
        operation_types: typing.Optional[typing.Dict[str, OperationsMetadata]] = None,
        operations: typing.Optional[typing.Dict[str, Operation]] = None,
        values: typing.Optional[typing.Dict[str, Value]] = None,
        kiara: Kiara = None,
    ):

        if kiara is None:
            kiara = Kiara.instance()

        init_dict: typing.Dict[str, typing.Any] = {}
        if module_types:
            init_dict["module_types"] = module_types
        if operation_types:
            init_dict["operation_types"] = operation_types
        if operations:
            init_dict["operations"] = operations
        if values:
            init_dict["values"] = values

        super().__init__(**init_dict)
        self._kiara = kiara

    _kiara: Kiara = PrivateAttr()

    id: str = Field(
        description="The entity instance id.", default_factory=create_uuid4_string
    )
    module_types: typing.Dict[str, KiaraModuleTypeMetadata] = Field(
        description="Matching module types.", default_factory=dict
    )
    operation_types: typing.Dict[str, OperationsMetadata] = Field(
        description="Matching operation types.", default_factory=dict
    )
    operations: typing.Dict[str, Operation] = Field(
        description="Matching operations.", default_factory=dict
    )
    values: typing.Dict[str, ValueInfo] = Field(
        description="Matching values.", default_factory=dict
    )

    def get_id(self) -> str:
        return self.id

    def get_category_alias(self) -> str:
        return "instance.entity"

    @property
    def no_module_types(self) -> int:
        return len(self.module_types)

    @property
    def no_operation_types(self) -> int:
        return len(self.operation_types)

    @property
    def no_operations(self) -> int:
        return len(self.operations)

    @property
    def no_values(self) -> int:
        return len(self.values)

    @property
    def no_results(self) -> int:

        return (
            self.no_module_types
            + self.no_operation_types
            + self.no_operations
            + self.no_values
        )

    def get_single_result(
        self,
    ) -> typing.Optional[typing.Tuple[typing.Type, str, typing.Any]]:

        if self.no_results != 1:
            return None

        if self.module_types:
            match_key = next(iter(self.module_types.keys()))
            match_value: typing.Any = self.module_types[match_key]
            return (KiaraModuleTypeMetadata, match_key, match_value)

        if self.operation_types:
            match_key = next(iter(self.operation_types.keys()))
            match_value = self.operation_types[match_key]
            return (OperationsMetadata, match_key, match_value)

        if self.operations:
            match_key = next(iter(self.operations.keys()))
            match_value = self.operations[match_key]
            return (Operation, match_key, match_value)

        if self.values:
            match_key = next(iter(self.values.keys()))
            match_value = self.values[match_key]
            return (ValueInfo, match_key, match_value)

        raise Exception("No match found. This is a bug.")

    def merge(self, other: "KiaraEntityMatches") -> "KiaraEntityMatches":

        module_types = copy.copy(self.module_types)
        module_types.update(other.module_types)

        operation_types = copy.copy(self.operation_types)
        operation_types.update(other.operation_types)

        operations = copy.copy(self.operations)
        operations.update(other.operations)

        values = copy.copy(self.values)
        values.update(other.values)

        return KiaraEntityMatches(
            module_types=module_types,
            operation_types=operation_types,
            operations=operations,
            values=values,
            kiara=self._kiara,
        )

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Id")
        table.add_column("Description")

        empty_line = False

        if self.module_types:

            table.add_row()
            table.add_row("[b]Module types[/b]", "")
            table.add_row()
            for module_type, module_type_info in self.module_types.items():
                table.add_row(
                    f"  [i]{module_type}[/i]",
                    module_type_info.documentation.description,
                )

            empty_line = True

        if self.operation_types:
            if empty_line:
                table.add_row()
            table.add_row("[b]Operation types[/b]", "")
            table.add_row()
            for op_type, op_type_info in self.operation_types.items():
                table.add_row(
                    f"  [i]{op_type}[/i]", op_type_info.documentation.description
                )

            empty_line = True

        if self.operations:
            if empty_line:
                table.add_row()
            table.add_row("[b]Operations[/b]", "")
            table.add_row()
            for op_id, op_info in self.operations.items():
                table.add_row(f"  [i]{op_id}[/i]", op_info.doc.description)

            empty_line = True

        if self.values:
            if empty_line:
                table.add_row()
            table.add_row("[b]Values[/b]", "")
            table.add_row()
            for value_id, value in self.values.items():
                table.add_row(f"  [i]{value_id}[/i]", value.value_schema.type)

        return table


@click.command()
@click.argument("search_terms", required=True, nargs=-1, metavar="search_term")
@click.pass_context
def explain(ctx, search_terms):
    """Find and explain any entity in kiara."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    search_ids = True
    search_aliases = True
    search_desc = True
    search_full_doc = False

    result = KiaraEntityMatches(kiara=kiara_obj)

    for search_term in search_terms:

        if os.path.isfile(os.path.realpath(search_term)):
            try:
                search_term = kiara_obj.register_pipeline_description(data=search_term)
            except Exception as e:
                log_message(f"Tried to import '{search_term}' as pipeline, failed: {e}")

        matches = KiaraEntityMatches.search(
            search_term=search_term,
            search_desc=search_desc,
            search_full_doc=search_full_doc,
            search_ids=search_ids,
            search_aliases=search_aliases,
            kiara=kiara_obj,
        )
        result = result.merge(matches)

    if result.no_results == 1:
        print()
        result_type, result_id, result_value = result.get_single_result()
        if result_type == KiaraModuleTypeMetadata:
            title = "Module type"
        elif result_type == OperationsMetadata:
            title = "Operations type"
        elif result_type == Operation:
            title = "Operation"
        elif result_type == ValueInfo:
            title = "Value"
        else:
            raise Exception("Invalid result type. This is a bug.")

        title = f"{title}: [b]{result_id}[/b]"
        rich_print(
            Panel(result_value, title=title, title_align="left", box=box.ROUNDED)
        )
    else:
        rich_print(result)
