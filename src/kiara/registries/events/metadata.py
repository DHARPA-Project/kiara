# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from typing import TYPE_CHECKING, Any, Iterable

from kiara.models.events import KiaraEvent
from kiara.models.values.value import Value
from kiara.operations.included_core_operations.metadata import (
    ExtractMetadataDetails,
    ExtractMetadataOperationType,
)

if TYPE_CHECKING:
    from kiara.context import Kiara


class CreateMetadataDestinies(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._skip_internal_types: bool = True

    def supported_event_types(self) -> Iterable[str]:
        return ["value_created", "value_registered"]

    def handle_events(self, *events: KiaraEvent) -> Any:

        for event in events:
            if event.get_event_type() == "value_created":  # type: ignore
                if event.value.is_set:  # type: ignore
                    self.attach_metadata(event.value)  # type: ignore

        for event in events:
            if event.get_event_type() == "value_registered":  # type: ignore
                self.resolve_all_metadata(event.value)  # type: ignore

    def attach_metadata(self, value: Value):

        assert not value.is_stored

        if self._skip_internal_types:

            if value.value_schema.type == "any":
                return
            lineage = self._kiara.type_registry.get_type_lineage(
                value.value_schema.type
            )
            if "any" not in lineage:
                return

        op_type: ExtractMetadataOperationType = self._kiara.operation_registry.get_operation_type("extract_metadata")  # type: ignore
        operations = op_type.get_operations_for_data_type(value.value_schema.type)
        for metadata_key, op in operations.items():
            op_details: ExtractMetadataDetails = op.operation_details  # type: ignore
            input_field_name = op_details.input_field_name
            result_field_name = op_details.result_field_name

            # self._kiara.destiny_registry.add_destiny(
            #     destiny_alias=f"metadata.{metadata_key}",
            #     values={input_field_name: value.value_id},
            #     manifest=op,
            #     result_field_name=result_field_name,
            # )
            self._kiara.data_registry.register_destiny(
                destiny_alias=f"metadata.{metadata_key}",
                values={input_field_name: value.value_id},
                manifest=op,
                result_field_name=result_field_name,
            )

    def resolve_all_metadata(self, value: Value):

        if self._skip_internal_types:

            lineage = self._kiara.type_registry.get_type_lineage(
                value.value_schema.type
            )
            if "any" not in lineage:
                return

        assert not value.is_stored

        # aliases = self._kiara.destiny_registry.get_destiny_aliases_for_value(
        #     value_id=value.value_id
        # )

        aliases = self._kiara.data_registry.get_destiny_aliases_for_value(
            value_id=value.value_id
        )

        for alias in aliases:
            destiny = self._kiara.data_registry.get_registered_destiny(
                value_id=value.value_id, destiny_alias=alias
            )
            # self._kiara.destiny_registry.resolve_destiny(destiny)
            # self._kiara.destiny_registry.attach_as_property(destiny)
            self._kiara.data_registry.attach_destiny_as_property(destiny)
