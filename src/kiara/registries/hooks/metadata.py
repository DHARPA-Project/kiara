# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Iterable

from kiara.models.events.data_registry import RegistryEvent
from kiara.models.values.value import Value
from kiara.registries.data import DataEventHook

if TYPE_CHECKING:
    from kiara.kiara import Kiara


class CreateMetadataDestinies(DataEventHook):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara

    def get_subscribed_event_types(self) -> Iterable[str]:
        return ["value_created", "value_pre_store"]

    def process_hook(self, event: RegistryEvent):

        # if not value._data_type_known:
        #     return

        if event.event_type == "value_created":  # type: ignore
            self.attach_metadata(event.value)
        elif event.event_type == "value_pre_store":  # type: ignore
            self.resolve_all_metadata(event.value)

    def attach_metadata(self, value: Value):

        op_type: ExtractMetadataOperationType = self._kiara.operation_registry.get_operation_type("extract_metadata")  # type: ignore
        operations = op_type.get_operations_for_data_type(value.value_schema.type)
        for metadata_key, op in operations.items():
            op_details: ExtractMetadataDetails = op.operation_details  # type: ignore
            input_field_name = op_details.input_field_name
            result_field_name = op_details.result_field_name
            self._kiara.data_registry.add_destiny(
                category="metadata",
                key=metadata_key,
                values={input_field_name: value},
                manifest=op,
                result_field_name=result_field_name,
            )

    def resolve_all_metadata(self, value: Value):

        self._kiara.data_registry.resolve_destinies_for_value(
            value=value, category="metadata"
        )
