# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


from kiara.context import Kiara
from kiara.models.values.value_metadata import ValueMetadata
from kiara.operations.included_core_operations.metadata import (
    ExtractMetadataOperationType,
)


def test_extract_metadata_all_available_data(presseeded_data_store_minimal: Kiara):

    op_type: ExtractMetadataOperationType = (  # type: ignore
        presseeded_data_store_minimal.operation_registry.operation_types[
            "extract_metadata"
        ]
    )

    for (
        value_id
    ) in presseeded_data_store_minimal.data_registry.retrieve_all_available_value_ids():

        value = presseeded_data_store_minimal.data_registry.get_value(value_id)
        ops = op_type.get_operations_for_data_type(value.value_schema.type)

        if not value.is_set:
            continue

        for op in ops.values():

            inputs = {"value": value}
            result = op.run(kiara=presseeded_data_store_minimal, inputs=inputs)

            md = result.get_value_data("value_metadata")
            assert isinstance(md, ValueMetadata)
            # TODO: validate schema
            # schema = json.loads(result["metadata_item_schema"].get_value_data())
            # item = result["metadata_item"].get_value_data()
            #
            # validate(instance=item, schema=schema)
