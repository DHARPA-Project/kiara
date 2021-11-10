# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import json
from jsonschema import validate

from kiara import Kiara
from kiara.operations.extract_metadata import ExtractMetadataOperationType


def test_extract_metadata_all_available_data(preseeded_data_store: Kiara):

    op_type: ExtractMetadataOperationType = (
        preseeded_data_store.operation_mgmt.operation_types["extract_metadata"]
    )
    for value_id in preseeded_data_store.data_store.value_ids:
        value = preseeded_data_store.data_store.get_value_obj(value_id)

        ops = op_type.get_all_operations_for_type(value.type_name)

        for op in ops.values():
            inputs = {value.type_name: value}
            result = op.module.run(**inputs)

            schema = json.loads(result["metadata_item_schema"].get_value_data())
            item = result["metadata_item"].get_value_data()

            validate(instance=item, schema=schema)
