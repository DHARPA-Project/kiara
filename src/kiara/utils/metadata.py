# -*- coding: utf-8 -*-
from typing import Optional

from kiara.models.values.value_metadata import MetadataTypeClassesInfo
from kiara.utils.class_loading import find_all_value_metadata_models


def find_metadata_models(
    alias: Optional[str] = None, only_for_package: Optional[str] = None
) -> MetadataTypeClassesInfo:

    models = find_all_value_metadata_models()

    group = MetadataTypeClassesInfo.create_from_type_items(group_alias=alias, **models)

    if only_for_package:
        temp = {}
        for key, info in group.items():
            if info.context.labels.get("package") == only_for_package:
                temp[key] = info

        group = MetadataTypeClassesInfo.construct(
            group_id=group.group_id, group_alias=group.group_alias, type_infos=temp
        )

    return group
