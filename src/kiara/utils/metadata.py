# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from typing import Dict, Optional, Type

from kiara.models.values.value_metadata import MetadataTypeClassesInfo, ValueMetadata
from kiara.registries.models import ModelRegistry


def find_metadata_models(
    alias: Optional[str] = None, only_for_package: Optional[str] = None
) -> MetadataTypeClassesInfo:

    model_registry = ModelRegistry.instance()
    _group = model_registry.get_models_of_type(ValueMetadata)  # type: ignore

    classes: Dict[str, Type[ValueMetadata]] = {}
    for model_id, info in _group.items():
        classes[model_id] = info.python_class.get_class()  # type: ignore

    group: MetadataTypeClassesInfo = MetadataTypeClassesInfo.create_from_type_items(group_alias=alias, **classes)  # type: ignore

    if only_for_package:
        temp = {}
        for key, info in group.items():
            if info.context.labels.get("package") == only_for_package:
                temp[key] = info

        group = MetadataTypeClassesInfo.construct(
            group_id=group.instance_id, group_alias=group.group_alias, item_infos=temp  # type: ignore
        )

    return group
