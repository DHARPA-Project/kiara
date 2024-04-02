# -*- coding: utf-8 -*-
from functools import lru_cache

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
from typing import TYPE_CHECKING, Dict, Type, Union

from kiara.models.values.value_metadata import ValueMetadata
from kiara.registries.models import ModelRegistry

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.interfaces.python_api.models.info import (
        MetadataTypeClassesInfo,
    )


@lru_cache()
def find_metadata_models(
    alias: Union[str, None] = None, only_for_package: Union[str, None] = None
) -> "MetadataTypeClassesInfo":

    from kiara.interfaces.python_api.models.info import MetadataTypeClassesInfo

    model_registry = ModelRegistry.instance()
    _group = model_registry.get_models_of_type(ValueMetadata)  # type: ignore

    classes: Dict[str, Type[ValueMetadata]] = {}
    for model_id, info in _group.item_infos.items():
        model_cls = info.python_class.get_class()  # type: ignore
        classes[model_id] = model_cls

    group: MetadataTypeClassesInfo = MetadataTypeClassesInfo.create_from_type_items(group_title=alias, kiara=None, **classes)  # type: ignore

    if only_for_package:
        temp = {}
        for key, _info in group.item_infos.items():
            if _info.context.labels.get("package") == only_for_package:
                temp[key] = _info

        group = MetadataTypeClassesInfo(
            group_id=group.instance_id, group_title=group.group_alias, item_infos=temp  # type: ignore
        )

    return group


def get_metadata_model_for_data_type(
    kiara: "Kiara", data_type: str
) -> "MetadataTypeClassesInfo":
    """
    Return all available metadata extract operations for the provided type (and it's parent types).

    Arguments:
    ---------
        data_type: the value type

    Returns:
    -------
        a mapping with the metadata type as key, and the operation as value
    """

    from kiara.interfaces.python_api.models.info import MetadataTypeClassesInfo

    # TODO: add models for parent types?
    # lineage = set(kiara.type_registry.get_type_lineage(data_type_name=data_type))

    model_registry = ModelRegistry.instance()
    all_metadata_models = model_registry.get_models_of_type(ValueMetadata)

    matching_types = {}

    for name, model_info in all_metadata_models.item_infos.items():

        metadata_cls: Type[ValueMetadata] = model_info.python_class.get_class()
        supported = metadata_cls.retrieve_supported_data_types()
        if data_type in supported:
            matching_types[name] = metadata_cls

    result: MetadataTypeClassesInfo = MetadataTypeClassesInfo.create_from_type_items(
        kiara=kiara,
        group_title=f"Metadata models for type '{data_type}'",
        **matching_types,
    )

    return result
