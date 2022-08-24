# -*- coding: utf-8 -*-

#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from pydantic import Field
from typing import TYPE_CHECKING, Any, Dict, Literal, Union

from kiara.interfaces.python_api.models.info import TypeInfo
from kiara.models.archives import ArchiveTypeClassesInfo
from kiara.models.runtime_environment import RuntimeEnvironment
from kiara.models.values.value_metadata import MetadataTypeClassesInfo
from kiara.utils.class_loading import find_all_archive_types
from kiara.utils.metadata import find_metadata_models

if TYPE_CHECKING:
    from kiara.context import Kiara


def find_archive_types(
    alias: Union[str, None] = None, only_for_package: Union[str, None] = None
) -> ArchiveTypeClassesInfo:

    archive_types = find_all_archive_types()

    kiara: Kiara = None  # type: ignore
    group: ArchiveTypeClassesInfo = ArchiveTypeClassesInfo.create_from_type_items(  # type: ignore
        kiara=kiara, group_title=alias, **archive_types
    )

    if only_for_package:
        temp: Dict[str, TypeInfo] = {}
        for key, info in group.item_infos.items():
            if info.context.labels.get("package") == only_for_package:
                temp[key] = info  # type: ignore

        group = ArchiveTypeClassesInfo.construct(
            group_id=group.group_id, group_alias=group.group_alias, item_infos=temp  # type: ignore
        )

    return group


class KiaraTypesRuntimeEnvironment(RuntimeEnvironment):

    _kiara_model_id = "info.runtime.kiara_types"

    environment_type: Literal["kiara_types"]
    archive_types: ArchiveTypeClassesInfo = Field(
        description="The available implemented store types."
    )
    metadata_types: MetadataTypeClassesInfo = Field(
        description="The available metadata types."
    )

    @classmethod
    def retrieve_environment_data(cls) -> Dict[str, Any]:

        result: Dict[str, Any] = {}
        result["metadata_types"] = find_metadata_models()
        result["archive_types"] = find_archive_types()

        return result
