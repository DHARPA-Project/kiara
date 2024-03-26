# -*- coding: utf-8 -*-
from functools import lru_cache
from typing import TYPE_CHECKING, Dict, Union

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.interfaces.python_api.models.info import TypeInfo
    from kiara.models.archives import ArchiveTypeClassesInfo


@lru_cache(maxsize=None)
def find_archive_types(
    alias: Union[str, None] = None, only_for_package: Union[str, None] = None
) -> "ArchiveTypeClassesInfo":

    from kiara.models.archives import ArchiveTypeClassesInfo
    from kiara.utils.class_loading import find_all_archive_types

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

        group = ArchiveTypeClassesInfo(
            group_id=group.group_id, group_title=group.group_alias, item_infos=temp  # type: ignore
        )

    return group
