# -*- coding: utf-8 -*-
import uuid
from typing import TYPE_CHECKING, Any, Mapping, Type, Union

from pydantic import BaseModel

if TYPE_CHECKING:
    from kiara.registries import KiaraArchive


def create_store(
    archive_id: uuid.UUID,
    store_type: str,
    store_config: Union[Mapping[str, Any], BaseModel],
    allow_write_access: bool = False,
):

    from kiara.utils.class_loading import find_all_archive_types

    archive_types = find_all_archive_types()

    archive_cls: Type[KiaraArchive] = archive_types.get(store_type, None)
    if archive_cls is None:
        raise Exception(
            f"Can't create context: no archive type '{store_type}' available. Available types: {', '.join(archive_types.keys())}"
        )

    config = archive_cls.create_config(config=store_config)
    # TODO: make sure this constructor always exists?

    force_read_only = not allow_write_access

    archive_instance = archive_cls(archive_id=archive_id, config=config, force_read_only=force_read_only)  # type: ignore
    return archive_instance
