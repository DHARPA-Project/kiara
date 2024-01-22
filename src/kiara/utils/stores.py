# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Any, Type

if TYPE_CHECKING:
    from kiara.registries import KiaraArchive


def create_new_store(
    archive_alias: str,
    store_base_path: str,
    store_type: str,
    allow_write_access: bool = False,
    **kwargs: Any,
) -> "KiaraArchive":

    from kiara.utils.class_loading import find_all_archive_types

    archive_types = find_all_archive_types()

    archive_cls: Type[KiaraArchive] = archive_types.get(store_type, None)
    if archive_cls is None:
        raise Exception(
            f"Can't create context: no archive type '{store_type}' available. Available types: {', '.join(archive_types.keys())}"
        )

    # config = archive_cls.create_store_config_instance(config=store_config)
    config = archive_cls.create_new_store_config(store_base_path, **kwargs)
    # TODO: make sure this constructor always exists?

    force_read_only = not allow_write_access

    archive_instance = archive_cls(archive_alias=archive_alias, archive_config=config, force_read_only=force_read_only)  # type: ignore
    return archive_instance
