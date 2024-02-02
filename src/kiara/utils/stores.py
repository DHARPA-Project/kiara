# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Any, Dict, Mapping, Type, Union

if TYPE_CHECKING:
    from kiara.registries import KiaraArchive


def create_new_archive(
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

    if not force_read_only:
        archive_instance.set_archive_metadata_value("archive_alias", archive_alias)

    return archive_instance


def check_external_archive(
    archive: Union[str, "KiaraArchive"], allow_write_access: bool = False
) -> Mapping[str, "KiaraArchive"]:

    from kiara.context.config import KiaraArchiveReference
    from kiara.registries import KiaraArchive

    if isinstance(archive, (KiaraArchive, str)):
        _archives = [archive]
    else:
        _archives = archive

    archive_instances: Dict[str, KiaraArchive] = {}
    for _archive in _archives:

        if isinstance(_archive, KiaraArchive):
            for archive_type in _archives.supported_item_types():
                if archive_type in archive_instances.keys():
                    raise Exception(
                        "Multiple archives of the same type are not supported."
                    )
                archive_instances[archive_type] = _archive
            continue

        loaded = KiaraArchiveReference.load_existing_archive(
            archive_uri=_archive, allow_write_access=allow_write_access
        )

        for _archive_inst in loaded.archives:
            for archive_type in _archive_inst.supported_item_types():
                if archive_type in archive_instances.keys():
                    raise Exception(
                        "Multiple archives of the same type are not supported."
                    )
                archive_instances[archive_type] = _archive_inst

    return archive_instances
