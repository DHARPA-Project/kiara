# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Type, Union

from kiara.defaults import ARCHIVE_NAME_MARKER

if TYPE_CHECKING:
    from kiara.registries import KiaraArchive


def create_new_archive(
    archive_name: str,
    store_base_path: str,
    store_type: str,
    allow_write_access: bool = False,
    set_archive_name_metadata: bool = True,
    **kwargs: Any,
) -> "KiaraArchive":
    """Create a new archive instance of the specified type.

    Arguments:
        archive_name: Name of the archive.
        store_base_path: Base path for the archive.
        store_type: Type of the archive.
        allow_write_access: Whether write access should be allowed.
        set_archive_name_metadata: Whether to set the archive name as metadata within the archive.
        **kwargs: Additional arguments to pass to the archive config constructor.
    """

    from kiara.utils.class_loading import find_all_archive_types

    archive_types = find_all_archive_types()

    archive_cls: Union[Type[KiaraArchive], None] = archive_types.get(store_type, None)
    if archive_cls is None:
        raise Exception(
            f"Can't create context: no archive type '{store_type}' available. Available types: {', '.join(archive_types.keys())}"
        )

    # config = archive_cls.create_store_config_instance(config=store_config)
    config = archive_cls.create_new_store_config(store_base_path, **kwargs)
    # TODO: make sure this constructor always exists?

    force_read_only = not allow_write_access

    archive_instance = archive_cls(archive_name=archive_name, archive_config=config, force_read_only=force_read_only)  # type: ignore

    if not force_read_only and set_archive_name_metadata:
        archive_instance.set_archive_metadata_value(ARCHIVE_NAME_MARKER, archive_name)

    return archive_instance


def check_external_archive(
    archive: Union[str, "KiaraArchive", Iterable[Union["KiaraArchive", str]]],
    allow_write_access: bool = False,
    archive_name: Union[str, None] = None,
) -> Mapping[str, "KiaraArchive"]:

    from kiara.context.config import KiaraArchiveReference
    from kiara.registries import KiaraArchive

    if isinstance(archive, (KiaraArchive, str)):
        _archives: List[Union[str, KiaraArchive]] = [archive]
    else:
        _archives = list(archive)

    archive_instances: Dict[str, KiaraArchive] = {}

    for _archive in _archives:

        if isinstance(_archive, KiaraArchive):
            for archive_type in _archive.supported_item_types():
                if archive_type in archive_instances.keys():
                    raise Exception(
                        "Multiple archives of the same type are not supported."
                    )
                if archive_name and _archive.archive_name != archive_name:
                    raise Exception(
                        f"Archive alias '{_archive.archive_name}' does not match expected alias '{archive_name}'"
                    )
                archive_instances[archive_type] = _archive
            continue

        loaded = KiaraArchiveReference.load_existing_archive(
            archive_uri=_archive,
            allow_write_access=allow_write_access,
            archive_name=archive_name,
        )

        for _archive_inst in loaded.archives:
            for archive_type in _archive_inst.supported_item_types():
                if archive_type in archive_instances.keys():
                    raise Exception(
                        "Multiple archives of the same type are not supported."
                    )
                archive_instances[archive_type] = _archive_inst

    return archive_instances
