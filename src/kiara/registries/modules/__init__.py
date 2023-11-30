# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Base module for code that handles the import and management of [KiaraModule][kiara.module.KiaraModule] sub-classes."""

from typing import TYPE_CHECKING, Dict, Iterable, Mapping, Type, Union

import structlog
from multiformats import CID

from kiara.exceptions import InvalidManifestException
from kiara.interfaces.python_api.models.info import ModuleTypeInfo, ModuleTypesInfo
from kiara.models.module.manifest import Manifest
from kiara.utils import is_debug

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.modules import KiaraModule

logget = structlog.getLogger()


class ModuleRegistry(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara

        self._cached_modules: Dict[str, Dict[CID, KiaraModule]] = {}

        from kiara.utils.class_loading import find_all_kiara_modules

        module_classes = find_all_kiara_modules()

        self._module_classes: Mapping[str, Type[KiaraModule]] = {}
        self._module_class_metadata: Dict[str, ModuleTypeInfo] = {}

        for k, v in module_classes.items():
            self._module_classes[k] = v

    @property
    def module_types(self) -> Mapping[str, Type["KiaraModule"]]:
        return self._module_classes

    def get_module_class(self, module_type: str) -> Type["KiaraModule"]:

        cls = self._module_classes.get(module_type, None)
        if cls is None:
            raise InvalidManifestException(
                f"No module of type '{module_type}' available.",
                module_type=module_type,
                available_module_types=self._module_classes.keys(),
            )
        return cls

    def get_module_type_names(self) -> Iterable[str]:
        return self._module_classes.keys()

    def get_module_type_metadata(self, type_name: str) -> ModuleTypeInfo:

        md = self._module_class_metadata.get(type_name, None)
        if md is None:
            md = ModuleTypeInfo.create_from_type_class(
                type_cls=self.get_module_class(module_type=type_name), kiara=self._kiara
            )
            self._module_class_metadata[type_name] = md
        return self._module_class_metadata[type_name]

    def get_context_metadata(
        self, alias: Union[str, None] = None, only_for_package: Union[str, None] = None
    ) -> ModuleTypesInfo:

        result = {}
        for type_name in self.module_types.keys():
            md = self.get_module_type_metadata(type_name=type_name)
            if only_for_package:
                if md.context.labels.get("package") == only_for_package:
                    result[type_name] = md
            else:
                result[type_name] = md

        return ModuleTypesInfo(group_title=alias, item_infos=result)  # type: ignore

    def resolve_manifest(self, manifest: Manifest) -> Manifest:
        """Returns a cloned manifest with resolved module config."""

        if manifest.is_resolved:
            return manifest.model_copy()

        m_cls = self.get_module_class(manifest.module_type)

        try:
            resolved = m_cls._resolve_module_config(**manifest.module_config)
            resolved_dict = resolved.model_dump()
            manifest_clone = manifest.model_copy(
                update={"module_config": resolved_dict, "is_resolved": True}
            )
            return manifest_clone

        except Exception as e:
            if is_debug():
                import traceback

                traceback.print_exc()

            raise InvalidManifestException(
                f"Error while resolving module config for module '{manifest.module_type}': {e}",
                module_type=manifest.module_type,
                module_config=manifest.module_config,
                parent=e,
            )

    def create_module(self, manifest: Union[Manifest, str]) -> "KiaraModule":
        """
        Create a [KiaraModule][kiara.module.KiaraModule] object from a module configuration.

        Arguments:
        ---------
            manifest: the module configuration
        """
        if isinstance(manifest, str):
            manifest = Manifest(module_type=manifest, module_config={})

        m_cls: Type[KiaraModule] = self.get_module_class(manifest.module_type)

        if not manifest.is_resolved:
            manifest = self.resolve_manifest(manifest)

        if self._cached_modules.setdefault(manifest.module_type, {}).get(
            manifest.instance_cid, None
        ):
            return self._cached_modules[manifest.module_type][manifest.instance_cid]

        if manifest.module_type in self.get_module_type_names():
            kiara_module = m_cls(module_config=manifest.module_config)
            kiara_module._manifest_cache = self.resolve_manifest(manifest)

        else:
            raise Exception(
                f"Invalid module type '{manifest.module_type}'. Available type names: {', '.join(self.get_module_type_names())}"
            )

        return kiara_module
