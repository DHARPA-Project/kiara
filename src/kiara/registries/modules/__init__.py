# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Base module for code that handles the import and management of [KiaraModule][kiara.module.KiaraModule] sub-classes."""

import structlog
from typing import TYPE_CHECKING, Dict, Iterable, Mapping, Optional, Type, Union

from kiara.models.module import KiaraModuleTypeInfo, ModuleTypeClassesInfo
from kiara.models.module.manifest import Manifest

if TYPE_CHECKING:
    from kiara.modules import KiaraModule

logget = structlog.getLogger()


class ModuleRegistry(object):
    def __init__(self):

        self._cached_modules: Dict[str, Dict[int, KiaraModule]] = {}

        from kiara.utils.class_loading import find_all_kiara_modules

        module_classes = find_all_kiara_modules()

        self._module_classes: Mapping[str, Type[KiaraModule]] = {}
        self._module_class_metadata: Dict[str, KiaraModuleTypeInfo] = {}

        for k, v in module_classes.items():
            self._module_classes[k] = v

    @property
    def module_types(self) -> Mapping[str, Type["KiaraModule"]]:
        return self._module_classes

    def get_module_class(self, module_type: str) -> Type["KiaraModule"]:

        cls = self._module_classes.get(module_type, None)
        if cls is None:
            raise ValueError(f"No module of type '{module_type}' available.")
        return cls

    def get_module_type_names(self) -> Iterable[str]:
        return self._module_classes.keys()

    def get_module_type_metadata(self, type_name: str) -> KiaraModuleTypeInfo:

        md = self._module_class_metadata.get(type_name, None)
        if md is None:
            md = KiaraModuleTypeInfo.create_from_type_class(
                self.get_module_class(module_type=type_name)
            )
            self._module_class_metadata[type_name] = md
        return self._module_class_metadata[type_name]

    def get_context_metadata(
        self, alias: Optional[str] = None, only_for_package: Optional[str] = None
    ) -> ModuleTypeClassesInfo:

        result = {}
        for type_name in self.module_types.keys():
            md = self.get_module_type_metadata(type_name=type_name)
            if only_for_package:
                if md.context.labels.get("package") == only_for_package:
                    result[type_name] = md
            else:
                result[type_name] = md

        return ModuleTypeClassesInfo.construct(group_alias=alias, type_infos=result)  # type: ignore

    def create_module(self, manifest: Union[Manifest, str]) -> "KiaraModule":
        """Create a [KiaraModule][kiara.module.KiaraModule] object from a module configuration.

        Arguments:
            manifest: the module configuration
        """

        if isinstance(manifest, str):
            manifest = Manifest.construct(module_type=manifest, module_config={})

        if self._cached_modules.setdefault(manifest.module_type, {}).get(
            manifest.manifest_hash, None
        ):
            return self._cached_modules[manifest.module_type][manifest.manifest_hash]

        if manifest.module_type in self.get_module_type_names():

            m_cls: Type[KiaraModule] = self.get_module_class(manifest.module_type)
            m_hash = m_cls._calculate_module_hash(manifest.module_config)

            kiara_module = m_cls(module_config=manifest.module_config)
            assert (
                kiara_module.module_instance_hash == m_hash
            )  # TODO: might not be necessary? Leaving it in here for now, to see if it triggers at any stage.
        else:
            raise Exception(
                f"Invalid module type '{manifest.module_type}'. Available type names: {', '.join(self.get_module_type_names())}"
            )

        return kiara_module
