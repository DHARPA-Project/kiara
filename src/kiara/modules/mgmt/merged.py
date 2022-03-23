# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import logging
import typing
from pathlib import Path
from pydantic import BaseModel, Field, validator

from kiara.modules.mgmt import ModuleManager
from kiara.modules.mgmt.pipelines import PipelineModuleManager
from kiara.modules.mgmt.python_classes import PythonModuleManager

if typing.TYPE_CHECKING:
    from kiara import KiaraModule

log = logging.getLogger("kiara")

try:
    from typing import Literal
except Exception:
    from typing_extensions import Literal  # type: ignore


class MergedModuleManagerConfig(BaseModel):

    module_manager_type: Literal["pipeline"]
    folders: typing.List[str] = Field(
        description="A list of folders that contain pipeline descriptions.",
        default_factory=list,
    )

    @validator("folders", pre=True)
    def _validate_folders(cls, v):

        if isinstance(v, str):
            v = [v]

        assert isinstance(v, typing.Iterable)

        result = []
        for item in v:
            if isinstance(v, Path):
                item = v.as_posix()
            assert isinstance(item, str)
            result.append(item)

        return result


class MergedModuleManager(ModuleManager):
    def __init__(
        self,
        module_managers: typing.Optional[typing.List[str]] = None,
        extra_pipeline_folders: typing.Iterable[str] = None,
        ignore_errors: bool = False,
    ):

        self._modules: typing.Optional[typing.Dict[str, ModuleManager]] = None
        self._module_cls_cache: typing.Dict[str, typing.Type[KiaraModule]] = {}

        self._default_python_mgr: typing.Optional[PythonModuleManager] = None
        self._default_pipeline_mgr: typing.Optional[PipelineModuleManager] = None
        self._custom_pipelines_mgr: typing.Optional[PipelineModuleManager] = None

        if extra_pipeline_folders is None:
            extra_pipeline_folders = []
        self._extra_pipeline_folders: typing.Iterable[str] = extra_pipeline_folders
        self._ignore_errors: bool = ignore_errors

        if module_managers:

            raise NotImplementedError()
            # for mmc in module_managers:
            #     mm = ModuleManager.from_config(mmc)
            #     _mms.append(mm)

        self._module_mgrs: typing.Optional[typing.List[ModuleManager]] = None

    @property
    def module_managers(self) -> typing.Iterable[ModuleManager]:

        if self._module_mgrs is not None:
            return self._module_mgrs

        _mms = [
            self.default_python_module_manager,
            self.default_pipeline_module_manager,
            self.default_custom_pipelines_manager,
        ]
        self._module_mgrs = []
        self._modules = {}
        for mm in _mms:
            self.add_module_manager(mm)

        return self._module_mgrs

    @property
    def module_map(self) -> typing.MutableMapping[str, ModuleManager]:

        if self._modules is not None:
            return self._modules

        # make sure module managers are initialized before this
        # this will also initialize the _modules attribute
        self.module_managers  # noqa
        return self._modules  # type: ignore

    @property
    def default_python_module_manager(self) -> PythonModuleManager:

        if self._default_python_mgr is None:
            self._default_python_mgr = PythonModuleManager()
        return self._default_python_mgr

    @property
    def default_pipeline_module_manager(self) -> PipelineModuleManager:

        if self._default_pipeline_mgr is None:
            self._default_pipeline_mgr = PipelineModuleManager(
                folders=None, ignore_errors=self._ignore_errors
            )
        return self._default_pipeline_mgr

    @property
    def default_custom_pipelines_manager(self) -> PipelineModuleManager:

        if self._custom_pipelines_mgr is None:
            if self._extra_pipeline_folders:
                folders = {"extra": self._extra_pipeline_folders}
            else:
                folders = {}
            self._custom_pipelines_mgr = PipelineModuleManager(
                folders=folders, ignore_errors=self._ignore_errors
            )
        return self._custom_pipelines_mgr

    @property
    def module_type_names(self) -> typing.List[str]:
        """Return the names of all available modules"""
        return self.get_module_type_names()

    def get_module_type_names(self) -> typing.List[str]:
        return sorted(set(self.module_map.keys()))

    @property
    def module_types(self) -> typing.Mapping[str, typing.Type["KiaraModule"]]:
        for n in self.module_map.keys():
            self._module_cls_cache[n] = self.get_module_class(n)
        return self._module_cls_cache

    @property
    def available_non_pipeline_module_types(self) -> typing.List[str]:
        """Return the names of all available pipeline-type modules."""

        return [
            module_type
            for module_type in self.module_type_names
            if module_type != "pipeline"
            and not self.get_module_class(module_type).is_pipeline()
        ]

    @property
    def available_pipeline_module_types(self) -> typing.List[str]:
        """Return the names of all available pipeline-type modules."""

        return [
            module_type
            for module_type in self.module_type_names
            if module_type != "pipeline"
            and self.get_module_class(module_type).is_pipeline()
        ]

    # def get_module_types(self) -> typing.Iterable[str]:
    #
    #     return self.available_module_types

    def is_pipeline_module(self, module_type: str):

        cls = self.get_module_class(module_type=module_type)
        return cls.is_pipeline()

    def add_module_manager(self, module_manager: ModuleManager):

        if self._module_mgrs is None:
            self.module_managers  # noqa

        for module_type in module_manager.get_module_type_names():
            if module_type in self.module_map.keys():
                log.warning(
                    f"Duplicate module name '{module_type}'. Ignoring all but the first."
                )
                continue
            self.module_map[module_type] = module_manager

        self._module_mgrs.append(module_manager)  # type: ignore
        self.data_types = None

    def register_pipeline_description(
        self,
        data: typing.Union[Path, str, typing.Mapping[str, typing.Any]],
        module_type_name: typing.Optional[str] = None,
        namespace: typing.Optional[str] = None,
        raise_exception: bool = False,
    ) -> typing.Optional[str]:

        # making sure the module_map attribute is populated
        self.module_map  # noqa
        name = self.default_custom_pipelines_manager.register_pipeline(
            data=data, module_type_name=module_type_name, namespace=namespace
        )

        if name in self.module_map.keys():
            if raise_exception:
                raise Exception(f"Duplicate module name: {name}")
            log.warning(f"Duplicate module name '{name}'. Ignoring all but the first.")
            return None
        else:
            self.module_map[name] = self.default_custom_pipelines_manager

            return name

    def get_module_class(self, module_type: str) -> typing.Type["KiaraModule"]:

        if module_type == "pipeline":
            from kiara import PipelineModule

            return PipelineModule

        mm = self.module_map.get(module_type, None)
        if mm is None:
            raise Exception(f"No module '{module_type}' available.")

        if module_type in self._module_cls_cache.keys():
            return self._module_cls_cache[module_type]

        cls = mm.get_module_class(module_type)
        if not hasattr(cls, "_module_type_name"):
            raise Exception(
                f"Class does not have a '_module_type_name' attribute: {cls}"
            )

        # assert module_type.endswith(cls._module_type_name)  # type: ignore
        #
        # if cls._module_type_name != "pipeline" and cls._module_type_name != module_type:  # type: ignore
        #     raise Exception(
        #         f"Can't create module class '{cls}', it already has a _module_type_name attribute and it's different to the module name '{module_type}': {cls._module_type_id}"  # type: ignore
        #     )

        self._module_cls_cache[module_type] = cls

        return cls

    # def get_module_instance(
    #     self,
    #     kiara: "Kiara",
    #     module_type: str,
    #     module_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    # ) -> "KiaraModule":
    #
    #     mm = self.module_map.get(module_type, None)
    #     if mm is None:
    #         raise Exception(
    #             f"No module '{module_type}' registered. Available modules: {', '.join(self.module_type_names)}"
    #         )
    #
    #     _ = self.get_module_class(
    #         module_type
    #     )  # just to make sure the _module_type_id attribute is added
    #
    #     mi = mm.create_module_config()
    #     return mm.create_module(
    #         id=id,
    #         parent_id=parent_id,
    #         module_type=module_type,
    #         module_config=module_config,
    #         kiara=kiara,
    #     )

    def find_modules_for_package(
        self,
        package_name: str,
        include_core_modules: bool = True,
        include_pipelines: bool = True,
    ) -> typing.Dict[str, typing.Type["KiaraModule"]]:

        result = {}
        for module_type in self.module_type_names:

            if module_type == "pipeline":
                continue
            module_cls = self.get_module_class(module_type)

            module_package = module_cls.get_type_metadata().context.labels.get(
                "package", None
            )
            if module_package != package_name:
                continue
            if module_cls.is_pipeline():
                if include_pipelines:
                    result[module_type] = module_cls
            else:
                if include_core_modules:
                    result[module_type] = module_cls

        return result
