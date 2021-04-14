# -*- coding: utf-8 -*-
import abc
import logging
import os
import typing
from pathlib import Path

from kiara.defaults import (
    DEFAULT_EXCLUDE_DIRS,
    KIARA_RESOURCES_FOLDER,
    MODULE_TYPE_NAME_KEY,
    VALID_PIPELINE_FILE_EXTENSIONS,
)
from kiara.modules.pipelines import create_pipeline_class
from kiara.utils import find_kiara_modules, get_data_from_file

if typing.TYPE_CHECKING:
    from kiara.config import KiaraModuleConfig
    from kiara.module import KiaraModule
    from kiara.pipeline.module import PipelineModule

log = logging.getLogger("kiara")


# extensions
# ------------------------------------------------------------------------


class ModuleManager(abc.ABC):
    @abc.abstractmethod
    def get_module_types(self) -> typing.Iterable[str]:
        pass

    @abc.abstractmethod
    def get_module_class(self, module_type: str) -> typing.Type["KiaraModule"]:
        pass

    def create_module_config(
        self, module_type: str, module_config: typing.Mapping[str, typing.Any]
    ) -> "KiaraModuleConfig":

        cls = self.get_module_class(module_type)
        config = cls._config_cls(**module_config)

        return config

    def create_module(
        self,
        id: str,
        module_type: str,
        module_config: typing.Mapping[str, typing.Any] = None,
        parent_id: typing.Optional[str] = None,
    ) -> "KiaraModule":

        module_cls = self.get_module_class(module_type)

        module = module_cls(id=id, parent_id=parent_id, module_config=module_config)
        return module


class PythonModuleManager(ModuleManager):
    def __init__(self, **module_classes: typing.Type["KiaraModule"]):

        if not module_classes:
            module_classes = find_kiara_modules()

        self._module_classes: typing.Mapping[
            str, typing.Type[KiaraModule]
        ] = module_classes

    def get_module_class(self, module_type: str) -> typing.Type["KiaraModule"]:

        cls = self._module_classes.get(module_type, None)
        if cls is None:
            raise ValueError(f"No module of type '{module_type}' availble.")
        return cls

    def get_module_types(self) -> typing.Iterable[str]:
        return self._module_classes.keys()


class PipelineModuleManager(ModuleManager):
    def __init__(self, *folders: typing.Union[str, Path]):

        if not folders:
            folders = (os.path.join(KIARA_RESOURCES_FOLDER, "pipelines"),)

        self._pipeline_desc_folders: typing.List[Path] = []
        self._pipeline_descs: typing.Dict[str, typing.Mapping[str, typing.Any]] = {}
        self._cached_classes: typing.Dict[str, typing.Type[PipelineModule]] = {}

        for folder in folders:
            self.add_pipelines_folder(folder)

    def add_pipelines_folder(self, folder: typing.Union[str, Path]):

        if isinstance(folder, str):
            folder = Path(os.path.expanduser(folder))
        if not folder.is_dir():
            raise Exception(f"Pipeline folder path not a directory: {folder}")

        files: typing.Dict[str, typing.Mapping[str, typing.Any]] = {}
        for root, dirnames, filenames in os.walk(folder, topdown=True):

            dirnames[:] = [d for d in dirnames if d not in DEFAULT_EXCLUDE_DIRS]

            for filename in [
                f
                for f in filenames
                if os.path.isfile(os.path.join(root, f))
                and any(f.endswith(ext) for ext in VALID_PIPELINE_FILE_EXTENSIONS)
            ]:

                try:

                    path = os.path.join(root, filename)
                    data = get_data_from_file(path)

                    if not data:
                        raise Exception("No content.")
                    if not isinstance(data, typing.Mapping):
                        raise Exception("Not a dictionary type.")
                    name = data.get(MODULE_TYPE_NAME_KEY, None)
                    if name is None:
                        name = filename.split(".", maxsplit=1)[0]

                    if name in files.keys():
                        raise Exception(f"Duplicate workflow name: {name}")
                    if name in self._pipeline_descs.keys():
                        raise Exception(f"Duplicate workflow name: {name}")
                    files[name] = {"data": data, "source": path, "source_type": "file"}
                except Exception as e:
                    log.warning(f"Ignoring invalid pipeline file '{path}': {e}")

        self._pipeline_descs.update(files)

    @property
    def pipeline_descs(self) -> typing.Mapping[str, typing.Mapping[str, typing.Any]]:
        return self._pipeline_descs

    def get_module_class(self, module_type: str) -> typing.Type["PipelineModule"]:

        if module_type in self._cached_classes.keys():
            return self._cached_classes[module_type]

        desc = self._pipeline_descs.get(module_type, None)
        if desc is None:
            raise Exception(f"No pipeline with name '{module_type}' available.")

        cls_name = "".join(x.capitalize() or "_" for x in module_type.split("_"))
        cls = create_pipeline_class(cls_name, desc["data"])

        self._cached_classes[module_type] = cls
        return self._cached_classes[module_type]

    def get_module_types(self) -> typing.Iterable[str]:
        return self._pipeline_descs.keys()


class WorkflowManager(object):
    def __init__(self, module_manager: PythonModuleManager):

        self._module_mgr: PythonModuleManager = module_manager

    def create_workflow(
        self,
        workflow_id: str,
        config: typing.Union[str, typing.Mapping[str, typing.Any]],
    ):

        if isinstance(config, typing.Mapping):
            raise NotImplementedError()
