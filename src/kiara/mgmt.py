# -*- coding: utf-8 -*-
import abc
import logging
import os
import typing
from pathlib import Path

from kiara.defaults import (
    DEFAULT_EXCLUDE_DIRS,
    MODULE_TYPE_NAME_KEY,
    USER_PIPELINES_FOLDER,
    VALID_PIPELINE_FILE_EXTENSIONS,
)
from kiara.modules.pipelines import create_pipeline_class
from kiara.utils import (
    find_kiara_modules,
    find_kiara_pipeline_folders,
    get_data_from_file,
)

if typing.TYPE_CHECKING:
    from kiara import Kiara
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
        kiara: "Kiara",
        id: str,
        module_type: str,
        module_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        parent_id: typing.Optional[str] = None,
    ) -> "KiaraModule":

        module_cls = self.get_module_class(module_type)

        module = module_cls(
            id=id, parent_id=parent_id, module_config=module_config, kiara=kiara
        )
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
            raise ValueError(f"No module of type '{module_type}' available.")
        return cls

    def get_module_types(self) -> typing.Iterable[str]:
        return self._module_classes.keys()


class PipelineModuleManager(ModuleManager):
    def __init__(
        self, folders: typing.Optional[typing.Iterable[typing.Union[str, Path]]] = None
    ):

        if folders is None:
            folders = list(find_kiara_pipeline_folders().values())
            if os.path.exists(USER_PIPELINES_FOLDER):
                folders.append(USER_PIPELINES_FOLDER)

        self._pipeline_desc_folders: typing.List[Path] = []
        self._pipeline_descs: typing.Dict[str, typing.Mapping[str, typing.Any]] = {}
        self._cached_classes: typing.Dict[str, typing.Type[PipelineModule]] = {}

        for folder in folders:
            self.add_pipelines_path(folder)

    def _get_pipeline_details_from_path(
        self,
        path: typing.Union[str, Path],
        module_type_name: typing.Optional[str] = None,
    ):

        if isinstance(path, str):
            path = Path(os.path.expanduser(path))

        if not path.is_file():
            raise Exception(
                f"Can't add pipeline description '{path.as_posix()}': not a file"
            )

        data = get_data_from_file(path)

        if not data:
            raise Exception(
                f"Can't register pipeline file '{path.as_posix()}': no content."
            )

        if module_type_name:
            data[MODULE_TYPE_NAME_KEY] = module_type_name

        filename = path.name

        if not isinstance(data, typing.Mapping):
            raise Exception("Not a dictionary type.")
        name = data.get(MODULE_TYPE_NAME_KEY, None)
        if name is None:
            name = filename.split(".", maxsplit=1)[0]

        result = {"data": data, "source": path.as_posix(), "source_type": "file"}
        return (name, result)

    def register_pipeline(
        self,
        data: typing.Union[str, Path, typing.Mapping[str, typing.Any]],
        module_type_name: typing.Optional[str] = None,
    ) -> str:
        """Register a pipeline description to the pipeline pool.

        Arguments:
            data: the pipeline data (a dict, or a path to a file)
            module_type_name: the type name this pipeline should be registered as
        """

        if isinstance(data, str):
            data = Path(os.path.expanduser(data))

        if isinstance(data, Path):
            _name, _data = self._get_pipeline_details_from_path(data)
        elif isinstance(data, typing.Mapping):
            _data = dict(data)
            if module_type_name:
                _data[MODULE_TYPE_NAME_KEY] = module_type_name

            _name = _data.get(MODULE_TYPE_NAME_KEY, None)
            if not _name:
                raise Exception(
                    f"Can't register pipeline, no module type name available: {data}"
                )

            _data = {"data": _data, "source": data, "source_type": "dict"}
        else:
            raise Exception(
                f"Can't register pipeline, must be dict-like data, not {type(data)}"
            )

        if _name in self._pipeline_descs.keys():
            raise Exception(
                f"Can't register pipeline: duplicate workflow name '{_name}'"
            )

        self._pipeline_descs[_name] = _data

        return _name

    def add_pipelines_path(self, path: typing.Union[str, Path]) -> typing.Iterable[str]:
        """Add a pipeline description file or folder containing some to this manager.

        Arguments:
            path: the path to a pipeline description file, or folder which contains some
        Returns:
            a list of module type names that were added
        """

        if isinstance(path, str):
            path = Path(os.path.expanduser(path))

        if not path.exists():
            log.warning(f"Can't add pipeline path '{path}': path does not exist")
            return []

        elif path.is_dir():

            files: typing.Dict[str, typing.Mapping[str, typing.Any]] = {}
            for root, dirnames, filenames in os.walk(path, topdown=True):

                dirnames[:] = [d for d in dirnames if d not in DEFAULT_EXCLUDE_DIRS]

                for filename in [
                    f
                    for f in filenames
                    if os.path.isfile(os.path.join(root, f))
                    and any(f.endswith(ext) for ext in VALID_PIPELINE_FILE_EXTENSIONS)
                ]:

                    try:

                        full_path = os.path.join(root, filename)
                        name, data = self._get_pipeline_details_from_path(full_path)
                        if name in files.keys():
                            raise Exception(f"Duplicate workflow name: {name}")

                        files[name] = data
                    except Exception as e:
                        log.warning(
                            f"Ignoring invalid pipeline file '{full_path}': {e}"
                        )
        elif path.is_file():
            name, data = self._get_pipeline_details_from_path(path)
            files = {name: data}

        for name in files.keys():
            if name in self._pipeline_descs.keys():
                raise Exception(f"Duplicate workflow name: {name}")

        self._pipeline_descs.update(files)
        return files.keys()

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
