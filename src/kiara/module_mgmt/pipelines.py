# -*- coding: utf-8 -*-
import os
import re
import typing
from pathlib import Path
from pydantic import BaseModel, Field, validator

from kiara.defaults import (
    DEFAULT_EXCLUDE_DIRS,
    MODULE_TYPE_NAME_KEY,
    USER_PIPELINES_FOLDER,
    VALID_PIPELINE_FILE_EXTENSIONS,
)
from kiara.module_mgmt import ModuleManager, log
from kiara.modules.pipelines import create_pipeline_class
from kiara.utils import get_data_from_file

if typing.TYPE_CHECKING:
    from kiara import PipelineModule

try:
    from typing import Literal
except Exception:
    from typing_extensions import Literal  # type: ignore


class PipelineModuleManagerConfig(BaseModel):

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


def get_pipeline_details_from_path(
    path: typing.Union[str, Path],
    module_type_name: typing.Optional[str] = None,
    base_module: typing.Optional[str] = None,
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
    if base_module:
        result["base_module"] = base_module
    return (name, result)


def check_doc_sidecar(
    path: typing.Union[Path, str], data: typing.Mapping[str, typing.Any]
) -> typing.Mapping[str, typing.Any]:

    if isinstance(path, str):
        path = Path(os.path.expanduser(path))

    _doc = data["data"].get("documentation", None)
    if _doc is None:
        _doc_path = Path(path.as_posix() + ".md")
        if _doc_path.is_file():
            doc = _doc_path.read_text()
            if doc:
                data["data"]["documentation"] = doc

    return data


class PipelineModuleManager(ModuleManager):
    def __init__(
        self,
        folders: typing.Optional[
            typing.Mapping[
                str, typing.Union[str, Path, typing.Iterable[typing.Union[str, Path]]]
            ]
        ] = None,
    ):

        if folders is None:
            from kiara.utils.class_loading import find_all_kiara_pipeline_paths

            folders_map: typing.Dict[
                str, typing.List[typing.Tuple[typing.Optional[str], str]]
            ] = find_all_kiara_pipeline_paths()
            if os.path.exists(USER_PIPELINES_FOLDER):
                folders_map["user"] = [(None, USER_PIPELINES_FOLDER)]
        elif not folders:
            folders_map = {}
        else:
            assert isinstance(folders, typing.Mapping)
            assert "user" not in folders.keys()
            folders_map = folders  # type: ignore
            raise NotImplementedError()

        self._pipeline_desc_folders: typing.List[Path] = []
        self._pipeline_descs: typing.Dict[str, typing.Mapping[str, typing.Any]] = {}
        self._cached_classes: typing.Dict[str, typing.Type[PipelineModule]] = {}

        for ns, paths in folders_map.items():

            for path in paths:
                self.add_pipelines_path(ns, path[1], path[0])

    def register_pipeline(
        self,
        data: typing.Union[str, Path, typing.Mapping[str, typing.Any]],
        module_type_name: typing.Optional[str] = None,
        namespace: typing.Optional[str] = None,
    ) -> str:
        """Register a pipeline description to the pipeline pool.

        Arguments:
            data: the pipeline data (a dict, or a path to a file)
            module_type_name: the type name this pipeline should be registered as
        """

        # TODO: verify that there is no conflict with module_type_name
        if isinstance(data, str):
            data = Path(os.path.expanduser(data))

        if isinstance(data, Path):
            _name, _data = get_pipeline_details_from_path(data)
            _data = check_doc_sidecar(data, _data)

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

        if not namespace:
            full_name = _name
        else:
            full_name = f"{namespace}.{_name}"
        if full_name.startswith("core."):
            full_name = full_name[5:]
        if full_name in self._pipeline_descs.keys():
            raise Exception(
                f"Can't register pipeline: duplicate workflow name '{_name}'"
            )

        self._pipeline_descs[full_name] = _data

        return full_name

    def add_pipelines_path(
        self,
        namespace: str,
        path: typing.Union[str, Path],
        base_module: typing.Optional[str],
    ) -> typing.Iterable[str]:
        """Add a pipeline description file or folder containing some to this manager.

        Arguments:
            namespace: the namespace the pipeline modules found under this path will be part of, if it starts with '_' it will be omitted
            path: the path to a pipeline description file, or folder which contains some
        Returns:
            a list of module type names that were added
        """

        if isinstance(path, str):
            path = Path(os.path.expanduser(path))
        elif isinstance(path, typing.Iterable):
            raise TypeError(f"Invalid type for path: {path}")

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

                        name, data = get_pipeline_details_from_path(
                            path=full_path, base_module=base_module
                        )
                        data = check_doc_sidecar(full_path, data)
                        rel_path = os.path.relpath(os.path.dirname(full_path), path)
                        if not rel_path or rel_path == ".":
                            ns_name = name
                        else:
                            _rel_path = rel_path.replace(os.path.sep, ".")
                            ns_name = f"{_rel_path}.{name}"
                        if ns_name in files.keys():
                            raise Exception(
                                f"Duplicate workflow name in namespace '{namespace}': {ns_name}"
                            )
                        files[ns_name] = data
                    except Exception as e:
                        log.warning(
                            f"Ignoring invalid pipeline file '{full_path}': {e}"
                        )
        elif path.is_file():
            name, data = get_pipeline_details_from_path(
                path=path, base_module=base_module
            )
            data = check_doc_sidecar(path, data)
            files = {name: data}

        result = {}
        for k, v in files.items():

            if namespace.startswith("_"):
                tokens = namespace.split(".")
                if len(tokens) == 1:
                    _namespace = ""
                else:
                    _namespace = ".".join(tokens[1:])
            else:
                _namespace = namespace

            if not _namespace:
                full_name = k
            else:
                full_name = f"{_namespace}.{k}"

            if full_name.startswith("core."):
                full_name = full_name[5:]
            if full_name in self._pipeline_descs.keys():
                raise Exception(f"Duplicate workflow name: {name}")
            result[full_name] = v

        self._pipeline_descs.update(result)
        return result.keys()

    @property
    def pipeline_descs(self) -> typing.Mapping[str, typing.Mapping[str, typing.Any]]:
        return self._pipeline_descs

    def get_module_class(self, module_type: str) -> typing.Type["PipelineModule"]:

        if module_type in self._cached_classes.keys():
            return self._cached_classes[module_type]

        desc = self._pipeline_descs.get(module_type, None)
        if desc is None:
            raise Exception(f"No pipeline with name '{module_type}' available.")

        tokens = re.split(r"\.|_", module_type)
        cls_name = "".join(x.capitalize() or "_" for x in tokens)

        if len(tokens) != 1:
            full_name = ".".join(tokens[0:-1] + [cls_name])
        else:
            full_name = cls_name

        base_module = desc.get("base_module", None)
        cls = create_pipeline_class(
            f"{cls_name}PipelineModule",
            full_name,
            desc["data"],
            base_module=base_module,
        )
        setattr(cls, "_module_type_name", module_type)
        self._cached_classes[module_type] = cls
        return self._cached_classes[module_type]

    def get_module_types(self) -> typing.Iterable[str]:
        return self._pipeline_descs.keys()
