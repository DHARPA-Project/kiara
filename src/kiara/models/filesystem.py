# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import atexit
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable, ClassVar, Dict, List, Mapping, Union

import structlog
from deepdiff import DeepHash
from multiformats import CID
from pydantic import BaseModel, Field, PrivateAttr
from rich import box
from rich.console import RenderableType
from rich.table import Table

from kiara.defaults import (
    DEFAULT_EXCLUDE_DIRS,
    DEFAULT_EXCLUDE_FILES,
)
from kiara.exceptions import KiaraException
from kiara.models import KiaraModel
from kiara.utils import log_message
from kiara.utils.files import unpack_archive
from kiara.utils.hashing import KIARA_HASH_FUNCTION, compute_cid_from_file

logger = structlog.getLogger()

FILE_BUNDLE_IMPORT_AVAILABLE_COLUMNS = [
    "id",
    "rel_path",
    # "import_time",
    "mime_type",
    "size",
    "content",
    "file_name",
]


class KiaraFile(KiaraModel):
    """Describes properties for the 'file' value type."""

    _kiara_model_id: ClassVar = "instance.data.file"

    @classmethod
    def load_file(
        cls,
        source: str,
        file_name: Union[str, None] = None,
        # import_time: Optional[datetime.datetime] = None,
    ) -> "KiaraFile":
        """Utility method to read metadata of a file from disk."""
        import mimetypes

        import filetype

        if not source:
            raise ValueError("No source path provided.")

        if not os.path.exists(os.path.realpath(source)):
            raise ValueError(f"Path does not exist: {source}")

        if not os.path.isfile(os.path.realpath(source)):
            raise ValueError(f"Path is not a file: {source}")

        if file_name is None:
            file_name = os.path.basename(source)

        path: str = os.path.abspath(source)
        # if import_time:
        #     file_import_time = import_time
        # else:
        #     file_import_time = datetime.datetime.now()  # TODO: timezone

        file_stats = os.stat(path)
        size = file_stats.st_size

        r = mimetypes.guess_type(path)
        if r[0] is not None:
            mime_type = r[0]
        else:
            _mime_type = filetype.guess(path)
            if not _mime_type:
                mime_type = "application/octet-stream"
            else:
                mime_type = _mime_type.MIME

        m = KiaraFile(
            # import_time=file_import_time,
            mime_type=mime_type,
            size=size,
            file_name=file_name,
        )
        m._path = path
        return m

    # import_time: datetime.datetime = Field(
    #     description="The time when the file was imported."
    # )
    mime_type: str = Field(description="The mime type of the file.")
    file_name: str = Field("The name of the file.")
    size: int = Field(description="The size of the file.")
    metadata: Dict[str, Any] = Field(
        description="Additional, ustructured, user-defined metadata.",
        default_factory=dict,
    )
    metadata_schemas: Dict[str, str] = Field(
        description="The metadata schemas for each of the metadata values (if available).",
        default_factory=dict,
    )

    _path: Union[str, None] = PrivateAttr(default=None)
    _path_resolver: Union[Callable, None] = PrivateAttr(default=None)
    _file_hash: Union[str, None] = PrivateAttr(default=None)
    _file_cid: Union[CID, None] = PrivateAttr(default=None)

    # @validator("path")
    # def ensure_abs_path(cls, value):
    #     return os.path.abspath(value)

    @property
    def path(self) -> str:
        if self._path is None:
            if self._path_resolver is not None:
                self._path = self._path_resolver()
            else:
                raise Exception("File path not set for file model.")
        return self._path

    def _retrieve_data_to_hash(self) -> Any:
        data = {
            "file_name": self.file_name,
            "file_cid": self.file_cid,
        }
        return data

    # def get_id(self) -> str:
    #     return self.path

    def get_category_alias(self) -> str:
        return "instance.file_model"

    def copy_file(self, target: str, new_name: Union[str, None] = None) -> "KiaraFile":

        target_path: str = os.path.abspath(target)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)

        shutil.copy2(self.path, target_path)
        fm = KiaraFile.load_file(target, file_name=new_name)

        if self._file_hash is not None:
            fm._file_hash = self._file_hash

        return fm

    @property
    def file_hash(self) -> str:

        if self._file_hash is not None:
            return self._file_hash

        self._file_hash = str(self.file_cid)
        return self._file_hash

    @property
    def file_cid(self) -> CID:

        if self._file_cid is not None:
            return self._file_cid

        # TODO: auto-set codec?
        self._file_cid = compute_cid_from_file(file=self.path, codec="raw")
        return self._file_cid

    @property
    def file_name_without_extension(self) -> str:

        return self.file_name.split(".")[0]

    @property
    def file_extension(self) -> str:
        return self.file_name.split(".")[-1]

    def read_text(self, max_lines: int = -1) -> str:
        """Read the content of a file."""
        with open(self.path, "rt") as f:
            if max_lines <= 0:
                content = f.read()
            else:
                content = "".join((next(f) for x in range(max_lines)))
        return content

    def read_bytes(self, length: int = -1) -> bytes:
        """Read the content of a file."""
        with open(self.path, "rb") as f:
            if length <= 0:
                content = f.read()
            else:
                content = f.read(length)
        return content

    def __repr__(self):
        return f"KiaraFile(name={self.file_name})"

    def __str__(self):
        return self.__repr__()


class FolderImportConfig(BaseModel):

    sub_path: Union[str, None] = Field(
        description="The sub-path to import from the folder.", default=None
    )

    include_files: Union[List[str], None] = Field(
        description="A list of strings, include all files where the filename ends with that string.",
        default=None,
    )
    exclude_dirs: Union[List[str], None] = Field(
        description="A list of strings, exclude all folders whose name ends with that string.",
        default=DEFAULT_EXCLUDE_DIRS,
    )
    exclude_files: Union[List[str], None] = Field(
        description=f"A list of strings, exclude all files that match those (takes precedence over 'include_files'). Defaults to: {DEFAULT_EXCLUDE_FILES}.",
        default=DEFAULT_EXCLUDE_FILES,
    )


class KiaraFileBundle(KiaraModel):
    """Describes properties for the 'file_bundle' value type."""

    _kiara_model_id: ClassVar = "instance.data.file_bundle"

    @classmethod
    def create_tmp_dir(self) -> Path:
        """Utility method to create a temp folder that gets deleted when kiara exits."""
        temp_f = tempfile.mkdtemp()

        def cleanup():
            shutil.rmtree(temp_f, ignore_errors=True)

        atexit.register(cleanup)

        return Path(temp_f)

    @classmethod
    def from_archive(
        cls,
        archive_path: str,
        import_config: Union[FolderImportConfig, None] = None,
        bundle_name: Union[str, None] = None,
    ) -> "KiaraFileBundle":
        """Extracts the contents of an archive file to a target folder."""

        if not os.path.isfile(archive_path):
            raise KiaraException(
                msg=f"Archive file '{archive_path}' does not exist or is not a file."
            )

        out_dir = tempfile.mkdtemp()

        def del_out_dir():
            shutil.rmtree(out_dir, ignore_errors=True)

        atexit.register(del_out_dir)

        unpack_archive(archive_path, out_dir)

        bundle = KiaraFileBundle.import_folder(
            out_dir, import_config=import_config, bundle_name=bundle_name
        )
        return bundle

    @classmethod
    def from_archive_file(
        cls,
        archive_file: KiaraFile,
        import_config: Union[FolderImportConfig, None] = None,
    ) -> "KiaraFileBundle":
        """Extracts the contents of an archive file to a target folder."""

        bundle = KiaraFileBundle.from_archive(
            archive_path=archive_file.path,
            bundle_name=archive_file.file_name,
            import_config=import_config,
        )

        bundle.metadata = archive_file.metadata
        bundle.metadata_schemas = archive_file.metadata_schemas
        return bundle

    @classmethod
    def import_folder(
        cls,
        source: str,
        bundle_name: Union[str, None] = None,
        import_config: Union[None, Mapping[str, Any], FolderImportConfig] = None,
        # import_time: Optional[datetime.datetime] = None,
    ) -> "KiaraFileBundle":

        if not source:
            raise ValueError("No source path provided.")

        if not os.path.exists(os.path.realpath(source)):
            raise ValueError(f"Path does not exist: {source}")

        if not os.path.isdir(os.path.realpath(source)):
            raise ValueError(f"Path is not a folder: {source}")

        if source.endswith(os.path.sep):
            source = source[0:-1]

        if import_config is None:
            _import_config = FolderImportConfig()
        elif isinstance(import_config, Mapping):
            _import_config = FolderImportConfig(**import_config)
        elif isinstance(import_config, FolderImportConfig):
            _import_config = import_config
        else:
            raise TypeError(
                f"Invalid type for folder import config: {type(import_config)}."
            )

        abs_path = os.path.abspath(source)
        if _import_config.sub_path:
            abs_path = os.path.join(abs_path, _import_config.sub_path)

        included_files: Dict[str, KiaraFile] = {}
        exclude_dirs = _import_config.exclude_dirs
        invalid_extensions = _import_config.exclude_files

        valid_extensions = _import_config.include_files

        sum_size = 0

        def include_file(filename: str) -> bool:

            if invalid_extensions and any(
                filename.endswith(ext) for ext in invalid_extensions
            ):
                return False
            if not valid_extensions:
                return True
            else:
                return any(filename.endswith(ext) for ext in valid_extensions)

        if os.path.isfile(abs_path):
            file_model = KiaraFile.load_file(abs_path)
            sum_size = file_model.size
            included_files[file_model.file_name] = file_model
        else:
            for root, dirnames, filenames in os.walk(abs_path, topdown=True):

                if exclude_dirs:
                    dirnames[:] = [d for d in dirnames if d not in exclude_dirs]

                for filename in [
                    f
                    for f in filenames
                    if os.path.isfile(os.path.join(root, f)) and include_file(f)
                ]:

                    full_path = os.path.join(root, filename)
                    rel_path = os.path.relpath(full_path, abs_path)

                    file_model = KiaraFile.load_file(full_path)
                    sum_size = sum_size + file_model.size
                    included_files[rel_path] = file_model

        if bundle_name is None:
            bundle_name = os.path.basename(source)

        bundle = KiaraFileBundle.create_from_file_models(
            files=included_files,
            path=abs_path,
            bundle_name=bundle_name,
            sum_size=sum_size,
        )
        return bundle

    @classmethod
    def create_from_file_models(
        cls,
        files: Mapping[str, KiaraFile],
        bundle_name: str,
        path: Union[str, None] = None,
        sum_size: Union[int, None] = None,
        # import_time: Optional[datetime.datetime] = None,
    ) -> "KiaraFileBundle":

        # if import_time:
        #     bundle_import_time = import_time
        # else:
        #     bundle_import_time = datetime.datetime.now()  # TODO: timezone

        result: Dict[str, Any] = {}

        result["included_files"] = files

        # result["import_time"] = datetime.datetime.now().isoformat()
        result["number_of_files"] = len(files)
        result["bundle_name"] = bundle_name
        # result["import_time"] = bundle_import_time

        if sum_size is None:
            sum_size = 0
            for f in files.values():
                sum_size = sum_size + f.size
        result["size"] = sum_size

        bundle = KiaraFileBundle(**result)
        bundle._path = path
        return bundle

    _file_bundle_hash: Union[int, None] = PrivateAttr(default=None)

    bundle_name: str = Field(description="The name of this bundle.")
    # import_time: datetime.datetime = Field(
    #     description="The time when the file bundle was imported."
    # )
    number_of_files: int = Field(
        description="How many files are included in this bundle."
    )
    included_files: Dict[str, KiaraFile] = Field(
        description="A map of all the included files, incl. their properties. Uses the relative path of each file as key."
    )
    size: int = Field(description="The size of all files in this folder, combined.")
    metadata: Dict[str, Any] = Field(
        description="Additional, ustructured, user-defined metadata.",
        default_factory=dict,
    )
    metadata_schemas: Dict[str, str] = Field(
        description="The metadata schemas for each metadata value (if available).",
        default_factory=dict,
    )
    _path: Union[str, None] = PrivateAttr(default=None)

    @property
    def path(self) -> str:
        if self._path is None:
            # TODO: better explanation, offer remedy like copying into temp folder
            raise Exception(
                "File bundle path not set, it appears this bundle is comprised of symlinks only."
            )
        return self._path

    def _retrieve_id(self) -> str:
        return str(self.file_bundle_hash)

    # @property
    # def model_data_hash(self) -> int:
    #     return self.file_bundle_hash

    def _retrieve_data_to_hash(self) -> Any:

        return {
            "bundle_name": self.bundle_name,
            "included_files": {
                k: v.instance_cid for k, v in self.included_files.items()
            },
        }

    def get_relative_path(self, file: KiaraFile):
        return os.path.relpath(file.path, self.path)

    def read_text_file_contents(self, ignore_errors: bool = False) -> Mapping[str, str]:

        content_dict: Dict[str, str] = {}

        def read_file(rel_path: str, full_path: str):
            with open(full_path, encoding="utf-8") as f:
                try:
                    content = f.read()
                    content_dict[rel_path] = content  # type: ignore
                except Exception as e:
                    if ignore_errors:
                        log_message(f"Can't read file: {e}")
                        logger.warning("ignore.file", path=full_path, reason=str(e))
                    else:
                        raise Exception(f"Can't read file (as text) '{full_path}: {e}")

        # TODO: common ignore files and folders
        for rel_path, f in self.included_files.items():
            if f._path:
                path = f._path
            else:
                path = self.get_relative_path(f)
            read_file(rel_path=rel_path, full_path=path)

        return content_dict

    @property
    def file_bundle_hash(self) -> int:

        # TODO: use sha256?
        if self._file_bundle_hash is not None:
            return self._file_bundle_hash

        obj = {k: v.file_hash for k, v in self.included_files.items()}
        h = DeepHash(obj, hasher=KIARA_HASH_FUNCTION)

        self._file_bundle_hash = h[obj]
        return self._file_bundle_hash

    def copy_bundle(
        self, target_path: str, bundle_name: Union[str, None] = None
    ) -> "KiaraFileBundle":

        if target_path == self.path:
            raise Exception(f"Target path and current path are the same: {target_path}")

        result = {}
        for rel_path, item in self.included_files.items():
            _target_path = os.path.join(target_path, rel_path)
            new_fm = item.copy_file(_target_path)
            result[rel_path] = new_fm

        if bundle_name is None:
            bundle_name = os.path.basename(target_path)

        fb = KiaraFileBundle.create_from_file_models(
            files=result,
            bundle_name=bundle_name,
            path=target_path,
            sum_size=self.size,
            # import_time=self.import_time,
        )
        if self._file_bundle_hash is not None:
            fb._file_bundle_hash = self._file_bundle_hash

        return fb

    def create_renderable(self, **config: Any) -> RenderableType:

        show_bundle_hash = config.get("show_bundle_hash", False)

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("key")
        table.add_column("value", style="i")

        table.add_row("bundle name", self.bundle_name)
        # table.add_row("import_time", str(self.import_time))
        table.add_row("number_of_files", str(self.number_of_files))
        table.add_row("size", str(self.size))
        if show_bundle_hash:
            table.add_row("bundle_hash", str(self.file_bundle_hash))

        content = self._create_content_table(**config)
        table.add_row("included files", content)

        return table

    def _create_content_table(self, **render_config: Any) -> Table:

        # show_content = render_config.get("show_content_preview", False)
        max_no_included_files = render_config.get("max_no_files", 40)

        table = Table(show_header=True, box=box.SIMPLE)
        table.add_column("(relative) path")
        table.add_column("size")
        # if show_content:
        #     table.add_column("content preview")

        if (
            max_no_included_files < 0
            or len(self.included_files) <= max_no_included_files
        ):
            for f, model in self.included_files.items():
                row = [f, str(model.size)]
                table.add_row(*row)
        else:
            files = list(self.included_files.keys())
            half = int((max_no_included_files - 1) / 2)
            head = files[0:half]
            tail = files[-1 * half :]
            for rel_path in head:
                model = self.included_files[rel_path]
                row = [rel_path, str(model.size)]
                table.add_row(*row)
            table.add_row("   ... output skipped ...", "")
            table.add_row("   ... output skipped ...", "")
            for rel_path in tail:
                model = self.included_files[rel_path]
                row = [rel_path, str(model.size)]
                table.add_row(*row)

        return table

    def __repr__(self):
        return f"FileBundle(name={self.bundle_name})"

    def __str__(self):
        return self.__repr__()
