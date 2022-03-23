import datetime
import hashlib
import os
import shutil
from typing import Optional, Any, Union, Mapping, Dict, List

from deepdiff import DeepHash
from pydantic import PrivateAttr, Field, validator, BaseModel

from kiara.defaults import FILE_MODEL_CATEOGORY_ID, FILE_BUNDLE_MODEL_CATEOGORY_ID, KIARA_HASH_FUNCTION, \
    DEFAULT_EXCLUDE_FILES
from kiara.models import KiaraModel
from kiara.utils import log_message



class FileModel(KiaraModel):
    """Describes properties for the 'file' value type."""

    @classmethod
    def load_file(
        cls,
        source: str,
        import_time: Optional[datetime.datetime]=None
    ):
        """Utility method to read metadata of a file from disk and optionally move it into a data archive location."""

        import mimetypes

        import filetype

        if not source:
            raise ValueError("No source path provided.")

        if not os.path.exists(os.path.realpath(source)):
            raise ValueError(f"Path does not exist: {source}")

        if not os.path.isfile(os.path.realpath(source)):
            raise ValueError(f"Path is not a file: {source}")

        file_name = os.path.basename(source)
        path: str = os.path.abspath(source)
        if import_time:
            file_import_time = import_time
        else:
            file_import_time = datetime.datetime.now()  # TODO: timezone

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

        m = FileModel(
            import_time=file_import_time,
            mime_type=mime_type,
            size=size,
            file_name=file_name,
            path=path,
        )
        return m

    _file_hash: Optional[int] = PrivateAttr(default=None)

    import_time: datetime.datetime = Field(description="The time when the file was imported.")
    mime_type: str = Field(description="The mime type of the file.")
    file_name: str = Field("The name of the file.")
    size: int = Field(description="The size of the file.")
    path: str = Field(description="The archive path of the file.")

    @validator("path")
    def ensure_abs_path(cls, value):
        return os.path.abspath(value)

    def _retrieve_id(self) -> str:
        return self.path

    def _retrieve_category_id(self) -> str:
        return FILE_MODEL_CATEOGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return {
            "path": self.path,
            "hash": self.file_hash
        }

    def get_id(self) -> str:
        return self.path

    def get_category_alias(self) -> str:
        return "instance.metadata.file"

    def copy_file(
        self, target: str
    ) -> "FileModel":

        target_path: str = os.path.abspath(target)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)

        shutil.copy2(self.path, target_path)
        fm = FileModel.load_file(target, import_time=self.import_time)

        if self._file_hash is not None:
            fm._file_hash = self._file_hash

        return fm

    @property
    def file_hash(self) -> int:

        if self._file_hash is not None:
            return self._file_hash

        sha256_hash = hashlib.sha3_256()
        with open(self.path, "rb") as f:
            # Read and update hash string value in blocks of 4K
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)

        self._file_hash = int.from_bytes(sha256_hash.digest(), "big")
        return self._file_hash

    @property
    def file_name_without_extension(self) -> str:

        return self.file_name.split(".")[0]

    @property
    def import_time_as_datetime(self) -> datetime.datetime:
        from dateutil import parser

        return parser.parse(self.import_time)

    def read_text(
        self, max_lines: int = -1
    ) -> str:
        """Read the content of a file."""

        with open(self.path, "rt") as f:
            if max_lines <= 0:
                content = f.read()
            else:
                content = "".join((next(f) for x in range(max_lines)))
        return content

    def read_bytes(
        self, length: int = -1
    ) -> bytes:
        """Read the content of a file."""


        with open(self.path, "rb") as f:
            if length <= 0:
                content = f.read()
            else:
                content = f.read(length)
        return content

    def __repr__(self):
        return f"FileModel(name={self.file_name})"

    def __str__(self):
        return self.__repr__()

class FolderImportConfig(BaseModel):

    include_files: Optional[List[str]] = Field(
        description="A list of strings, include all files where the filename ends with that string.",
        default=None,
    )
    exclude_dirs: Optional[List[str]] = Field(
        description="A list of strings, exclude all folders whose name ends with that string.",
        default=None,
    )
    exclude_files: Optional[List[str]] = Field(
        description=f"A list of strings, exclude all files that match those (takes precedence over 'include_files'). Defaults to: {DEFAULT_EXCLUDE_FILES}.",
        default=DEFAULT_EXCLUDE_FILES,
    )

class FileBundle(KiaraModel):
    """Describes properties for the 'file_bundle' value type."""

    @classmethod
    def import_folder(
        cls,
        source: str,
        import_config: Union[
            None, Mapping[str, Any], FolderImportConfig
        ] = None,
        import_time: Optional[datetime.datetime] = None
    ):

        if not source:
            raise ValueError("No source path provided.")

        if not os.path.exists(os.path.realpath(source)):
            raise ValueError(f"Path does not exist: {source}")

        if not os.path.isdir(os.path.realpath(source)):
            raise ValueError(f"Path is not a file: {source}")

        if source.endswith(os.path.sep):
            source = source[0:-1]

        path = os.path.abspath(source)

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

        included_files: Dict[str, FileModel] = {}
        exclude_dirs = _import_config.exclude_dirs
        invalid_extensions = _import_config.exclude_files

        valid_extensions = _import_config.include_files

        if import_time:
            bundle_import_time = import_time
        else:
            bundle_import_time = datetime.datetime.now()  # TODO: timezone

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

        for root, dirnames, filenames in os.walk(path, topdown=True):

            if exclude_dirs:
                dirnames[:] = [d for d in dirnames if d not in exclude_dirs]

            for filename in [
                f
                for f in filenames
                if os.path.isfile(os.path.join(root, f)) and include_file(f)
            ]:

                full_path = os.path.join(root, filename)
                rel_path = os.path.relpath(full_path, path)

                file_model = FileModel.load_file(
                    full_path, import_time=bundle_import_time
                )
                sum_size = sum_size + file_model.size
                included_files[rel_path] = file_model

        bundle_name = os.path.basename(source)

        return FileBundle.create_from_file_models(
            files=included_files,
            path=path,
            bundle_name=bundle_name,
            sum_size=sum_size,
            import_time=bundle_import_time
        )

    @classmethod
    def create_from_file_models(
        self,
        files: Mapping[str, FileModel],
        bundle_name: str,
        path: str,
        sum_size: Optional[int] = None,
        import_time: Optional[datetime.datetime] = None
    ):

        if import_time:
            bundle_import_time = import_time
        else:
            bundle_import_time = datetime.datetime.now()  # TODO: timezone

        result: Dict[str, Any] = {}

        result["included_files"] = files

        result["path"] = path
        result["import_time"] = datetime.datetime.now().isoformat()
        result["number_of_files"] = len(files)
        result["bundle_name"] = bundle_name
        result["import_time"] = bundle_import_time

        if sum_size is None:
            sum_size = 0
            for f in files.values():
                sum_size = sum_size + f.size
        result["size"] = sum_size

        return FileBundle(**result)

    _file_bundle_hash: Optional[int] = PrivateAttr(default=None)

    path: str = Field(description="The archive path of the folder.")
    bundle_name: str = Field(description="The name of this bundle.")
    import_time: datetime.datetime = Field(description="The time when the file bundle was imported.")
    number_of_files: int = Field(
        description="How many files are included in this bundle."
    )
    included_files: Dict[str, FileModel] = Field(
        description="A map of all the included files, incl. their properties. Uses the relative path of each file as key."
    )
    size: int = Field(description="The size of all files in this folder, combined.")

    def _retrieve_id(self) -> str:
        return self.path

    def _retrieve_category_id(self) -> str:
        return FILE_BUNDLE_MODEL_CATEOGORY_ID

    def get_relative_path(self, file: FileModel):
        return os.path.relpath(file.path, self.path)

    def read_text_file_contents(
        self, ignore_errors: bool = False
    ) -> Mapping[str, str]:

        content_dict: Dict[str, str] = {}

        def read_file(rel_path: str, fm: FileModel):
            with open(fm.path, encoding="utf-8") as f:
                try:
                    content = f.read()
                    content_dict[rel_path] = content  # type: ignore
                except Exception as e:
                    if ignore_errors:
                        log_message(f"Can't read file: {e}")
                        logger.warning(f"ignore.file", path=fm.path, reason=str(e))
                    else:
                        raise Exception(f"Can't read file (as text) '{fm.path}: {e}")

        # TODO: common ignore files and folders
        for f in self.included_files.values():
            rel_path = self.get_relative_path(f)
            read_file(rel_path=rel_path, fm=f)

        return content_dict

    @property
    def file_bundle_hash(self) -> int:

        if self._file_bundle_hash is not None:
            return self._file_bundle_hash

        obj = {k: v.file_hash for k, v in self.included_files.items()}
        h = DeepHash(obj, hasher=KIARA_HASH_FUNCTION)

        self._file_bundle_hash = h[obj]
        return self._file_bundle_hash

    def copy_bundle(
        self, target_path: str, bundle_name: Optional[str]=None
    ) -> "FileBundle":

        if target_path == self.path:
            raise Exception(f"Target path and current path are the same: {target_path}")

        result = {}
        for rel_path, item in self.included_files.items():
            _target_path = os.path.join(target_path, rel_path)
            new_fm = item.copy_file(_target_path)
            result[rel_path] = new_fm

        if bundle_name is None:
            bundle_name = os.path.basename(target_path)

        fb = FileBundle.create_from_file_models(
            files=result,
            bundle_name=bundle_name,
            path=target_path,
            sum_size=self.size,
            import_time=self.import_time
        )
        if self._file_bundle_hash is not None:
            fb._file_bundle_hash = self._file_bundle_hash

        return fb

    def __repr__(self):
        return f"FileBundle(name={self.bundle_name})"

    def __str__(self):
        return self.__repr__()
