# -*- coding: utf-8 -*-
import datetime
import filetype
import logging
import os.path
import shutil
import typing
from anyio import create_task_group, open_file, start_blocking_portal
from pydantic import BaseModel, Field

from kiara.data.types import ValueType

log = logging.getLogger("kiara")


class FileModel(BaseModel):
    @classmethod
    def import_file(cls, source: str, target: typing.Optional[str] = None):

        if not source:
            raise ValueError("No source path provided.")

        if not os.path.exists(os.path.realpath(source)):
            raise ValueError(f"Path does not exist: {source}")

        if not os.path.isfile(os.path.realpath(source)):
            raise ValueError(f"Path is not a file: {source}")

        orig_filename = os.path.basename(source)
        orig_path = os.path.abspath(source)
        file_import_time = datetime.datetime.now().isoformat()  # TODO: timezone

        file_stats = os.stat(orig_path)
        size = file_stats.st_size

        if target:
            if os.path.exists(target):
                raise ValueError(f"Target path exists: {target}")
            os.makedirs(os.path.dirname(target), exist_ok=True)
            shutil.copy2(source, target)
        else:
            target = orig_path
        mime_type = filetype.guess(target)
        if not mime_type:
            mime_type = "application/octet-stream"

        m = FileModel(
            orig_filename=orig_filename,
            orig_path=orig_path,
            import_time=file_import_time,
            mime_type=mime_type,
            size=size,
            file_name=orig_filename,
            path=target,
        )
        return m

    orig_filename: str = Field(
        description="The original filename of this file at the time of import."
    )
    orig_path: str = Field(
        description="The original path to this file at the time of import."
    )
    import_time: str = Field(description="The time when the file was imported.")
    mime_type: str = Field(description="The mime type of the file.")
    file_name: str = Field("The name of the file.")
    size: int = Field(description="The size of the file.")
    path: str = Field(description="The archive path of the file.")

    def __repr__(self):
        return f"FileMode(name={self.file_name})"

    def __str__(self):
        return self.__repr__()


class FileType(ValueType):
    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:

        assert isinstance(v, FileModel)

        md = v.dict()
        md["python_cls"] = FileModel.__name__
        return md


class FolderImportConfig(BaseModel):

    exclude_dirs: typing.Optional[typing.List[str]] = Field(
        description="A list of strings, exclude all folders whose name ends with that string.",
        default=None,
    )
    include_files: typing.Optional[typing.List[str]] = Field(
        description="A list of strings, include all files where the filename ends with that string.",
        default=None,
    )


class FileBundleModel(BaseModel):
    @classmethod
    def import_folder(
        cls,
        source: str,
        target: typing.Optional[str] = None,
        import_config: typing.Union[
            None, typing.Mapping[str, typing.Any], FolderImportConfig
        ] = None,
    ):

        if not source:
            raise ValueError("No source path provided.")

        if not os.path.exists(os.path.realpath(source)):
            raise ValueError(f"Path does not exist: {source}")

        if not os.path.isdir(os.path.realpath(source)):
            raise ValueError(f"Path is not a file: {source}")

        if target and os.path.exists(target):
            raise ValueError(f"Target path already exists: {target}")

        if source.endswith(os.path.sep):
            source = source[0:-1]

        if target and target.endswith(os.path.sep):
            target = target[0:-1]

        if import_config is None:
            _import_config = FolderImportConfig()
        elif isinstance(import_config, typing.Mapping):
            _import_config = FolderImportConfig(**import_config)
        elif isinstance(import_config, FolderImportConfig):
            _import_config = import_config
        else:
            raise TypeError(
                f"Invalid type for folder import config: {type(import_config)}."
            )

        result: typing.Dict[str, typing.Any] = {}

        included_files: typing.Dict[str, FileModel] = {}
        exclude_dirs = _import_config.exclude_dirs
        valid_extensions = _import_config.include_files

        sum_size = 0

        for root, dirnames, filenames in os.walk(source, topdown=True):

            if exclude_dirs:
                dirnames[:] = [d for d in dirnames if d not in exclude_dirs]

            for filename in [
                f
                for f in filenames
                if os.path.isfile(os.path.join(root, f))
                and (
                    not valid_extensions
                    or any(f.endswith(ext) for ext in valid_extensions)
                )
            ]:

                full_path = os.path.join(root, filename)
                rel_path = os.path.relpath(full_path, source)
                if target:
                    target_path: typing.Optional[str] = os.path.join(target, rel_path)
                else:
                    target_path = None

                file_model = FileModel.import_file(full_path, target_path)
                sum_size = sum_size + file_model.size
                included_files[rel_path] = file_model

        result["included_files"] = included_files
        result["orig_bundle_name"] = os.path.basename(source)
        result["orig_path"] = source
        if target:
            result["path"] = target
        else:
            result["path"] = source
        result["import_time"] = datetime.datetime.now().isoformat()
        result["number_of_files"] = len(included_files)
        result["bundle_name"] = os.path.basename(result["path"])
        result["size"] = sum_size

        return FileBundleModel(**result)

    orig_bundle_name: str = Field(
        description="The original name of this folder at the time of import."
    )
    bundle_name: str = Field(description="The name of this bundle.")
    orig_path: typing.Optional[str] = Field(
        description="The original path to this folder at the time of import.",
        default=None,
    )
    import_time: str = Field(description="The time when the file was imported.")
    number_of_files: int = Field(
        description="How many files are included in this bundle."
    )
    included_files: typing.Dict[str, FileModel] = Field(
        description="A map of all the included files, incl. their properties."
    )
    size: int = Field(description="The size of all files in this folder, combined.")
    path: str = Field(description="The archive path of the folder.")

    def get_relative_path(self, file: FileModel):

        return os.path.relpath(file.path, self.path)

    def read_text_file_contents(self) -> typing.Mapping[str, str]:

        content_dict = {}

        with start_blocking_portal() as portal:

            async def read_file(rel_path: str, fm: FileModel):
                async with await open_file(fm.path) as f:
                    content = await f.read()
                    content_dict[rel_path] = content

            async def read_files():

                async with create_task_group() as tg:
                    for f in self.included_files.values():
                        rel_path = self.get_relative_path(f)
                        tg.start_soon(read_file, rel_path, f)

            portal.call(read_files)

        return content_dict

    def __repr__(self):
        return f"FileBundle(name={self.bundle_name})"

    def __str__(self):
        return self.__repr__()


class FileBundleType(ValueType):
    def extract_metadata(cls, v: typing.Any) -> typing.Mapping[str, typing.Any]:
        assert isinstance(v, FileBundleModel)

        # TODO: remove the exclude
        md = v.dict(exclude={"included_files"})
        md["python_cls"] = FileBundleModel.__name__
        return md
