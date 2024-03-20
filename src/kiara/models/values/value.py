# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import atexit
import hashlib
import logging
import os
import tempfile
import uuid
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Mapping,
    MutableMapping,
    Sequence,
    Set,
    Union,
)

import orjson
from humanfriendly import format_size
from multiformats import CID, multihash
from multiformats.multihash import Multihash
from multiformats.varint import BytesLike
from pydantic import BaseModel, ConfigDict, PrivateAttr, model_validator
from pydantic.fields import Field
from rich import box
from rich.console import Group, RenderableType
from rich.panel import Panel
from rich.rule import Rule
from rich.syntax import Syntax
from rich.table import Table

from kiara.defaults import (
    NO_MODULE_TYPE,
    NO_SERIALIZATION_MARKER,
    VOID_KIARA_ID,
    SpecialValue,
)
from kiara.exceptions import DataTypeUnknownException, InvalidValuesException
from kiara.models import KiaraModel
from kiara.models.module.manifest import InputsManifest, Manifest
from kiara.models.python_class import PythonClass
from kiara.models.values import DataTypeCharacteristics, ValueStatus
from kiara.models.values.value_schema import ValueSchema
from kiara.utils import is_jupyter, log_exception
from kiara.utils.dates import get_current_time_incl_timezone
from kiara.utils.hashing import create_cid_digest
from kiara.utils.json import orjson_dumps
from kiara.utils.yaml import StringYAML

log = logging.getLogger("kiara")
yaml = StringYAML()

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.data_types import DataType
    from kiara.interfaces.python_api.models.info import ValueInfo
    from kiara.models.values.lineage import ValueLineage
    from kiara.registries.data import DataRegistry


class SerializedChunks(BaseModel, abc.ABC):

    model_config = ConfigDict(extra="forbid")

    _size_cache: Union[int, None] = PrivateAttr(default=None)
    _hashes_cache: Dict[str, Sequence[CID]] = PrivateAttr(default_factory=dict)

    @abc.abstractmethod
    def get_chunks(
        self, as_files: bool = True, symlink_ok: bool = True
    ) -> Generator[Union[str, "BytesLike"], None, None]:
        """
        Retrieve the chunks belonging to this data instance.

        If 'as_file' is False, return the data as bytes. If set to 'True' store it to an arbitrary location (or use
        an existing one), and return the path to that file. If 'as_file' is a string, write the data (bytes) into
        a new file using the string as path. If 'symlink_ok' is set to True, symlinking an existing file to the value of
        'as_file' is also ok, otherwise copy the content.
        """

    @abc.abstractmethod
    def get_number_of_chunks(self) -> int:
        pass

    @abc.abstractmethod
    def _get_size(self) -> int:
        pass

    @abc.abstractmethod
    def _create_cids(self, hash_codec: str) -> Sequence[CID]:
        pass

    def get_size(self) -> int:

        if self._size_cache is None:
            self._size_cache = self._get_size()
        return self._size_cache

    def get_cids(self, hash_codec: str) -> Sequence[CID]:

        if self._hashes_cache.get(hash_codec, None) is None:
            self._hashes_cache[hash_codec] = self._create_cids(hash_codec=hash_codec)
        return self._hashes_cache[hash_codec]

    def _store_bytes_to_file(
        self, chunks: Iterable[bytes], file: Union[str, None] = None
    ) -> str:
        "Utility method to store bytes to a file."
        if file is None:
            file_desc, file = tempfile.mkstemp()

            def del_temp_file():
                os.remove(file)

            atexit.register(del_temp_file)

        else:
            if os.path.exists(file):
                raise Exception(f"Can't write to file, file exists: {file}")
            file_desc = os.open(file, 0o600)

        with os.fdopen(file_desc, "wb") as tmp:
            for chunk in chunks:
                tmp.write(chunk)

        return file

    def _read_bytes_from_file(self, file: str) -> bytes:

        with open(file, "rb") as f:
            content = f.read()

        return content


class SerializedPreStoreChunks(SerializedChunks):

    codec: str = Field(
        description="The codec used to encode the chunks in this model. Using the [multicodecs](https://github.com/multiformats/multicodec) codec table."
    )

    def _create_cid_from_chunk(self, chunk: bytes, hash_codec: str) -> CID:

        multihash = Multihash(codec=hash_codec)
        hash = multihash.digest(chunk)
        return create_cid_digest(digest=hash, codec=self.codec)

    def _create_cid_from_file(self, file: str, hash_codec: str) -> CID:

        assert hash_codec == "sha2-256"

        hash_func = hashlib.sha256
        file_hash = hash_func()

        CHUNK_SIZE = 65536
        with open(file, "rb") as f:
            fb = f.read(CHUNK_SIZE)
            while len(fb) > 0:
                file_hash.update(fb)
                fb = f.read(CHUNK_SIZE)

        wrapped = multihash.wrap(file_hash.digest(), "sha2-256")
        return create_cid_digest(digest=wrapped, codec=self.codec)


class SerializedBytes(SerializedPreStoreChunks):

    type: Literal["chunk"] = "chunk"
    chunk: bytes = Field(description="A byte-array")

    def get_chunks(
        self, as_files: Union[bool, str, Sequence[str]] = True, symlink_ok: bool = True
    ) -> Generator[Union[str, BytesLike], None, None]:

        if as_files is False:
            yield self.chunk
        else:
            if as_files is True:
                file = None
            elif isinstance(as_files, str):
                file = as_files
            else:
                assert len(as_files) == 1
                file = as_files[0]
            path = self._store_bytes_to_file([self.chunk], file=file)
            yield path

    def get_number_of_chunks(self) -> int:
        return 1

    def _get_size(self) -> int:
        return len(self.chunk)

    def _create_cids(self, hash_codec: str) -> Sequence[CID]:
        return [self._create_cid_from_chunk(self.chunk, hash_codec=hash_codec)]


class SerializedListOfBytes(SerializedPreStoreChunks):

    type: Literal["chunks"] = "chunks"
    chunks: List[bytes] = Field(description="A list of byte arrays.")

    def get_chunks(
        self, as_files: Union[bool, str, Sequence[str]] = True, symlink_ok: bool = True
    ) -> Generator[Union[str, BytesLike], None, None]:
        if as_files is False:
            for chunk in self.chunks:
                yield chunk
        else:
            if as_files is None or as_files is True or isinstance(as_files, str):
                # means we write all the chunks into one file
                file = None if as_files is True else as_files
                path = self._store_bytes_to_file(self.chunks, file=file)
                yield path
            else:
                assert len(as_files) == self.get_number_of_chunks()
                for idx, chunk in enumerate(self.chunks):
                    _file = as_files[idx]
                    path = self._store_bytes_to_file([chunk], file=_file)
                    yield path

    def get_number_of_chunks(self) -> int:
        return len(self.chunks)

    def _get_size(self) -> int:
        size = 0
        for chunk in self.chunks:
            size = size + len(chunk)
        return size

    def _create_cids(self, hash_codec: str) -> Sequence[CID]:
        return [
            self._create_cid_from_chunk(chunk, hash_codec=hash_codec)
            for chunk in self.chunks
        ]


class SerializedFile(SerializedPreStoreChunks):

    type: Literal["file"] = "file"
    file: str = Field(description="A path to a file containing the serialized data.")

    def get_chunks(
        self, as_files: Union[bool, str, Sequence[str]] = True, symlink_ok: bool = True
    ) -> Generator[Union[str, BytesLike], None, None]:

        if as_files is False:
            chunk = self._read_bytes_from_file(self.file)
            yield chunk
        else:
            if as_files is True:
                yield self.file
            else:
                if isinstance(as_files, str):
                    file = as_files
                else:
                    assert len(as_files) == 1
                    file = as_files[0]
                if os.path.exists(file):
                    raise Exception(f"Can't write to file '{file}': file exists.")
                if symlink_ok:
                    os.symlink(self.file, file)
                    yield file
                else:
                    raise NotImplementedError()

    def get_number_of_chunks(self) -> int:
        return 1

    def _get_size(self) -> int:
        return os.path.getsize(os.path.realpath(self.file))

    def _create_cids(self, hash_codec: str) -> Sequence[CID]:
        return [self._create_cid_from_file(self.file, hash_codec=hash_codec)]


class SerializedFiles(SerializedPreStoreChunks):

    type: Literal["files"] = "files"
    files: List[str] = Field(
        description="A list of strings, pointing to files containing parts of the serialized data."
    )

    def get_chunks(
        self, as_files: Union[bool, str, Sequence[str]] = True, symlink_ok: bool = True
    ) -> Generator[Union[str, BytesLike], None, None]:
        raise NotImplementedError()

    def get_number_of_chunks(self) -> int:
        return len(self.files)

    def _get_size(self) -> int:

        size = 0
        for file in self.files:
            size = size + os.path.getsize(os.path.realpath(file))
        return size

    def _create_cids(self, hash_codec: str) -> Sequence[CID]:
        return [
            self._create_cid_from_file(file, hash_codec=hash_codec)
            for file in self.files
        ]


class SerializedInlineJson(SerializedPreStoreChunks):

    type: Literal["inline-json"] = "inline-json"
    inline_data: Any = Field(
        None,
        description="Data that will not be stored externally, but inline in the containing model. This should only contain data types that can be serialized reliably using json (scalars, etc.).",
    )
    _json_cache: Union[bytes, None] = PrivateAttr(default=None)

    def as_json(self) -> bytes:
        assert self.inline_data is not None
        if self._json_cache is None:
            self._json_cache = orjson.dumps(
                self.inline_data,
                option=orjson.OPT_NON_STR_KEYS,
            )
        return self._json_cache

    def get_chunks(
        self, as_files: Union[bool, str, Sequence[str]] = True, symlink_ok: bool = True
    ) -> Generator[Union[str, BytesLike], None, None]:

        if as_files is False:
            yield self.as_json()
        else:
            raise NotImplementedError()

    def get_number_of_chunks(self) -> int:
        return 1

    def _get_size(self) -> int:
        return len(self.as_json())

    def _create_cids(self, hash_codec: str) -> Sequence[CID]:
        return [self._create_cid_from_chunk(self.as_json(), hash_codec=hash_codec)]


class SerializedChunkIDs(SerializedChunks):

    type: Literal["chunk-ids"] = "chunk-ids"
    chunk_id_list: List[str] = Field(
        description="A list of chunk ids, which will be resolved via the attached data registry."
    )
    archive_id: Union[uuid.UUID, None] = Field(
        None, description="The preferred data archive to get the chunks from."
    )
    size: int = Field(description="The size of all chunks combined.")
    _data_registry: "DataRegistry" = PrivateAttr(default=None)

    def get_chunks(
        self, as_files: bool = True, symlink_ok: bool = True
    ) -> Generator[Union[str, BytesLike], None, None]:
        """Retrieve the chunks of this value data.

        If 'as_files' is 'True', it will return strings representing paths to files containing the chunk data. If symlink_ok is also set to 'True', the returning Path could potentially be a symlink, which means the underlying function might not need to copy the file. In this case, you are responsible to not change the contents of the path, ever.

        If 'as_files' is 'False', BytesLike objects will be returned, containing the chunk data bytes directly.

        """

        chunk_ids = self.chunk_id_list
        return self._data_registry.retrieve_chunks(
            chunk_ids=chunk_ids,
            as_files=as_files,
            symlink_ok=symlink_ok,
            archive_id=self.archive_id,
        )

        # return (
        #     self._data_registry.retrieve_chunk(
        #         chunk_id=chunk,
        #         archive_id=self.archive_id,
        #         as_file=as_files,
        #         symlink_ok=symlink_ok,
        #     )
        #     for chunk in self.chunk_id_list
        # )

    def get_number_of_chunks(self) -> int:
        return len(self.chunk_id_list)

    def _get_size(self) -> int:
        return self.size

    def _create_cids(self, hash_codec: str) -> Sequence[CID]:

        result = []
        for chunk_id in self.chunk_id_list:
            cid = CID.decode(chunk_id)
            result.append(cid)

        return result


SERIALIZE_TYPES = {
    "chunk": SerializedBytes,
    "chunks": SerializedListOfBytes,
    "file": SerializedFile,
    "files": SerializedFiles,
    "inline-json": SerializedInlineJson,
    "chunk-ids": SerializedChunkIDs,
}


class SerializationMetadata(KiaraModel):

    _kiara_model_id: ClassVar = "metadata.serialized_data"

    environment: Mapping[str, int] = Field(
        description="Hash(es) for the environments the value was created/serialized.",
        default_factory=dict,
    )
    deserialize: Mapping[str, Manifest] = Field(
        description="Suggested manifest configs to use to de-serialize the data.",
        default_factory=dict,
    )


class SerializedData(KiaraModel):

    data_type: str = Field(
        description="The name of the data type for this serialized value."
    )
    data_type_config: Mapping[str, Any] = Field(
        description="The (optional) config for the data type for this serialized value.",
        default_factory=dict,
    )
    serialization_profile: str = Field(
        description="An identifying name for the serialization method used."
    )
    metadata: SerializationMetadata = Field(
        description="Optional metadata describing aspects of the serialization used.",
        default_factory=SerializationMetadata,
    )

    hash_codec: str = Field(
        description="The codec used to hash the value.", default="sha2-256"
    )
    _cids_cache: Dict[str, Sequence[CID]] = PrivateAttr(default_factory=dict)

    _cached_data_size: Union[int, None] = PrivateAttr(default=None)
    _cached_dag: Union[Dict[str, Sequence[CID]], None] = PrivateAttr(default=None)
    # _cached_cid: Optional[CID] = PrivateAttr(default=None)

    def _retrieve_data_to_hash(self) -> Any:

        return self.dag

    @property
    def data_size(self) -> int:
        if self._cached_data_size is not None:
            return self._cached_data_size

        size = 0
        for k in self.get_keys():
            model = self.get_serialized_data(k)
            size = size + model.get_size()
        self._cached_data_size = size
        return self._cached_data_size

    @abc.abstractmethod
    def get_keys(self) -> Iterable[str]:
        pass

    @abc.abstractmethod
    def get_serialized_data(self, key: str) -> SerializedChunks:
        pass

    def get_cids_for_key(self, key) -> Sequence[CID]:

        if key in self._cids_cache.keys():
            return self._cids_cache[key]

        model = self.get_serialized_data(key)
        self._cids_cache[key] = model.get_cids(hash_codec=self.hash_codec)
        return self._cids_cache[key]

    @property
    def dag(self) -> Mapping[str, Sequence[CID]]:

        if self._cached_dag is not None:
            return self._cached_dag

        dag: Dict[str, Sequence[CID]] = {}
        for key in self.get_keys():
            dag[key] = self.get_cids_for_key(key)

        self._cached_dag = dag
        return self._cached_dag


class SerializationResult(SerializedData):

    _kiara_model_id: ClassVar[str] = "instance.serialization_result"

    data: Dict[
        str,
        Union[
            SerializedBytes,
            SerializedListOfBytes,
            SerializedFile,
            SerializedFiles,
            SerializedInlineJson,
        ],
    ] = Field(
        description="One or several byte arrays representing the serialized state of the value."
    )

    def get_keys(self) -> Iterable[str]:
        return self.data.keys()

    def get_serialized_data(self, key: str) -> SerializedChunks:
        return self.data[key]

    @model_validator(mode="before")
    @classmethod
    def validate_data(cls, values):

        codec = values.get("codec", None)
        if codec is None:
            codec = "sha2-256"
            values["hash_codec"] = codec

        v = values.get("data")
        assert isinstance(v, Mapping)

        result = {}
        for field_name, data in v.items():
            if isinstance(data, SerializedChunks):
                result[field_name] = data
            elif isinstance(data, Mapping):
                s_type = data.get("type", None)
                if not s_type:
                    raise ValueError(
                        f"Invalid serialized data config, missing 'type' key: {data}"
                    )

                if s_type not in SERIALIZE_TYPES.keys():
                    raise ValueError(
                        f"Invalid serialized data type '{s_type}'. Allowed types: {', '.join(SERIALIZE_TYPES.keys())}"
                    )

                assert s_type != "chunk-ids"
                cls = SERIALIZE_TYPES[s_type]
                result[field_name] = cls(**data)

        values["data"] = result
        return values

    def create_renderable(self, **config: Any) -> RenderableType:

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("key")
        table.add_column("value")
        table.add_row("data_type", self.data_type)
        _config = Syntax(
            orjson_dumps(self.data_type_config), "json", background_color="default"
        )
        table.add_row("data_type_config", _config)

        data_fields = {}
        for field, model in self.data.items():
            data_fields[field] = {"type": model.type}
        data_json = Syntax(
            orjson_dumps(data_fields), "json", background_color="default"
        )
        table.add_row("data", data_json)
        table.add_row("size", str(self.data_size))
        table.add_row("hash", self.instance_id)

        return table

    def __repr__(self):

        return f"{self.__class__.__name__}(type={self.data_type} size={self.data_size})"

    def __str__(self):
        return self.__repr__()


class PersistedData(SerializedData):

    _kiara_model_id: ClassVar = "instance.persisted_data"

    archive_id: uuid.UUID = Field(
        description="The id of the store that persisted the data."
    )
    chunk_id_map: Mapping[str, SerializedChunkIDs] = Field(
        description="Reference-ids that resolve to the values' serialized chunks."
    )

    def get_keys(self) -> Iterable[str]:
        return self.chunk_id_map.keys()

    def get_serialized_data(self, key: str) -> SerializedChunks:
        return self.chunk_id_map[key]


class ValuePedigree(InputsManifest):

    _kiara_model_id: ClassVar = "instance.value_pedigree"

    kiara_id: uuid.UUID = Field(
        description="The id of the kiara context a value was created in."
    )
    environments: Dict[str, str] = Field(
        description="References to the runtime environment details a value was created in."
    )

    def _retrieve_data_to_hash(self) -> Any:
        return {
            "manifest": self.manifest_cid,
            "inputs": self.inputs_cid,
            "environments": self.environments,
        }

    def __repr__(self):
        return f"ValuePedigree(module_type={self.module_type}, inputs=[{', '.join(self.inputs.keys())}], instance_id={self.instance_id})"

    def __str__(self):
        return self.__repr__()


class DataTypeInfo(KiaraModel):

    _kiara_model_id: ClassVar = "info.data_type_instance"

    data_type_name: str = Field(description="The registered name of this data type.")
    data_type_config: Mapping[str, Any] = Field(
        description="The (optional) configuration for this data type.",
        default_factory=dict,
    )
    characteristics: DataTypeCharacteristics = Field(
        description="Characteristics of this data type."
    )
    data_type_class: PythonClass = Field(
        description="The python class that is associated with this model."
    )
    _data_type_instance: "DataType" = PrivateAttr(default=None)

    @property
    def data_type_instance(self) -> "DataType":

        if self._data_type_instance is not None:
            return self._data_type_instance

        self._data_type_instance = self.data_type_class.get_class()(
            **self.data_type_config
        )
        return self._data_type_instance


class ValueDetails(KiaraModel):
    """A wrapper class that manages and retieves value data and its details."""

    _kiara_model_id: ClassVar = "instance.value_details"

    value_id: uuid.UUID = Field(description="The id of the value.")

    kiara_id: uuid.UUID = Field(
        description="The id of the kiara context this value belongs to."
    )

    value_schema: ValueSchema = Field(
        description="The schema that was used for this Value."
    )

    value_created: datetime = Field(
        description="The time when this value was created.",
        default_factory=get_current_time_incl_timezone,
    )
    value_status: ValueStatus = Field(description="The set/unset status of this value.")
    value_size: int = Field(description="The size of this value, in bytes.")
    value_hash: str = Field(description="The hash of this value.")
    pedigree: ValuePedigree = Field(
        description="Information about the module and inputs that went into creating this value."
    )
    pedigree_output_name: str = Field(
        description="The output name that produced this value (using the manifest inside the pedigree)."
    )
    data_type_info: DataTypeInfo = Field(
        description="Information about the data type this value is made of."
    )

    def _retrieve_id(self) -> str:
        return str(self.value_id)

    def _retrieve_data_to_hash(self) -> Any:
        return {
            "value_type": self.value_schema.type,
            "value_hash": self.value_hash,
            "value_size": self.value_size,
        }

    @property
    def data_type_name(self) -> str:
        return self.data_type_info.data_type_name

    @property
    def data_type_config(self) -> Mapping[str, Any]:
        return self.data_type_info.data_type_config

    @property
    def is_optional(self) -> bool:
        return self.value_schema.optional

    @property
    def is_valid(self) -> bool:
        """Check whether the current value is valid."""
        if self.is_optional:
            return True
        else:
            return self.value_status == ValueStatus.SET

    @property
    def is_set(self) -> bool:
        return self.value_status in [ValueStatus.SET, ValueStatus.DEFAULT]

    @property
    def value_status_string(self) -> str:
        """Print a human readable short description of this values status."""
        if self.value_status == ValueStatus.DEFAULT:
            return "set (default)"
        elif self.value_status == ValueStatus.SET:
            return "set"
        elif self.value_status == ValueStatus.NONE:
            result = "no value"
        elif self.value_status == ValueStatus.NOT_SET:
            result = "not set"
        else:
            raise Exception(
                f"Invalid internal status of value '{self.value_id}'. This is most likely a bug."
            )

        if self.is_optional:
            result = f"{result} (not required)"
        return result

    def __repr__(self):

        return f"{self.__class__.__name__}(id={self.value_id}, type={self.data_type_name}, status={self.value_status.value})"

    def __str__(self):

        return self.__repr__()


class Value(ValueDetails):

    _kiara_model_id: ClassVar = "instance.value"

    _value_data: Any = PrivateAttr(default=SpecialValue.NOT_SET)
    _serialized_data: Union[None, str, SerializedData] = PrivateAttr(default=None)
    _data_retrieved: bool = PrivateAttr(default=False)
    _data_registry: "DataRegistry" = PrivateAttr(default=None)
    # _data_type: "DataType" = PrivateAttr(default=None)
    _is_stored: bool = PrivateAttr(default=False)
    _cached_properties: Union["ValueMap", None] = PrivateAttr(default=None)
    _lineage: Union["ValueLineage", None] = PrivateAttr(default=None)

    environment_hashes: Mapping[str, Mapping[str, str]] = Field(
        description="Hashes for the environments this value was created in."
    )
    # enviroments: Union[Mapping[str, Mapping[str, Any]], None] = Field(
    #     description="Information about the environments this value was created in.",
    #     default=None,
    # )
    property_links: Mapping[str, uuid.UUID] = Field(
        description="Links to values that are properties of this value.",
        default_factory=dict,
    )
    destiny_backlinks: Mapping[uuid.UUID, str] = Field(
        description="Backlinks to values that this value acts as destiny/or property for.",
        default_factory=dict,
    )
    job_id: Union[uuid.UUID, None] = Field(
        description="The id of the job that created this value (if applicable).",
        default=None,
    )

    def add_property(
        self,
        value_id: Union[uuid.UUID, "Value"],
        property_path: str,
        add_origin_to_property_value: bool = True,
    ):

        value = None
        try:
            value_temp = value
            value_id = value_id.value_id  # type: ignore
            value = value_temp
        except Exception:
            # in case a Value object was provided
            pass
        finally:
            del value_temp

        if add_origin_to_property_value:
            if value is None:
                value = self._data_registry.get_value(value=value_id)  # type: ignore

            if value._is_stored:
                raise Exception(
                    f"Can't add property to value '{self.value_id}': referenced value '{value.value_id}' already locked, so it's not possible to add the property backlink (as requested)."
                )

        assert value is not None

        if self._is_stored:
            raise Exception(
                f"Can't add property to value '{self.value_id}': value already locked."
            )

        if property_path in self.property_links.keys():
            raise Exception(
                f"Can't add property to value '{self.value_id}': property '{property_path}' already set."
            )

        self.property_links[property_path] = value_id  # type: ignore

        if add_origin_to_property_value:
            value.add_destiny_details(
                value_id=self.value_id, destiny_alias=property_path
            )

        self._cached_properties = None

    def add_destiny_details(self, value_id: uuid.UUID, destiny_alias: str):

        if self._is_stored:
            raise Exception(
                f"Can't set destiny_refs to value '{self.value_id}': value already locked."
            )

        self.destiny_backlinks[value_id] = destiny_alias  # type: ignore

    @property
    def is_serializable(self) -> bool:

        try:
            if self._serialized_data == NO_SERIALIZATION_MARKER:
                return False
            self.serialized_data
            return True
        except Exception:
            pass

        return False

    # @property
    # def data_type_class(self) -> "PythonClass":
    #     """Return the (Python) type of the underlying 'DataType' subclass."""
    #     return self.data_type_info.data_type_class

    @property
    def serialized_data(self) -> SerializedData:

        # if not self.is_set:
        #     raise Exception(f"Can't retrieve serialized data: value not set.")

        if self._serialized_data is not None:
            if isinstance(self._serialized_data, str):
                raise Exception(
                    f"Data type '{self.data_type_name}' does not support serializing: {self._serialized_data}"
                )

            return self._serialized_data

        self._serialized_data = self._data_registry.retrieve_persisted_value_details(
            self.value_id
        )
        return self._serialized_data

    @property
    def data(self) -> Any:
        if not self.is_initialized:
            raise Exception(
                f"Can't retrieve data for value '{self.value_id}': value not initialized yet. This is most likely a bug."
            )
        try:
            return self._retrieve_data()
        except DataTypeUnknownException as dtue:
            dtue._value = self
            raise dtue

    def _retrieve_data(self) -> Any:

        if self._value_data is not SpecialValue.NOT_SET:
            return self._value_data

        if self.value_status in [ValueStatus.NOT_SET, ValueStatus.NONE]:
            self._value_data = None
            return self._value_data
        elif self.value_status not in [ValueStatus.SET, ValueStatus.DEFAULT]:
            raise Exception(f"Invalid internal state of value '{self.value_id}'.")

        retrieved = self._data_registry.retrieve_value_data(value=self)

        if retrieved is None or isinstance(retrieved, SpecialValue):
            raise Exception(
                f"Can't set value data, invalid data type: {type(retrieved)}"
            )

        self._value_data = retrieved
        self._data_retrieved = True
        return self._value_data

    # def retrieve_load_config(self) -> Optional[LoadConfig]:
    #     return self._data_registry.retrieve_persisted_value_details(
    #         value_id=self.value_id
    #     )

    def __repr__(self):

        return f"{self.__class__.__name__}(id={self.value_id}, type={self.data_type_name}, status={self.value_status.value}, initialized={self.is_initialized} optional={self.value_schema.optional})"

    def _set_registry(self, data_registry: "DataRegistry") -> None:
        self._data_registry = data_registry

    @property
    def is_initialized(self) -> bool:
        result = not self.is_set or self._data_registry is not None
        return result

    @property
    def is_stored(self) -> bool:
        return self._is_stored

    @property
    def data_type(self) -> "DataType":

        return self.data_type_info.data_type_instance

    @property
    def lineage(self) -> "ValueLineage":
        if self._lineage is not None:
            return self._lineage

        from kiara.models.values.lineage import ValueLineage

        self._lineage = ValueLineage(kiara=self._data_registry._kiara, value=self)
        return self._lineage

    @property
    def property_values(self) -> "ValueMap":
        """Return a dictionary of all of this values properties."""

        if self._cached_properties is not None:
            return self._cached_properties

        self._cached_properties = self._data_registry.load_values(self.property_links)
        return self._cached_properties

    @property
    def property_names(self) -> Iterable[str]:
        return self.property_links.keys()

    def get_property_value(self, property_key) -> "Value":

        if property_key not in self.property_links.keys():
            raise Exception(
                f"Value '{self.value_id}' has no property with key '{property_key}."
            )

        return self._data_registry.get_value(self.property_links[property_key])

    def get_property_data(self, property_key: str) -> Any:

        try:
            return self.get_property_value(property_key=property_key).data
        except Exception as e:
            log_exception(e)
            return None

    def get_all_property_data(self, flatten_models: bool = False) -> Mapping[str, Any]:

        result = {k: self.get_property_data(k) for k in self.property_names}
        if not flatten_models:
            return result

        flat = {}
        for k, v in result.items():
            if hasattr(v, "model_dump"):
                flat[k] = v.model_dump()
            elif hasattr(v, "dict"):
                flat[k] = v.dict()
            else:
                flat[k] = v
        return flat

    def lookup_self_aliases(self) -> Set[str]:

        if not self._data_registry:
            raise Exception(
                f"Can't lookup aliases for value '{self.value_id}': data registry not set (yet)."
            )

        return self._data_registry.lookup_aliases(self)

    def create_info(self) -> "ValueInfo":

        if not self._data_registry:
            raise Exception(
                f"Can't create info object for value '{self.value_id}': data registry not set (yet)."
            )

        return self._data_registry.create_value_info(value=self.value_id)

    def create_info_data(self, **config: Any) -> Mapping[str, Any]:

        show_pedigree = config.get("show_pedigree", False)
        show_lineage = config.get("show_lineage", False)
        show_properties = config.get("show_properties", False)
        # show_destinies = config.get("show_destinies", False)
        # show_destiny_backlinks = config.get("show_destiny_backlinks", False)
        # show_data = config.get("show_data_preview", False)
        show_serialized = config.get("show_serialized", False)
        show_env_data_hashes = config.get("show_environment_hashes", False)
        show_env_data = config.get("show_environment_data", False)

        ignore_fields = config.get("ignore_fields", [])

        table: Dict[str, Any] = {}

        if "value_id" not in ignore_fields:
            table["value_id"] = self.value_id
        if "aliases" not in ignore_fields:
            if hasattr(self, "aliases"):
                table["aliases"] = self.aliases  # type: ignore

        if "kiara_id" not in ignore_fields:
            table["kiara_id"] = self.kiara_id

        for k in sorted(self.model_fields.keys()):

            if (
                k
                in [
                    "serialized",
                    "value_id",
                    "aliases",
                    "kiara_id",
                    "environments",
                    "lineage",
                    "environment_hashes",
                ]
                or k in ignore_fields
            ):
                continue

            attr = getattr(self, k)
            if k in ["pedigree_output_name", "pedigree"]:
                continue
            else:
                v = attr

            table[k] = v

        if show_pedigree:
            pedigree = getattr(self, "pedigree")

            table["pedigree"] = pedigree
            if pedigree == ORPHAN:
                pedigree_output_name: Union[Any, None] = None
            else:
                pedigree_output_name = getattr(self, "pedigree_output_name")

            table["pedigree_output_name"] = pedigree_output_name

        if show_lineage:
            table["lineage"] = self.lineage

        if show_serialized:
            serialized = self._data_registry.retrieve_persisted_value_details(
                self.value_id
            )
            table["serialized"] = serialized

        if show_env_data_hashes:
            env_hashes = Syntax(
                orjson_dumps(self.environment_hashes, option=orjson.OPT_INDENT_2),
                "json",
                background_color="default",
            )
            table["environment_hashes"] = env_hashes

        if show_env_data:
            raise NotImplementedError()

        if show_properties:
            if not self.property_links:
                table["properties"] = {}
            else:
                properties = self._data_registry.load_values(self.property_links)
                table["properties"] = properties

        # if hasattr(self, "destiny_links") and show_destinies:
        #     if not self.destiny_links:  # type: ignore
        #         table["destinies"] = {}
        #     else:
        #         destinies = self._data_registry.load_values(self.destiny_links)  # type: ignore
        #         table["destinies"] = destinies
        #
        # if show_destiny_backlinks:
        #     if not self.destiny_backlinks:
        #         table["destiny backlinks"] = {}
        #     else:
        #         destiny_items: List[Any] = []
        #         for v_id, alias in self.destiny_backlinks.items():
        #             destiny_items.append(
        #                 f"[b]Value: [i]{v_id}[/i] (destiny alias: {alias})[/b]"
        #             )
        #             rendered = self._data_registry.pretty_print_data(
        #                 value_id=v_id, **config
        #             )
        #             destiny_items.append(rendered)
        #         table["destiny backlinks"] = destiny_items
        #
        # if show_data:
        #     rendered = self._data_registry.pretty_print_data(
        #         self.value_id, target_type="terminal_renderable"
        #     )
        #     table["data preview"] = rendered

        return table

    def create_renderable(self, **render_config: Any) -> RenderableType:

        from kiara.utils.output import extract_renderable

        show_pedigree = render_config.get("show_pedigree", False)
        show_lineage = render_config.get("show_lineage", False)
        show_properties = render_config.get("show_properties", False)
        show_destinies = render_config.get("show_destinies", False)
        show_destiny_backlinks = render_config.get("show_destiny_backlinks", False)
        show_data = render_config.get("show_data_preview", False)
        show_serialized = render_config.get("show_serialized", False)
        show_env_data_hashes = render_config.get("show_environment_hashes", False)
        show_env_data = render_config.get("show_environment_data", False)

        ignore_fields = render_config.get("ignore_fields", [])

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")

        info_data = self.create_info_data(**render_config)

        if "value_id" not in ignore_fields:
            table.add_row("value_id", str(info_data["value_id"]))
        if "aliases" not in ignore_fields:
            if info_data.get("aliases", None):
                aliases_str = ", ".join(info_data["aliases"])  # type: ignore
                table.add_row("aliases", aliases_str)
            # else:
            #     aliases_str = "-- n/a --"
            #     table.add_row("aliases", aliases_str)

        if "kiara_id" not in ignore_fields:
            table.add_row("kiara_id", str(info_data["kiara_id"]))

        table.add_row("", "")
        table.add_row("", Rule())
        for k in sorted(info_data.keys()):

            if (
                k
                in [
                    "serialized",
                    "value_id",
                    "aliases",
                    "kiara_id",
                    "lineage",
                    "properties",
                    "environments",
                    "environment_hashes",
                ]
                or k in ignore_fields
            ):
                continue

            attr = info_data[k]
            if k in ["pedigree_output_name", "pedigree"]:
                continue

            elif k == "value_status":
                v: RenderableType = f"[i]-- {attr.value} --[/i]"
            elif k == "value_size":
                v = format_size(attr)
            else:
                v = extract_renderable(attr)

            table.add_row(k, v)

        if (
            show_pedigree
            or show_lineage
            or show_serialized
            or show_properties
            or show_destinies
            or show_destiny_backlinks
            or show_env_data_hashes
            or show_env_data
        ):
            table.add_row("", "")
            table.add_row("", Rule())
            table.add_row("", "")

        if show_pedigree:
            pedigree = info_data["pedigree"]

            if pedigree == ORPHAN:
                v = "[i]-- external data --[/i]"
                pedigree_output_name: Union[Any, None] = None
            else:
                v = extract_renderable(pedigree)
                pedigree_output_name = info_data["pedigree_output_name"]

            row = ["pedigree", v]
            table.add_row(*row)
            if pedigree_output_name:
                row = ["pedigree_output_name", pedigree_output_name]
                table.add_row(*row)

        if show_lineage:
            table.add_row(
                "lineage", info_data["lineage"].create_renderable(include_ids=True)
            )

        if show_serialized:
            serialized = info_data["serialized"]
            table.add_row("serialized", serialized.create_renderable())

        if show_env_data_hashes:
            env_hashes = Syntax(
                orjson_dumps(
                    info_data["environment_hashes"], option=orjson.OPT_INDENT_2
                ),
                "json",
                background_color="default",
            )
            table.add_row("environment_hashes", env_hashes)

        if show_env_data:
            raise NotImplementedError()

        if show_properties:
            if not info_data["properties"]:
                table.add_row("properties", "{}")
            else:
                properties = info_data["properties"]
                pr = properties.create_renderable(show_header=False)
                table.add_row("properties", pr)

        if hasattr(self, "destiny_links") and show_destinies:
            if not self.destiny_links:  # type: ignore
                table.add_row("destinies", "{}")
            else:
                destinies = self._data_registry.load_values(self.destiny_links)  # type: ignore
                dr = destinies.create_renderable(show_header=False)
                table.add_row("destinies", dr)

        if show_destiny_backlinks:
            if not self.destiny_backlinks:
                table.add_row("destiny backlinks", "{}")
            else:
                destiny_items: List[Any] = []
                for v_id, alias in self.destiny_backlinks.items():
                    destiny_items.append(Rule())
                    destiny_items.append(
                        f"[b]Value: [i]{v_id}[/i] (destiny alias: {alias})[/b]"
                    )
                    rendered = self._data_registry.pretty_print_data(
                        value_id=v_id, **render_config
                    )
                    destiny_items.append(rendered)
                table.add_row("destiny backlinks", Group(*destiny_items))

        if show_data:
            rendered = self._data_registry.pretty_print_data(
                self.value_id, target_type="terminal_renderable"
            )
            table.add_row("", "")
            table.add_row("", Rule())
            table.add_row("data preview", rendered)

        return table


class UnloadableData(KiaraModel):
    """
    A special 'marker' model, indicating that the data of value can't be loaded.

    In most cases, the reason this happens is because the current kiara context is missing some value types and/or modules.
    """

    _kiara_model_id: ClassVar = "instance.unloadable_data"

    value: Value = Field(description="A reference to the value.")

    def _retrieve_id(self) -> str:
        return self.value.instance_id

    def _retrieve_data_to_hash(self) -> Any:
        return self.value.value_id.bytes


class ValueMap(KiaraModel, MutableMapping[str, Value]):  # type: ignore

    values_schema: Dict[str, ValueSchema] = Field(
        description="The schemas for all the values in this set."
    )

    @property
    def field_names(self) -> Iterable[str]:
        return sorted(self.values_schema.keys())

    @abc.abstractmethod
    def get_value_obj(self, field_name: str) -> Value:
        pass

    @property
    def all_items_valid(self) -> bool:
        for field_name in self.values_schema.keys():
            item = self.get_value_obj(field_name)
            if not item.is_valid:
                return False
        return True

    def _retrieve_data_to_hash(self) -> Any:
        return {
            k: self.get_value_obj(k).instance_cid for k in self.values_schema.keys()
        }

    def check_invalid(self) -> Dict[str, str]:
        """Check whether the value set is invalid, if it is, return a description of what's wrong."""
        invalid: Dict[str, str] = {}
        for field_name in self.values_schema.keys():

            item = self.get_value_obj(field_name)
            field_schema = self.values_schema[field_name]
            if not field_schema.optional:
                msg: Union[str, None] = None
                if not item.value_status == ValueStatus.SET:

                    item_schema = self.values_schema[field_name]
                    if item_schema.is_required():

                        if not item.is_set:
                            msg = "not set"
                        elif item.value_status == ValueStatus.NONE:
                            msg = "no value"
                if msg:
                    invalid[field_name] = msg

        return invalid

    def get_value_data_for_fields(
        self, *field_names: str, raise_exception_when_unset: bool = False
    ) -> Dict[str, Any]:
        """
        Return the data for a one or several fields of this ValueMap.

        If a value is unset, by default 'None' is returned for it. Unless 'raise_exception_when_unset' is set to 'True',
        in which case an Exception will be raised (obviously).
        """
        if raise_exception_when_unset:
            unset: List[str] = []
            for k in field_names:
                v = self.get_value_obj(k)
                if not v.is_set:
                    if raise_exception_when_unset:
                        unset.append(k)
            if unset:
                raise Exception(
                    f"Can't get data for fields, one or several of the requested fields are not set yet: {', '.join(unset)}."
                )

        result: Dict[str, Any] = {}
        for k in field_names:
            v = self.get_value_obj(k)
            if not v.is_set:
                result[k] = None
            else:
                result[k] = v.data
        return result

    def get_value_data(
        self, field_name: str, raise_exception_when_unset: bool = False
    ) -> Any:
        return self.get_value_data_for_fields(
            field_name, raise_exception_when_unset=raise_exception_when_unset
        )[field_name]

    def get_all_value_ids(self) -> Dict[str, uuid.UUID]:
        return {k: self.get_value_obj(k).value_id for k in self.field_names}

    def get_all_value_data(
        self, raise_exception_when_unset: bool = False
    ) -> Dict[str, Any]:
        return self.get_value_data_for_fields(
            *self.field_names,
            raise_exception_when_unset=raise_exception_when_unset,
        )

    def set_values(self, **values) -> None:

        for k, v in values.items():
            self.set_value(k, v)

    def set_value(self, field_name: str, data: Any) -> None:
        raise Exception(
            f"The value set implementation '{self.__class__.__name__}' is read-only, and does not support the setting or changing of values."
        )

    def __getitem__(self, item: str) -> Value:

        return self.get_value_obj(item)

    def __setitem__(self, key: str, value):

        raise NotImplementedError()
        # self.set_value(key, value)

    def __delitem__(self, key: str):

        raise Exception(f"Removing items not supported: {key}")

    def __iter__(self):
        return iter(self.field_names)

    def __len__(self):
        return len(list(self.values_schema))

    def __repr__(self):
        return f"{self.__class__.__name__}(field_names={self.field_names})"

    def __str__(self):
        return self.__repr__()

    def create_invalid_renderable(self, **config) -> Union[RenderableType, None]:

        inv = self.check_invalid()
        if not inv:
            return None

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("field name", style="i")
        table.add_column("details", style="b red")

        for field, err in inv.items():
            table.add_row(field, err)

        return table

    def create_renderable(self, **config: Any) -> RenderableType:

        in_panel = config.get("in_panel", None)
        if in_panel is None:
            if is_jupyter():
                in_panel = True
            else:
                in_panel = False

        render_value_data = config.get("render_value_data", True)
        field_title = config.get("field_title", "field")
        value_title = config.get("value_title", "value")
        show_header = config.get("show_header", True)
        show_type = config.get("show_data_type", False)

        table = Table(show_lines=False, show_header=show_header, box=box.SIMPLE)
        table.add_column(field_title, style="b")
        if show_type:
            table.add_column("data_type")
        table.add_column(value_title, style="i")

        for field_name in self.field_names:

            value = self.get_value_obj(field_name=field_name)
            if render_value_data:
                rendered = value._data_registry.pretty_print_data(
                    value_id=value.value_id, target_type="terminal_renderable", **config
                )
            else:
                rendered = value.create_renderable(**config)

            if show_type:
                table.add_row(field_name, value.value_schema.type, rendered)
            else:
                table.add_row(field_name, rendered)

        if in_panel:
            return Panel(table)
        else:
            return table


class ValueMapReadOnly(ValueMap):  # type: ignore

    _kiara_model_id: ClassVar = "instance.value_map.readonly"

    @classmethod
    def create_from_ids(cls, data_registry: "DataRegistry", **value_ids: uuid.UUID):

        values = {k: data_registry.get_value(v) for k, v in value_ids.items()}
        values_schema = {k: v.value_schema for k, v in values.items()}
        return ValueMapReadOnly(value_items=values, values_schema=values_schema)

    @classmethod
    def create_from_values(cls, **values: Value) -> "ValueMapReadOnly":

        values_schema = {k: v.value_schema for k, v in values.items()}
        return ValueMapReadOnly(value_items=values, values_schema=values_schema)

    value_items: Dict[str, Value] = Field(
        description="The values contained in this set."
    )

    def get_value_obj(self, field_name: str) -> Value:

        if field_name not in self.value_items.keys():
            raise KeyError(
                f"Field '{field_name}' not available in value set. Available fields: {', '.join(self.field_names)}"
            )
        return self.value_items[field_name]


class ValueMapWritable(ValueMap):  # type: ignore

    _kiara_model_id: ClassVar = "instance.value_map.writeable"

    @classmethod
    def create_from_schema(
        cls,
        kiara: "Kiara",
        schema: Mapping[str, ValueSchema],
        pedigree: ValuePedigree,
        unique_value_ids: bool = False,
    ) -> "ValueMapWritable":

        v = ValueMapWritable(
            values_schema=dict(schema),
            pedigree=pedigree,
            unique_value_ids=unique_value_ids,
        )
        v._kiara = kiara
        v._data_registry = kiara.data_registry
        return v

    value_items: Dict[str, Value] = Field(
        description="The values contained in this set.", default_factory=dict
    )
    pedigree: ValuePedigree = Field(
        description="The pedigree to add to all of the result values."
    )
    unique_value_ids: bool = Field(
        description="Whether this value map always creates new value(id)s, even when a dataset with matching hash is found.",
        default=True,
    )

    _values_uncommitted: Dict[str, Any] = PrivateAttr(default_factory=dict)
    _kiara: "Kiara" = PrivateAttr(default=None)
    _data_registry: "DataRegistry" = PrivateAttr(default=None)
    _auto_commit: bool = PrivateAttr(default=True)

    def get_value_obj(self, field_name: str) -> Value:
        """
        Retrieve the value object for the specified field.

        This class only creates the actual value object the first time it is requested, because there is a potential
        cost to assembling it, and it might not be needed ever.
        """
        if field_name not in self.values_schema.keys():
            raise Exception(
                f"Can't set data for field '{field_name}': field not valid, valid field names: {', '.join(self.field_names)}."
            )

        if field_name in self.value_items.keys():
            return self.value_items[field_name]
        elif field_name not in self._values_uncommitted.keys():
            raise Exception(
                f"Can't retrieve value for field '{field_name}': value not set (yet)."
            )

        schema = self.values_schema[field_name]
        value_data = self._values_uncommitted[field_name]
        if isinstance(value_data, Value):
            value = value_data
        elif isinstance(value_data, uuid.UUID):
            value = self._data_registry.get_value(value_data)
        else:
            value = self._data_registry.register_data(
                data=value_data,
                schema=schema,
                pedigree=self.pedigree,
                pedigree_output_name=field_name,
                reuse_existing=not self.unique_value_ids,
            )

        self._values_uncommitted.pop(field_name)
        self.value_items[field_name] = value
        return self.value_items[field_name]

    def sync_values(self):

        for field_name in self.field_names:
            self.get_value_obj(field_name)

        invalid = self.check_invalid()
        if invalid:
            e = InvalidValuesException(invalid_values=invalid)
            try:
                raise e
            except Exception:
                # this is silly, I know
                log_exception(e)
                raise e

    def set_value(self, field_name: str, data: Any) -> None:
        """Set the value for the specified field."""
        if field_name not in self.field_names:
            raise Exception(
                f"Can't set data for field '{field_name}': field not valid, valid field names: {', '.join(self.field_names)}."
            )
        if self.value_items.get(field_name, False):
            raise Exception(
                f"Can't set data for field '{field_name}': field already committed."
            )
        if self._values_uncommitted.get(field_name, None) is not None:
            raise Exception(
                f"Can't set data for field '{field_name}': field already set."
            )

        self._values_uncommitted[field_name] = data
        if self._auto_commit:
            self.get_value_obj(field_name=field_name)


ValuePedigree.model_rebuild()
ORPHAN = ValuePedigree(
    kiara_id=VOID_KIARA_ID,
    environments={},
    module_type=NO_MODULE_TYPE,
    inputs={},
    is_resolved=True,
)
# GENESIS_PEDIGREE = None
