# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Mapping, Type

from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types import KiaraModelValueBaseType
from kiara.models import KiaraModel

if TYPE_CHECKING:
    from kiara.models.values.value import SerializedData


class Metadata(KiaraModel):

    _kiara_model_id: ClassVar = "instance.metadata"


class MetadataTypeConfig(DataTypeConfig):

    pass


class MetadataValueType(KiaraModelValueBaseType[Metadata, MetadataTypeConfig]):
    """A file."""

    _data_type_name: ClassVar[str] = "file"

    @classmethod
    def retrieve_available_type_profiles(cls) -> Mapping[str, Mapping[str, Any]]:
        return {}

    @classmethod
    def python_class(cls) -> Type:
        return Metadata

    @classmethod
    def data_type_config_class(cls) -> Type[MetadataTypeConfig]:
        return MetadataTypeConfig

    def serialize(self, data: Metadata) -> "SerializedData":

        # _data = {
        #     data.file_name: {
        #         "type": "file",
        #         "codec": "raw",
        #         "file": data.path,
        #     },
        #     "__file_metadata__": {
        #         "type": "inline-json",
        #         "codec": "json",
        #         "inline_data": {
        #             "file_name": data.file_name,
        #             # "import_time": data.import_time,
        #         },
        #     },
        # }
        _data: Dict[str, Any] = {}

        serialized_data = {
            "data_type": self.data_type_name,
            "data_type_config": self.type_config.model_dump(),
            "data": _data,
            "serialization_profile": "copy",
            "metadata": {
                # "profile": "",
                "environment": {},
                "deserialize": {
                    "python_object": {
                        "module_type": "deserialize.file",
                        "module_config": {
                            "value_type": "file",
                            "target_profile": "python_object",
                            "serialization_profile": "copy",
                        },
                    }
                },
            },
        }
        from kiara.models.values.value import SerializationResult

        serialized = SerializationResult(**serialized_data)
        return serialized

    def create_model_from_python_obj(self, data: Any) -> Metadata:

        # if isinstance(data, Mapping):
        #     return Metadata(**data)
        # if isinstance(data, str):
        #     return Metadata.load_file(source=data)
        # else:
        raise Exception(
            f"Can't create Metadata instance from data of type '{type(data)}'."
        )
