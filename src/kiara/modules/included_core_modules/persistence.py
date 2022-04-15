# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import orjson
from pydantic import Field, validator
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Union

from kiara.defaults import LOAD_CONFIG_DATA_TYPE_NAME
from kiara.exceptions import KiaraProcessingException
from kiara.models import KiaraModel
from kiara.models.module import KiaraModuleConfig
from kiara.models.module.persistence import (
    ByteProvisioningStrategy,
    BytesStructure,
    LoadConfig,
)
from kiara.models.python_class import PythonClass
from kiara.models.values.value import Value, ValueMap
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import KiaraModule, ModuleCharacteristics, ValueSetSchema
from kiara.utils import orjson_dumps


class PersistValueConfig(KiaraModuleConfig):

    source_type: str = Field(description="The value type of the source.")
    source_type_config: Dict[str, Any] = Field(
        description="The value type config (if applicable).", default_factory=dict
    )

    @validator("source_type")
    def validate_source_type(cls, value):
        if value == "persist_config":
            raise ValueError(f"Invalid source type: {value}.")
        return value


class LoadDataModuleConfig(KiaraModuleConfig):

    data_type: str = Field(description="The data type of the deserialized data.")


class PersistValueModule(KiaraModule):

    _config_cls = PersistValueConfig

    @classmethod
    def retrieve_supported_source_types(cls) -> Iterable[str]:

        result = []
        for attr in dir(cls):
            if attr.startswith("data_type__"):
                result.append(attr[11:])
        return result

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        source_type = self.get_config_value("source_type")
        assert source_type not in ["target", "base_name"]

        schema = {
            source_type: {"type": source_type, "doc": "The value to serialize."},
        }

        return schema

    def create_outputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        return {
            "load_config": {
                "type": LOAD_CONFIG_DATA_TYPE_NAME,
                "type_config": {
                    "persistence_target": self.get_persistence_target_name(),
                    "persistence_format": self.get_persistence_format_name(),
                },
                "doc": "The value in serialized form.",
            },
            "bytes_structure": {
                "type": "any",
                "doc": "The actual serialized value bytes or path to saved files containing the value data.",
                "optional": True,
            },
        }

    @abc.abstractmethod
    def get_persistence_target_name(self) -> str:
        pass

    @abc.abstractmethod
    def get_persistence_format_name(self) -> str:
        pass

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:

        source_type = self.get_config_value("source_type")
        value = inputs.get_value_obj(source_type)

        func_name = f"data_type__{self.get_config_value('source_type')}"
        func = getattr(self, func_name)

        result: LoadConfig
        bytes_structure: Optional[BytesStructure]
        result, bytes_structure = func(value=value, persistence_config={"x": "y"})

        outputs.set_values(load_config=result, bytes_structure=bytes_structure)


class SaveInlineDataModule(PersistValueModule):

    _module_type_name = "value.save_inline"

    def get_persistence_target_name(self) -> str:
        return "inline"

    def get_persistence_format_name(self) -> str:
        return "json_string"

    def _create_json(self, data: Any, type_hint: str):

        try:

            all_json = orjson_dumps({"value": data})

            load_config_data = {
                "provisioning_strategy": ByteProvisioningStrategy.INLINE,
                "module_type": "value.load_inline",
                "module_config": {"data_type": type_hint},
                "inputs": {"json_data": all_json},
                "output_name": "value",
            }

            load_config = LoadConfig(**load_config_data)
        except Exception as e:
            raise KiaraProcessingException(
                f"Can't serialize value of type '{type_hint}' to json: {e}."
            )

        return load_config

    def data_type__string(self, value: Value, persistence_config: Mapping[str, Any]):

        load_config = self._create_json(
            data=value.data, type_hint=value.value_schema.type
        )
        return load_config, None


class LoadInlineDataModule(KiaraModule):

    _module_type_name = "value.load_inline"
    _config_cls = LoadDataModuleConfig

    def create_inputs_schema(
        self,
    ) -> ValueSetSchema:

        return {
            "json_data": {
                "type": "string",
                "doc": "The serialized data as json string.",
            }
        }

    def create_outputs_schema(
        self,
    ) -> ValueSetSchema:

        data_type = self.get_config_value("data_type")
        return {
            "value": {"type": data_type, "doc": f"The deserialized {data_type} value."}
        }

    def process(self, inputs: ValueMap, outputs: ValueMap):

        data_str = inputs.get_value_data("json_data")
        data = orjson.loads(data_str)

        outputs.set_value("value", data["value"])


class SavePickleToDiskModule(PersistValueModule):

    _module_type_name = "value.save_to.disk.as.pickle"

    def get_persistence_target_name(self) -> str:
        return "disk"

    def get_persistence_format_name(self) -> str:
        return "pickle_file"

    def data_type__any(
        self, value: Value, persistence_config: Mapping[str, Any]
    ) -> Tuple[LoadConfig, Optional[BytesStructure]]:
        """Persist any Python object using 'pickle'."""

        import pickle5 as pickle

        pickled_bytes = pickle.dumps(value.data, protocol=5)

        bytes_structure_data: Dict[str, Any] = {
            "data_type": value.value_schema.type,
            "data_type_config": value.value_schema.type_config,
            "chunk_map": {"serialized_value.pickle": [pickled_bytes]},
        }

        bytes_structure = BytesStructure.construct(**bytes_structure_data)

        load_config_data = {
            "provisioning_strategy": ByteProvisioningStrategy.BYTES,
            "module_type": "value.load_pickled_data",
            "module_config": {
                "data_type": value.value_schema.type,
            },
            "inputs": {"bytes_structure": "__dummy__"},
            "output_name": value.value_schema.type,
        }

        load_config = LoadConfig(**load_config_data)
        return load_config, bytes_structure


class LoadPickledDataModule(KiaraModule):

    _module_type_name = "value.load_pickled_data"
    _config_cls = LoadDataModuleConfig

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        return {"bytes_structure": {"type": "any", "doc": "The raw pickle data."}}

    def create_outputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        data_type_name = self.get_config_value("data_type")
        return {
            data_type_name: {
                "type": data_type_name,
                "doc": f"The deserialized {data_type_name} value, loaded from disk.",
            }
        }

    def process(self, inputs: ValueMap, outputs: ValueMap):

        import pickle5 as pickle

        data_type_name = self.get_config_value("data_type")

        bytes_structure: BytesStructure = inputs.get_value_data("bytes_structure")

        assert len(bytes_structure.chunk_map) == 1

        key = next(iter(bytes_structure.chunk_map))
        value = bytes_structure.chunk_map[key]

        data = pickle.loads(value)

        outputs.set_value(data_type_name, data)


class SaveInternalModelModule(PersistValueModule):
    """Persist internally used model data."""

    _module_type_name = "internal_model.save_to.disk.as.json_file"

    def get_persistence_target_name(self) -> str:
        return "disk"

    def get_persistence_format_name(self) -> str:
        return "json_file"

    def data_type__internal_model(
        self, value: Value, persistence_config: Mapping[str, Any]
    ) -> Tuple[LoadConfig, Optional[BytesStructure]]:
        """Persist internally used model data as a json file."""

        try:
            value_metadata: KiaraModel = value.data
            data_json = value_metadata.json()
            python_class_json = PythonClass.from_class(value_metadata.__class__).json()
            all_json = (
                '{"data": ' + data_json + ', "python_class": ' + python_class_json + "}"
            )

            load_config_data = {
                "provisioning_strategy": ByteProvisioningStrategy.INLINE,
                "module_type": "internal_model.load_from_store",
                "inputs": {"model_data": all_json},
                "output_name": "internal_model",
            }

            load_config = LoadConfig(**load_config_data)
        except Exception as e:
            raise KiaraProcessingException(
                f"Can't serialize value of type '{value.value_schema.type}' to json: {e}."
            )

        return load_config, None


class LoadInternalModelModule(KiaraModule):
    """Load a json file from disk and create a kiara value from it."""

    _module_type_name = "internal_model.load_from_store"

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        return {
            "model_data": {
                "type": "any",
                "doc": "The serialized model data.",
            }
        }

    def create_outputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        return {
            "internal_model": {
                "type": "internal_model",
                "doc": "The deserialized internal_model value, loaded from the data store.",
            }
        }

    def _retrieve_module_characteristics(self) -> ModuleCharacteristics:
        return ModuleCharacteristics(is_internal=True)

    def process(self, inputs: ValueMap, outputs: ValueMap):

        data_str = inputs.get_value_data("model_data")
        data = orjson.loads(data_str)

        model_data = data["data"]
        python_class_data = data["python_class"]

        model_cls = PythonClass(**python_class_data).get_class()

        model = model_cls(**model_data)
        outputs.set_value("internal_model", model)
