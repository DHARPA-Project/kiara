import abc
import os
from typing import Iterable, Mapping, Union, Any, Dict, Optional

import mmh3
from pydantic import Field, validator

from kiara.modules import KiaraModule
from kiara.defaults import LOAD_CONFIG_VALUE_TYPE_NAME
from kiara.exceptions import KiaraProcessingException
from kiara.models.module import ModuleTypeConfigSchema
from kiara.models.values.value import ValueSet, Value
from kiara.models.values.value_schema import ValueSchema
from kiara.value_types.included_core_types.persistence import LoadConfig


class PersistValueConfig(ModuleTypeConfigSchema):

    source_type: str = Field(description="The value type of the source.")

    @validator("source_type")
    def validate_source_type(cls, value):
        if value == "persist_config":
            raise ValueError(f"Invalid source type: {value}.")
        return value

class PersistValueModule(KiaraModule):

    _config_cls = PersistValueConfig

    @classmethod
    def retrieve_supported_source_types(cls) -> Iterable[str]:

        result = []
        for attr in dir(cls):
            if attr.startswith("value_type__"):
                result.append(attr[12:])
        return result

    # def create_persistence_config_schema(self) -> Optional[Mapping[str, Mapping[str, Any]]]:
    #     return None

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        source_type = self.get_config_value("source_type")
        assert source_type not in ["target", "base_name"]

        schema = {
            source_type: {
                "type": source_type,
                "doc": "The value to serialize."
            },
            "target": {
                "type": "string",
                "doc": "The folder to store the value in."
            },
            "base_name": {
                "type": "string",
                "doc": "The base name for the saved file. Depending on the value type, the extension might be automatically determined.",
                "optional": True
            }
        }

        return schema

    def create_outputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        return {
            "load_config": {
                "type": LOAD_CONFIG_VALUE_TYPE_NAME,
                "type_config": {
                    "persistence_target": self.get_persistence_target_name(),
                    "persistence_format": self.get_persistence_format_name()
                },
                "doc": "The value in serialized form."
            }
        }

    @abc.abstractmethod
    def get_persistence_target_name(self) -> str:
        pass

    @abc.abstractmethod
    def get_persistence_format_name(self) -> str:
        pass

    def process(
        self, inputs: ValueSet, outputs: ValueSet
    ) -> None:

        source_type = self.get_config_value("source_type")
        value = inputs.get_value_obj(source_type)

        target = inputs.get_value_data("target")
        base_name = inputs.get_value_data("base_name")
        # input_fields = set(inputs.field_names)
        # input_fields.remove(source_type)
        #
        # persistence_config = inputs.get_value_data_for_fields(*input_fields)

        func_name = f"value_type__{self.get_config_value('source_type')}"
        func = getattr(self, func_name)

        result: LoadConfig = func(value=value, target=target, base_name=base_name)
        outputs.set_value("load_config", result)


class SavePickleToDiskModule(PersistValueModule):

    _module_type_name = "value.save_to.disk.as.pickle"

    def create_persistence_config_schema(self) -> Optional[Mapping[str, Mapping[str, Any]]]:
        return {

        }

    def get_persistence_target_name(self) -> str:
        return "disk"

    def get_persistence_format_name(self) -> str:
        return "pickle_file"

    def value_type__any(self, value: Value, target: str, base_name: str) -> LoadConfig:
        """Serialize any Python object."""

        import pickle5 as pickle

        os.makedirs(target, exist_ok=True)

        target_file = os.path.join(target, f"{base_name}.pickle")

        # value_hash = mmh3.hash_from_buffer(value.value_data)
        # value_size = len(value.value_data)

        with open(target_file, 'wb') as f:
            pickle.dump(value.data, f, protocol=5)

        # value_size = os.path.getsize(target_file)
        # print("-------------")
        # print(value_size)
        # print(file_size)

        load_config_data = {
            "inputs": {
                "path": target_file
            },
            "module_type": "value.load.pickle_file.from.disk",
            "module_config": {
                "value_type": value.value_schema.type
            },
            "output_name": value.value_schema.type
        }

        load_config = LoadConfig(**load_config_data)
        return load_config

class LoadFromDiskConfig(ModuleTypeConfigSchema):

    value_type: str = Field(description="The value type the deserialized data should have.")


class LoadPickleFromDiskModule(KiaraModule):

    _module_type_name = "value.load.pickle_file.from.disk"
    _config_cls = LoadFromDiskConfig

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        return {
            "path": {
                "type": "string",
                "doc": "The path to the pickle file."
            }
        }

    def create_outputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        value_type = self.get_config_value("value_type")
        return {
            value_type: {
                "type": value_type,
                "doc": f"The deserialized {value_type} value, loaded from disk."
            }
        }

    def process(self, inputs: ValueSet, outputs: ValueSet):

        import  pickle5 as pickle

        value_type = self.get_config_value("value_type")

        path = inputs.get_value_data("path")
        if not os.path.isfile(path):
            raise KiaraProcessingException(f"No pickle file found on path: {path}")

        with open(path, 'rb') as f:
            data = pickle.load(f)

        outputs.set_value(value_type, data)

