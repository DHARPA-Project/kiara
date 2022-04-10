# -*- coding: utf-8 -*-
import abc
import os
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
from kiara.models.values.value import Value, ValueSet
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import KiaraModule, ModuleCharacteristics


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

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        source_type = self.get_config_value("source_type")
        value = inputs.get_value_obj(source_type)

        func_name = f"data_type__{self.get_config_value('source_type')}"
        func = getattr(self, func_name)

        result: LoadConfig
        bytes_structure: Optional[BytesStructure]
        result, bytes_structure = func(value=value, persistence_config={"x": "y"})

        outputs.set_values(load_config=result, bytes_structure=bytes_structure)


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

        # os.makedirs(target, exist_ok=True)
        # target_file = os.path.join(target, f"{base_name}.pickle")
        # with open(target_file, "wb") as f:

        pickled_bytes = pickle.dumps(value.data, protocol=5)

        bytes_structure_data = {
            "data_type": value.value_schema.type,
            "data_type_config": value.value_schema.type_config,
            "bytes_map": {"serialized_value.pickle": pickled_bytes},
        }

        bytes_structure = BytesStructure.construct(**bytes_structure_data)

        load_config_data = {
            "provisioning_strategy": ByteProvisioningStrategy.BYTES,
            "module_type": "value.load.pickled_data",
            "module_config": {
                "data_type": value.value_schema.type,
                "data_type_config": value.value_schema.type_config,
            },
            "inputs": {"serialized_value": "serialized_value.pickle"},
            "output_name": value.value_schema.type,
        }

        load_config = LoadConfig(**load_config_data)
        return load_config, bytes_structure


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


class LoadInternalModelFromDiskModule(KiaraModule):
    """Load a json file from disk and create a kiara value from it."""

    _module_type_name = "internal_model.load_from.json_file"

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        return {
            "model_data": {
                "type": "dict",
                "doc": "The serialized model data.",
            }
        }

    def create_outputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        return {
            "internal_model": {
                "type": "internal_model",
                "doc": f"The deserialized internal_model value, loaded from the data store.",
            }
        }

    def _retrieve_module_characteristics(self) -> ModuleCharacteristics:
        return ModuleCharacteristics(is_internal=True)

    def process(self, inputs: ValueSet, outputs: ValueSet):

        model_data = inputs.get_value_data("data")
        python_class_data = inputs.get_value_data("python_class")

        model_cls = PythonClass(**python_class_data).get_class()

        model = model_cls(**model_data)
        outputs.set_value("internal_model", model)


class LoadFromDiskConfig(KiaraModuleConfig):

    data_type: str = Field(description="The value type of the deserialized data.")


class LoadPickleFromDiskModule(KiaraModule):

    _module_type_name = "value.load.pickle_file.from.disk"
    _config_cls = LoadFromDiskConfig

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        return {"pickle_file": {"type": "file", "doc": "The pickle file."}}

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

    def process(self, inputs: ValueSet, outputs: ValueSet):

        import pickle5 as pickle

        data_type_name = self.get_config_value("data_type")

        pickle_file = inputs.get_value_data("pickle_file")
        path = pickle_file.path

        if not os.path.isfile(path):
            raise KiaraProcessingException(f"No pickle file found on path: {path}")

        with open(path, "rb") as f:
            data = pickle.load(f)

        outputs.set_value(data_type_name, data)
