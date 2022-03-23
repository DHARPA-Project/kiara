# -*- coding: utf-8 -*-
import abc
import orjson
import os
from pydantic import Field, validator
from typing import Any, Dict, Iterable, Mapping, Union

from kiara.defaults import LOAD_CONFIG_DATA_TYPE_NAME
from kiara.exceptions import KiaraProcessingException
from kiara.models import KiaraModel
from kiara.models.filesystem import FileModel
from kiara.models.module import KiaraModuleConfig
from kiara.models.module.manifest import LoadConfig
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
            "target": {"type": "string", "doc": "The folder to store the value in."},
            "base_name": {
                "type": "string",
                "doc": "The base name for the saved file. Depending on the value type, the extension might be automatically determined.",
                "optional": True,
            },
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
            }
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

        target = inputs.get_value_data("target")
        base_name = inputs.get_value_data("base_name")
        # input_fields = set(inputs.field_names)
        # input_fields.remove(source_type)
        #
        # persistence_config = inputs.get_value_data_for_fields(*input_fields)

        func_name = f"data_type__{self.get_config_value('source_type')}"
        func = getattr(self, func_name)

        result: LoadConfig = func(value=value, target=target, base_name=base_name)
        outputs.set_value("load_config", result)


class SavePickleToDiskModule(PersistValueModule):

    _module_type_name = "value.save_to.disk.as.pickle"

    def get_persistence_target_name(self) -> str:
        return "disk"

    def get_persistence_format_name(self) -> str:
        return "pickle_file"

    def data_type__any(self, value: Value, target: str, base_name: str) -> LoadConfig:
        """Persist any Python object using 'pickle'."""

        import pickle5 as pickle

        os.makedirs(target, exist_ok=True)

        target_file = os.path.join(target, f"{base_name}.pickle")

        # value_hash = mmh3.hash_from_buffer(value.value_data)
        # value_size = len(value.value_data)

        with open(target_file, "wb") as f:
            pickle.dump(value.data, f, protocol=5)

        file_model = FileModel.load_file(target_file)
        load_config_data = {
            "inputs": {"pickle_file": file_model},
            "module_type": "value.load.pickle_file.from.disk",
            "module_config": {"data_type": value.value_schema.type},
            "output_name": value.value_schema.type,
        }

        load_config = LoadConfig(**load_config_data)
        return load_config


class SaveInternalModelModule(PersistValueModule):
    """Persist internally used model data."""

    _module_type_name = "internal_model.save_to.disk.as.json_file"

    def get_persistence_target_name(self) -> str:
        return "disk"

    def get_persistence_format_name(self) -> str:
        return "json_file"

    def data_type__internal_model(
        self, value: Value, target: str, base_name: str
    ) -> LoadConfig:
        """Persist internally used model data as a json file."""

        os.makedirs(target, exist_ok=True)
        target_file = os.path.join(target, f"{base_name}.json")

        try:
            value_metadata: KiaraModel = value.data
            data_json = value_metadata.json()
            python_class_json = PythonClass.from_class(value_metadata.__class__).json()
            all_json = (
                '{"data": ' + data_json + ', "python_class": ' + python_class_json + "}"
            )
        except Exception as e:
            raise KiaraProcessingException(
                f"Can't serialize value of type '{value.value_schema.type}' to json: {e}."
            )

        with open(target_file, "wt") as f:
            f.write(all_json)

        file_model = FileModel.load_file(source=target_file)
        load_config_data = {
            "inputs": {
                "json_file": file_model,
            },
            "module_type": "internal_model.load_from.json_file",
            "output_name": "internal_model",
        }

        load_config = LoadConfig(**load_config_data)
        return load_config


class LoadInternalModelFromDiskModule(KiaraModule):
    """Load a json file from disk and create a kiara value from it."""

    _module_type_name = "internal_model.load_from.json_file"

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        return {"json_file": {"type": "file", "doc": "The json file."}}

    def create_outputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        return {
            "internal_model": {
                "type": "internal_model",
                "doc": f"The deserialized internal_model value, loaded from disk.",
            }
        }

    def _retrieve_module_characteristics(self) -> ModuleCharacteristics:
        return ModuleCharacteristics(is_internal=True)

    def process(self, inputs: ValueSet, outputs: ValueSet):

        file_model: FileModel = inputs.get_value_data("json_file")
        path = file_model.path
        if not os.path.isfile(path):
            raise KiaraProcessingException(f"No json file found on path: {path}")

        with open(path, "rb") as f:
            content = f.read()
        data = orjson.loads(content)
        python_class_data = data["python_class"]
        model_cls = PythonClass(**python_class_data).get_class()

        model = model_cls(**data["data"])

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
