# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import abc
from typing import Iterable, Union, Mapping, Any, Dict, Set, Type

from pydantic import BaseModel, Field, PrivateAttr

from kiara.models.python_class import PythonClass
from kiara.models.values.value_metadata import ValueMetadata
from kiara.modules import KiaraModule
from kiara.exceptions import KiaraProcessingException
from kiara.models.module import ModuleTypeConfigSchema
from kiara.models.values.value import ValueSet
from kiara.models.values.value_schema import ValueSchema


class MetadataModuleConfig(ModuleTypeConfigSchema):

    metadata_key: str = Field(description="The key for the metadata association.")
    value_type: str = Field(description="The data type this module will be used for.")
    metadata_model: PythonClass = Field(description="The metadata model class.")


class ExtractMetadataModule(KiaraModule):
    """Base class to use when writing a module to extract metadata from a file.

    It's possible to use any arbitrary *kiara* module for this purpose, but sub-classing this makes it easier.
    """

    _config_cls = MetadataModuleConfig
    _module_type_name: str = "value.extract_metadata"

    def create_inputs_schema(
        self,
    ) -> Mapping[
        str, Union[ValueSchema, Mapping[str, Any]]
    ]:

        value_type = self.get_config_value("value_type")
        inputs = {
            "value": {
                "type": value_type,
                "doc": f"A value of type '{value_type}'",
                "optional": False,
            }
        }
        return inputs

    def create_outputs_schema(
        self,
    ) -> Mapping[
        str, Union[ValueSchema, Mapping[str, Any]]
    ]:
        outputs = {
            "value_metadata": {
                "type": "value_metadata",
                "type_config": {
                    "metadata_model": self.get_config_value("metadata_key")
                },
                "doc": "The metadata for the provided value.",
            }
        }

        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        metadata_key = self.get_config_value("metadata_key")
        value_type = self.get_config_value("value_type")
        metadata_model: PythonClass = self.get_config_value("metadata_model")

        value = inputs.get_value_obj("value")

        metadata_model_cls: Type[ValueMetadata] = metadata_model.get_class()
        metadata = metadata_model_cls.create_value_metadata(value=value)

        if not isinstance(metadata, metadata_model_cls):
            raise KiaraProcessingException(f"Invalid metadata model result, should be class '{metadata_model_cls.__name__}', but is: {metadata.__class__.__name__}. This is most likely a bug.")

        outputs.set_value("value_metadata", metadata)

