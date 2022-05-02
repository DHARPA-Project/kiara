# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from pydantic import Field
from typing import Any, Mapping, Type, Union

from kiara.exceptions import KiaraProcessingException
from kiara.models.module import KiaraModuleConfig
from kiara.models.python_class import PythonClass
from kiara.models.values.value import ValueMap
from kiara.models.values.value_metadata import ValueMetadata
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import KiaraModule


class MetadataModuleConfig(KiaraModuleConfig):

    # metadata_key: str = Field(description="The key for the metadata association.")
    data_type: str = Field(description="The data type this module will be used for.")
    metadata_model: PythonClass = Field(description="The metadata model class.")


class ExtractMetadataModule(KiaraModule):
    """Base class to use when writing a module to extract metadata from a file.

    It's possible to use any arbitrary *kiara* module for this purpose, but sub-classing this makes it easier.
    """

    _config_cls = MetadataModuleConfig
    _module_type_name: str = "value.extract_metadata"

    def create_inputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        data_type_name = self.get_config_value("data_type")
        inputs = {
            "value": {
                "type": data_type_name,
                "doc": f"A value of type '{data_type_name}'",
                "optional": False,
            }
        }
        return inputs

    def create_outputs_schema(
        self,
    ) -> Mapping[str, Union[ValueSchema, Mapping[str, Any]]]:

        result_model_cls: PythonClass = self.get_config_value("metadata_model")

        outputs = {
            "value_metadata": {
                "type": "internal_model",
                "type_config": {"model_cls": result_model_cls.full_name},
                "doc": "The metadata for the provided value.",
            }
        }

        return outputs

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:

        # metadata_key = self.get_config_value("metadata_key")
        # data_type = self.get_config_value("data_type")
        metadata_model: PythonClass = self.get_config_value("metadata_model")

        value = inputs.get_value_obj("value")

        metadata_model_cls: Type[ValueMetadata] = metadata_model.get_class()  # type: ignore
        metadata = metadata_model_cls.create_value_metadata(value=value)

        if not isinstance(metadata, metadata_model_cls):
            raise KiaraProcessingException(
                f"Invalid metadata model result, should be class '{metadata_model_cls.__name__}', but is: {metadata.__class__.__name__}. This is most likely a bug."
            )

        # if isinstance(metadata, Mapping):
        #     md = metadata_model_cls(**metadata)
        # elif isinstance(metadata, metadata_model_cls):
        #     md = metadata
        # else:
        #     raise KiaraProcessingException(
        #         f"Invalid type '{type(metadata)}' for result metadata, must be a mapping or subclass of '{metadata_model_cls.__name__}'."
        #     )

        outputs.set_value("value_metadata", metadata)
