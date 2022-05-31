# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from pydantic import Field
from typing import Any, Mapping, Type, Union

from kiara.exceptions import KiaraProcessingException
from kiara.models.module import KiaraModuleConfig
from kiara.models.values.value import ValueMap
from kiara.models.values.value_metadata import ValueMetadata
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import KiaraModule, ModuleCharacteristics
from kiara.registries.models import ModelRegistry


class MetadataModuleConfig(KiaraModuleConfig):

    data_type: str = Field(description="The data type this module will be used for.")
    kiara_model_id: str = Field(description="The id of the kiara (metadata) model.")


class ExtractMetadataModule(KiaraModule):
    """Base class to use when writing a module to extract metadata from a file.

    It's possible to use any arbitrary *kiara* module for this purpose, but sub-classing this makes it easier.
    """

    _config_cls = MetadataModuleConfig
    _module_type_name: str = "value.extract_metadata"

    def _retrieve_module_characteristics(self) -> ModuleCharacteristics:

        return ModuleCharacteristics(
            is_idempotent=True, is_internal=True, unique_result_values=True
        )

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

        kiara_model_id: str = self.get_config_value("kiara_model_id")

        # TODO: check it's subclassing the right class

        outputs = {
            "value_metadata": {
                "type": "internal_model",
                "type_config": {"kiara_model_id": kiara_model_id},
                "doc": "The metadata for the provided value.",
            }
        }

        return outputs

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:

        value = inputs.get_value_obj("value")

        kiara_model_id: str = self.get_config_value("kiara_model_id")

        model_registry = ModelRegistry.instance()
        metadata_model_cls: Type[ValueMetadata] = model_registry.get_model_cls(kiara_model_id=kiara_model_id, required_subclass=ValueMetadata)  # type: ignore

        metadata = metadata_model_cls.create_value_metadata(value=value)

        if not isinstance(metadata, metadata_model_cls):
            raise KiaraProcessingException(
                f"Invalid metadata model result, should be class '{metadata_model_cls.__name__}', but is: {metadata.__class__.__name__}. This is most likely a bug."
            )

        outputs.set_value("value_metadata", metadata)
