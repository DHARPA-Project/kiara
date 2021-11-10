# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import typing
from pydantic import Field

from kiara import Kiara, KiaraModule
from kiara.data import ValueSet
from kiara.data.values import Value, ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import ModuleTypeConfigSchema
from kiara.operations import Operation, OperationType
from kiara.utils import log_message


class SampleValueModuleConfig(ModuleTypeConfigSchema):

    sample_type: str = Field(description="The sample method.")


class SampleValueModule(KiaraModule):
    """Base class for operations that take samples of data."""

    _config_cls = SampleValueModuleConfig

    @classmethod
    @abc.abstractmethod
    def get_value_type(cls) -> str:
        """Return the value type for this sample module."""

    @classmethod
    def retrieve_module_profiles(
        cls, kiara: Kiara
    ) -> typing.Mapping[str, typing.Union[typing.Mapping[str, typing.Any], Operation]]:

        all_metadata_profiles: typing.Dict[
            str, typing.Dict[str, typing.Dict[str, typing.Any]]
        ] = {}

        value_type: str = cls.get_value_type()

        if value_type not in kiara.type_mgmt.value_type_names:
            log_message(
                f"Ignoring sample operation for source type '{value_type}': type not available"
            )
            return {}

        for sample_type in cls.get_supported_sample_types():

            op_config = {
                "module_type": cls._module_type_id,  # type: ignore
                "module_config": {"sample_type": sample_type},
                "doc": f"Sample value of type '{value_type}' using method: {sample_type}.",
            }
            key = f"sample.{value_type}.{sample_type}"
            if key in all_metadata_profiles.keys():
                raise Exception(f"Duplicate profile key: {key}")
            all_metadata_profiles[key] = op_config

        return all_metadata_profiles

    @classmethod
    def get_supported_sample_types(cls) -> typing.Iterable[str]:

        types = []
        for attr_name, attr in cls.__dict__.items():

            if attr_name.startswith("sample_") and callable(attr):
                sample_type = attr_name[7:]
                if sample_type in types:
                    raise Exception(
                        f"Error in sample module '{cls.__name__}': multiple sample methods for type '{sample_type}'."
                    )
                types.append(sample_type)

        return types

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        input_name = self.get_value_type()
        if input_name == "any":
            input_name = "value_item"

        return {
            input_name: {
                "type": self.get_value_type(),
                "doc": "The value to sample.",
            },
            "sample_size": {
                "type": "integer",
                "doc": "The sample size.",
                "default": 10,
            },
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {
            "sampled_value": {"type": self.get_value_type(), "doc": "The sampled value"}
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        sample_size: int = inputs.get_value_data("sample_size")
        sample_type: str = self.get_config_value("sample_type")

        if sample_size < 0:
            raise KiaraProcessingException(
                f"Invalid sample size '{sample_size}': can't be negative."
            )

        input_name = self.get_value_type()
        if input_name == "any":
            input_name = "value_item"
        value: Value = inputs.get_value_obj(input_name)

        func = getattr(self, f"sample_{sample_type}")
        result = func(value=value, sample_size=sample_size)

        outputs.set_value("sampled_value", result)


class SampleValueOperationType(OperationType):
    """Operation type for sampling data of different types.

    This is useful to reduce the size of some datasets in test-runs, while adjusting parameters and the like. Operations of this
    type can implement very simple or complex ways to take samples of the data they are fed. The most important ones are
    sampling operations relating to tables and arrays, but it might also make sense to sample texts, image-sets and so on.

    Modules that implement sampling should inherit from [SampleValueModule](https://dharpa.org/kiara/latest/api_reference/kiara.operations.sample/#kiara.operations.sample.SampleValueModule), and will
    get auto-registered with operation ids following this template: `<VALUE_TYPE>.sample.<SAMPLE_TYPE_NAME>`, where `SAMPLE_TYPE_NAME` is a descriptive name
    what will be sampled, or how sampling will be done.
    """

    def is_matching_operation(self, op_config: Operation) -> bool:

        return issubclass(op_config.module_cls, SampleValueModule)

    def get_operations_for_value_type(
        self, value_type: str
    ) -> typing.Dict[str, Operation]:
        """Find all operations that serialize the specified type.

        The result dict uses the serialization type as key, and the operation itself as value.
        """

        result: typing.Dict[str, Operation] = {}
        for o_id, op in self.operations.items():
            sample_op_module_cls: typing.Type[SampleValueModule] = op.module_cls  # type: ignore
            source_type = sample_op_module_cls.get_value_type()
            if source_type == value_type:
                target_type = op.module_config["sample_type"]
                if target_type in result.keys():
                    raise Exception(
                        f"Multiple operations to sample '{source_type}' using {target_type}"
                    )
                result[target_type] = op

        return result
