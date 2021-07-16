# -*- coding: utf-8 -*-
import typing
from pydantic import Field

from kiara import Kiara
from kiara.data.operations import OperationType


class TypeConversionOperationType(OperationType):
    @classmethod
    def retrieve_operation_configs(
        cls, kiara: "Kiara"
    ) -> typing.Mapping[str, typing.Mapping[str, typing.Mapping[str, typing.Any]]]:
        return {}

    convert_source_type: str = Field(description="The source type.")
    convert_target_type: str = Field(description="The target type.")
