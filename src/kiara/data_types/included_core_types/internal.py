# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import orjson
from rich.syntax import Syntax
from typing import TYPE_CHECKING, Any, Generic, Mapping, Type

from kiara.data_types import TYPE_CONFIG_CLS, TYPE_PYTHON_CLS, DataType, DataTypeConfig
from kiara.models import KiaraModel
from kiara.models.documentation import DocumentationMetadataModel

if TYPE_CHECKING:
    from kiara.models.values.value import Value


class InternalType(
    DataType[TYPE_PYTHON_CLS, TYPE_CONFIG_CLS],
    Generic[TYPE_PYTHON_CLS, TYPE_CONFIG_CLS],
):
    """'A 'marker' base data type for data types that are (mainly) used internally in kiara.."""

    _data_type_name = "internal"

    @classmethod
    def python_class(cls) -> Type:
        return object

    def reender_as__string(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        data = value.data
        return str(data)


class TerminalRenderable(InternalType[object, DataTypeConfig]):
    """A list of renderable objects, used in the 'rich' Python library, to print to the terminal or in Jupyter.

    Internally, the result list items can be either a string, a 'rich.console.ConsoleRenderable', or a 'rich.console.RichCast'.
    """

    _data_type_name = "terminal_renderable"

    @classmethod
    def python_class(cls) -> Type:
        return object


class InternalModelValueType(InternalType[KiaraModel, DataTypeConfig]):
    """A value type that is used internally.

    This type should not be used by user-facing modules and/or operations.
    """

    _data_type_name = "internal_model"

    @classmethod
    def python_class(cls) -> Type:
        return KiaraModel

    @classmethod
    def data_type_config_class(cls) -> Type[DataTypeConfig]:
        return DataTypeConfig

    def _validate(self, value: KiaraModel) -> None:

        if not isinstance(value, KiaraModel):
            raise Exception(f"Invalid type: {type(value)}.")

    def render_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ):
        json_str = value.data.json(option=orjson.OPT_INDENT_2)
        return Syntax(json_str, "json", background_color="default")


class DocumentationModelValueType(InternalModelValueType):
    """Documentation for an internal entity."""

    _data_type_name = "doc"

    def parse_python_obj(self, data: Any) -> DocumentationMetadataModel:

        return DocumentationMetadataModel.create(data)

    @classmethod
    def python_class(cls) -> Type:
        return DocumentationMetadataModel

    def render_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ):
        json_str = value.data.json(option=orjson.OPT_INDENT_2)
        return Syntax(json_str, "json", background_color="default")
