# -*- coding: utf-8 -*-
import orjson
import sys
from rich.syntax import Syntax
from typing import TYPE_CHECKING, Any, Mapping, Type

from kiara.data_types import TYPE_CONFIG_CLS, TYPE_PYTHON_CLS, DataType, DataTypeConfig
from kiara.models import KiaraModel
from kiara.models.documentation import DocumentationMetadataModel
from kiara.utils.hashing import compute_hash

if TYPE_CHECKING:
    from kiara.models.values.value import Value


class TerminalRenderable(DataType[object, DataTypeConfig]):
    """A list of renderable objects, used in the 'rich' Python library, to print to the terminal or in Jupyter.

    Internally, the result list items can be either a string, a 'rich.console.ConsoleRenderable', or a 'rich.console.RichCast'.
    """

    _value_type_name = "terminal_renderable"

    @classmethod
    def python_class(cls) -> Type:
        return object

    def calculate_hash(self, data: TYPE_PYTHON_CLS) -> int:
        return compute_hash(data)

    def calculate_size(self, data: TYPE_PYTHON_CLS) -> int:
        return sys.getsizeof(data)


class InternalModelValueType(DataType[KiaraModel, DataTypeConfig]):
    """A value type that is used internally.

    This type should not be used by user-facing modules and/or operations.
    """

    _data_type_name = "internal_model"

    @classmethod
    def python_class(cls) -> Type:
        return KiaraModel

    @classmethod
    def data_type_config_class(cls) -> Type[TYPE_CONFIG_CLS]:
        return DataTypeConfig

    def calculate_size(self, data: KiaraModel) -> int:
        return data.model_size

    def calculate_hash(self, data: KiaraModel) -> int:
        return data.model_data_hash

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

    def parse_python_obj(self, data: Any) -> TYPE_PYTHON_CLS:

        return DocumentationMetadataModel.create(data)

    @classmethod
    def python_class(cls) -> Type:
        return DocumentationMetadataModel

    def render_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ):
        json_str = value.data.json(option=orjson.OPT_INDENT_2)
        return Syntax(json_str, "json", background_color="default")
