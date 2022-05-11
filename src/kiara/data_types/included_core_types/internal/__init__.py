# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import orjson
import structlog
from pydantic import Field, PrivateAttr
from rich.syntax import Syntax
from typing import Any, Generic, Mapping, Optional, Type, Union

from kiara.data_types import TYPE_CONFIG_CLS, TYPE_PYTHON_CLS, DataType, DataTypeConfig
from kiara.defaults import NO_SERIALIZATION_MARKER
from kiara.models import KiaraModel
from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.values.value import SerializedData, Value
from kiara.registries.models import ModelRegistry

logger = structlog.getLogger()


class InternalType(
    DataType[TYPE_PYTHON_CLS, TYPE_CONFIG_CLS],
    Generic[TYPE_PYTHON_CLS, TYPE_CONFIG_CLS],
):
    """'A 'marker' base data type for data types that are (mainly) used internally in kiara.."""

    _data_type_name = "internal"

    @classmethod
    def python_class(cls) -> Type:
        return object

    def pretty_print_as__string(
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


class InternalModelTypeConfig(DataTypeConfig):

    kiara_model_id: Optional[str] = Field(
        description="The Python class backing this model (must sub-class 'KiaraModel')."
    )


class InternalModelValueType(InternalType[KiaraModel, InternalModelTypeConfig]):
    """A value type that is used internally.

    This type should not be used by user-facing modules and/or operations.
    """

    _data_type_name = "internal_model"
    _cls_cache: Optional[Type[KiaraModel]] = PrivateAttr(default=None)

    @classmethod
    def data_type_config_class(cls) -> Type[InternalModelTypeConfig]:
        return InternalModelTypeConfig  # type: ignore

    def serialize(self, data: KiaraModel) -> Union[str, SerializedData]:

        if self.type_config.kiara_model_id is None:
            logger.debug(
                "ignore.serialize_request",
                data_type="internal_model",
                cls=data.__class__.__name__,
                reason="no model id in module config",
            )
            return NO_SERIALIZATION_MARKER

        _data = {
            "data": {
                "type": "inline-json",
                "inline_data": data.dict(),
                "codec": "json",
            },
        }

        serialized_data = {
            "data_type": self.data_type_name,
            "data_type_config": self.type_config.dict(),
            "data": _data,
            "serialization_profile": "json",
            "metadata": {
                "environment": {},
                "deserialize": {
                    "python_object": {
                        "module_type": "load.internal_model",
                        "module_config": {
                            "value_type": "internal_model",
                            "target_profile": "python_object",
                            "serialization_profile": "json",
                        },
                    }
                },
            },
        }
        from kiara.models.values.value import SerializationResult

        serialized = SerializationResult(**serialized_data)
        return serialized

    @classmethod
    def python_class(cls) -> Type:
        return KiaraModel

    @property
    def model_cls(self) -> Type[KiaraModel]:

        if self._cls_cache is not None:
            return self._cls_cache

        model_type_id = self.type_config.kiara_model_id
        assert model_type_id is not None

        model_registry = ModelRegistry.instance()

        model_cls = model_registry.get_model_cls(
            model_type_id, required_subclass=KiaraModel
        )

        self._cls_cache = model_cls
        return self._cls_cache

    def parse_python_obj(self, data: Any) -> KiaraModel:

        if isinstance(data, KiaraModel):
            return data
        elif isinstance(data, Mapping):
            return self.model_cls(**data)
        else:
            raise ValueError(
                f"Can't parse data, invalid type '{type(data)}': must be subclass of 'KiaraModel' or Mapping."
            )

    def _validate(self, value: KiaraModel) -> None:

        if not isinstance(value, KiaraModel):
            raise Exception(f"Invalid type: {type(value)}.")

    def pretty_print_as__terminal_renderable(
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

    def pretty_print_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ):
        json_str = value.data.json(option=orjson.OPT_INDENT_2)
        return Syntax(json_str, "json", background_color="default")
