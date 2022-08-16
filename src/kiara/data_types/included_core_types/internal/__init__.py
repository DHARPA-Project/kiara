# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import orjson
import structlog
from pydantic import Field, PrivateAttr
from rich import box
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from typing import TYPE_CHECKING, Any, Generic, Iterable, Mapping, Type, Union

from kiara.data_types import TYPE_CONFIG_CLS, TYPE_PYTHON_CLS, DataType, DataTypeConfig
from kiara.defaults import NO_SERIALIZATION_MARKER
from kiara.models import KiaraModel
from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.python_class import PythonClass
from kiara.models.values.value import SerializedData, Value
from kiara.registries.models import ModelRegistry

if TYPE_CHECKING:
    from kiara.models.module.manifest import Manifest
    from kiara.models.rendering import RenderScene

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

        if hasattr(self, "_pretty_print_as__string"):
            return self._pretty_print_as_string(value=value, render_config=render_config)  # type: ignore

        return str(value.data)

    def pretty_print_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ):

        if hasattr(self, "_pretty_print_as__terminal_renderable"):
            return self._pretty_print_as__terminal_renderable(value=value, render_config=render_config)  # type: ignore

        data = value.data

        from pydantic import BaseModel

        if isinstance(data, BaseModel):
            from kiara.utils.output import create_table_from_model_object

            rendered = create_table_from_model_object(
                model=data, render_config=render_config
            )
        elif isinstance(data, Iterable):
            import pprint

            rendered = pprint.pformat(data)
        else:
            rendered = str(data)
        return rendered

    def render_as__string(
        self, value: "Value", render_config: "RenderScene", manifest: "Manifest"
    ):

        if hasattr(self, "_render_as__string"):
            return self._render_as__string(value=value, render_config=render_config, manifest=manifest)  # type: ignore
        else:
            return self.pretty_print_as__string(value=value, render_config={})

    def render_as__terminal_renderable(
        self, value: "Value", render_config: "RenderScene", manifest: "Manifest"
    ):

        if hasattr(self, "_render_as__terminal_renderable"):
            return self._render_as__terminal(value=value, render_config=render_config, manifest=manifest)  # type: ignore
        return self.render_as__string(
            value=value, render_config=render_config, manifest=manifest
        )


class TerminalRenderable(InternalType[object, DataTypeConfig]):
    """A list of renderable objects, used in the 'rich' Python library, to print to the terminal or in Jupyter.

    Internally, the result list items can be either a string, a 'rich.console.ConsoleRenderable', or a 'rich.console.RichCast'.
    """

    _data_type_name = "terminal_renderable"

    @classmethod
    def python_class(cls) -> Type:
        return object

    def _pretty_print_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        renderable = value.data

        table = Table(show_header=False, show_lines=False, box=box.SIMPLE)
        table.add_column("key", style="i")
        table.add_column("value")
        cls = PythonClass.from_class(renderable.__class__)
        table.add_row("python class", cls)
        table.add_row("preview", Panel(renderable, height=20))

        return table


class InternalModelTypeConfig(DataTypeConfig):

    kiara_model_id: Union[str, None] = Field(
        description="The Python class backing this model (must sub-class 'KiaraModel')."
    )


class InternalModelValueType(InternalType[KiaraModel, InternalModelTypeConfig]):
    """A value type that is used internally.

    This type should not be used by user-facing modules and/or operations.
    """

    _data_type_name = "internal_model"
    _cls_cache: Union[Type[KiaraModel], None] = PrivateAttr(default=None)

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

    def _pretty_print_as__terminal_renderable(
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

    def _pretty_print_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ):
        json_str = value.data.json(option=orjson.OPT_INDENT_2)
        return Syntax(json_str, "json", background_color="default")
