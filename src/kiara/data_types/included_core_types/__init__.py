# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import orjson
from pydantic import BaseModel
from rich import box
from rich.console import RenderableType
from rich.syntax import Syntax
from rich.table import Table
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    Iterable,
    Mapping,
    Type,
    TypeVar,
    Union,
)

from kiara.data_types import TYPE_CONFIG_CLS, TYPE_PYTHON_CLS, DataType, DataTypeConfig
from kiara.defaults import INVALID_HASH_MARKER, SpecialValue
from kiara.exceptions import DataTypeUnknownException, KiaraProcessingException
from kiara.models import KiaraModel
from kiara.models.data_types import DictModel
from kiara.models.python_class import PythonClass
from kiara.models.rendering import RenderScene, RenderValueResult
from kiara.models.values import DataTypeCharacteristics
from kiara.utils.json import orjson_dumps

if TYPE_CHECKING:
    from kiara.models.module.manifest import Manifest
    from kiara.models.values.value import SerializedData, Value


SCALAR_CHARACTERISTICS = DataTypeCharacteristics(
    is_scalar=True, is_json_serializable=True
)


class NoneType(DataType[SpecialValue, DataTypeConfig]):
    """Type indicating a 'None' value"""

    _data_type_name = "none"

    @classmethod
    def python_class(cls) -> Type:
        return SpecialValue

    # def is_immutable(self) -> bool:
    #     return False

    def calculate_hash(self, data: Any) -> str:
        return INVALID_HASH_MARKER

    def calculate_size(self, data: Any) -> int:
        return 0

    def parse_python_obj(self, data: Any) -> SpecialValue:
        return SpecialValue.NO_VALUE

    def pretty_print_as__string(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        return "None"

    def pretty_print_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ):

        return "None"


class AnyType(
    DataType[TYPE_PYTHON_CLS, DataTypeConfig], Generic[TYPE_PYTHON_CLS, TYPE_CONFIG_CLS]
):
    """'Any' type, the parent type for most other types.

    This type acts as the parents for all (or at least most) non-internal value types. There are some generic operations
    (like 'persist_value', or 'pretty_print') which are implemented for this type, so it's descendents have a fallback
    option in case no subtype-specific operations are implemented for it. In general, it is not recommended to use the 'any'
    type as module input or output, but it is possible. Values of type 'any' are not allowed to be persisted (at the moment,
    this might or might not change).
    """

    _data_type_name = "any"

    @classmethod
    def python_class(cls) -> Type:
        return object

    def pretty_print_as__string(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        if hasattr(self, "_pretty_print_as__string"):
            return self._pretty_print_as__string(value=value, render_config=render_config)  # type: ignore

        try:
            return str(value.data)
        except DataTypeUnknownException as dtue:
            return str(dtue)

    def pretty_print_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ):

        if hasattr(self, "_pretty_print_as__terminal_renderable"):
            return self._pretty_print_as__terminal_renderable(value=value, render_config=render_config)  # type: ignore

        try:
            data = value.data
        except DataTypeUnknownException as dtue:
            rendered: RenderableType = dtue.create_renderable(**render_config)
            from rich.panel import Panel

            return Panel(
                rendered,
                title=f"Unsupported data type: {dtue.data_type}",
                title_align="left",
            )
        except Exception as e:
            raise KiaraProcessingException(
                f"Error getting data for value '{value.value_id}': {e}"
            )

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
        self, value: "Value", render_config: Mapping[str, Any], manifest: "Manifest"
    ):
        if hasattr(self, "_render_as__string"):
            return self._render_as__string(value=value, render_scene=render_config, manifest=manifest)  # type: ignore
        else:
            return self.pretty_print_as__string(value=value, render_config={})

    def render_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any], manifest: "Manifest"
    ) -> RenderableType:

        if not hasattr(self, "_render_as__terminal_renderable"):

            try:
                value.data  # noqa
                return self.render_as__string(
                    value=value, render_config=render_config, manifest=manifest
                )
            except DataTypeUnknownException:
                return self.pretty_print_as__terminal_renderable(
                    value=value, render_config=render_config
                )

        else:
            return self._render_as__terminal_renderable(value=value, render_config=render_config, manifest=manifest)  # type: ignore


class BytesType(AnyType[bytes, DataTypeConfig]):
    """An array of bytes."""

    _data_type_name = "bytes"

    @classmethod
    def python_class(cls) -> Type:
        return bytes

    def serialize(self, data: bytes) -> "SerializedData":

        _data = {"bytes": {"type": "chunk", "chunk": data, "codec": "raw"}}

        serialized_data = {
            "data_type": self.data_type_name,
            "data_type_config": self.type_config.dict(),
            "data": _data,
            "serialization_profile": "raw",
            "metadata": {
                "environment": {},
                "deserialize": {
                    "python_object": {
                        "module_name": "load.bytes",
                        "module_config": {
                            "value_type": "bytes",
                            "target_profile": "python_object",
                            "serialization_profile": "raw",
                        },
                    }
                },
            },
        }
        from kiara.models.values.value import SerializationResult

        serialized = SerializationResult(**serialized_data)
        return serialized

    def _pretty_print_as__string(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        data: bytes = value.data
        return data.decode()


class StringType(AnyType[str, DataTypeConfig]):
    """A string."""

    _data_type_name = "string"

    @classmethod
    def python_class(cls) -> Type:
        return str

    def serialize(self, data: str) -> "SerializedData":

        _data = {
            "string": {"type": "chunk", "chunk": data.encode("utf-8"), "codec": "raw"}
        }

        serialized_data = {
            "data_type": self.data_type_name,
            "data_type_config": self.type_config.dict(),
            "data": _data,
            "serialization_profile": "raw",
            "metadata": {
                "environment": {},
                "deserialize": {
                    "python_object": {
                        "module_type": "load.string",
                        "module_config": {
                            "value_type": "string",
                            "target_profile": "python_object",
                            "serialization_profile": "raw",
                        },
                    }
                },
            },
        }
        from kiara.models.values.value import SerializationResult

        serialized = SerializationResult(**serialized_data)
        return serialized

    def _retrieve_characteristics(self) -> DataTypeCharacteristics:
        return SCALAR_CHARACTERISTICS

    def _validate(cls, value: Any) -> None:

        if not isinstance(value, str):
            raise ValueError(f"Invalid type '{type(value)}': string required")

    def pretty_print_as__bytes(self, value: "Value", render_config: Mapping[str, Any]):
        value_str: str = value.data
        return value_str.encode()


class BooleanType(AnyType[bool, DataTypeConfig]):
    "A boolean."

    _data_type_name = "boolean"

    @classmethod
    def python_class(cls) -> Type:
        return bool

    def serialize(self, data: bool) -> "SerializedData":
        result = self.serialize_as_json(data)
        return result

    def _retrieve_characteristics(self) -> DataTypeCharacteristics:
        return SCALAR_CHARACTERISTICS

    # def calculate_size(self, data: bool) -> int:
    #     return 24
    #
    # def calculate_hash(cls, data: bool) -> int:
    #     return 1 if data else 0

    def parse_python_obj(self, data: Any) -> bool:

        if data is True or data is False:
            return data
        elif data == 0:
            return False
        elif data == 1:
            return True
        elif isinstance(data, str):
            if data.lower() == "true":
                return True
            elif data.lower() == "false":
                return False
        raise Exception(f"Can't parse value '{data}' as boolean.")

    def validate(cls, value: Any):
        pass


class DictValueType(AnyType[DictModel, DataTypeConfig]):
    """A dictionary.

    In addition to the actual dictionary value, this value type comes also with an optional schema, describing the
    dictionary. In case no schema was attached, a simple generic one is attached. This data type is backed by the
    [DictModel][kiara_plugin.core_types.models.DictModel] class.
    """

    _data_type_name = "dict"

    @classmethod
    def python_class(cls) -> Type:
        return DictModel

    # def calculate_size(self, data: DictModel) -> int:
    #     return data.size
    #
    # def calculate_hash(self, data: DictModel) -> int:
    #     return data.value_hash

    def _retrieve_characteristics(self) -> DataTypeCharacteristics:
        return DataTypeCharacteristics(is_scalar=False, is_json_serializable=True)

    def parse_python_obj(self, data: Any) -> DictModel:

        python_cls = data.__class__
        dict_data = None
        schema = None

        if isinstance(data, Mapping):

            if (
                len(data) == 3
                and "dict_data" in data.keys()
                and "data_schema" in data.keys()
                and "python_class" in data.keys()
            ):
                dict_model = DictModel(
                    dict_data=data["dict_data"],
                    data_schema=data["data_schema"],
                    python_class=data["python_class"],
                )
                return dict_model

            schema = {"title": "dict", "type": "object"}
            dict_data = data
        elif isinstance(data, BaseModel):
            dict_data = data.dict()
            schema = data.schema()
        elif isinstance(data, str):
            try:
                dict_data = orjson.loads(data)
                schema = {"title": "dict", "type": "object"}
            except Exception:
                pass

        if dict_data is None or schema is None:
            raise Exception(f"Invalid data for value type 'dict': {data}")

        result = {
            "dict_data": dict_data,
            "data_schema": schema,
            "python_class": PythonClass.from_class(python_cls).dict(),
        }
        return DictModel(**result)

    def _validate(self, data: DictModel) -> None:

        if not isinstance(data, DictModel):
            raise Exception(f"Invalid type: {type(data)}.")

    # def render_as__string(self, value: Value, render_config: Mapping[str, Any]) -> str:
    #
    #     data: DictModel = value.data
    #     return orjson_dumps(data.dict_data, option=orjson.OPT_INDENT_2)

    def _pretty_print_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ):

        show_schema = render_config.get("show_schema", True)

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("key", style="i")
        table.add_column("value")

        data: DictModel = value.data
        data_json = orjson_dumps(data.dict_data, option=orjson.OPT_INDENT_2)
        table.add_row(
            "dict data", Syntax(data_json, "json", background_color="default")
        )

        if show_schema:
            schema_json = orjson_dumps(data.data_schema, option=orjson.OPT_INDENT_2)
            table.add_row(
                "dict schema", Syntax(schema_json, "json", background_color="default")
            )

        return table

    def serialize(self, data: DictModel) -> "SerializedData":

        result = self.serialize_as_json(data.dict())
        return result

    def render_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any], manifest: "Manifest"
    ) -> RenderableType:

        render_item = render_config.get("render_item", "data")
        width = render_config.get("display_width", 0)

        related_scenes: Dict[str, Union[None, RenderScene]] = {}
        if render_item == "data":
            dict_data = value.data.dict_data
            json_string = orjson_dumps(dict_data, option=orjson.OPT_INDENT_2)

            if width > 0:
                new_lines = []
                for line in json_string.split("\n"):
                    if len(line) > width:
                        new_lines.append(line[0 : width - 3] + "...")  # noqa
                    else:
                        new_lines.append(line)

            json_string = "\n".join(new_lines)

            rendered = Syntax(json_string, "json")
            related_scenes["data"] = None
            related_scenes["schema"] = RenderScene.construct(
                title="schema",
                description="The (json) schema for the data.",
                manifest_hash=manifest.manifest_hash,
                render_config={"render_item": "schema"},
            )

        elif render_item == "schema":
            schema = value.data.data_schema
            json_string = orjson_dumps(schema, option=orjson.OPT_INDENT_2)

            rendered = Syntax(json_string, "json")
            related_scenes["data"] = RenderScene.construct(
                title="data",
                description="The actual data of the dictionary.",
                manifest_hash=manifest.manifest_hash,
                render_config={"render_item": "data"},
            )
            related_scenes["schema"] = None

        else:
            raise KiaraProcessingException(
                f"Invalid render item '{render_item}', allowed: 'data', 'schema'."
            )

        result = RenderValueResult(
            value_id=value.value_id,
            render_config=render_config,
            render_manifest=manifest.manifest_hash,
            related_scenes=related_scenes,
            manifest_lookup={manifest.manifest_hash: manifest},
            rendered=rendered,
        )

        return result


KIARA_MODEL_CLS = TypeVar("KIARA_MODEL_CLS", bound=KiaraModel)


class KiaraModelValueType(
    AnyType[KIARA_MODEL_CLS, TYPE_CONFIG_CLS], Generic[KIARA_MODEL_CLS, TYPE_CONFIG_CLS]
):
    """A value type that is used internally.

    This type should not be used by user-facing modules and/or operations.
    """

    _data_type_name = None  # type: ignore

    @classmethod
    def data_type_config_class(cls) -> Type[DataTypeConfig]:
        return DataTypeConfig

    @abc.abstractmethod
    def create_model_from_python_obj(self, data: Any) -> KIARA_MODEL_CLS:
        pass

    def parse_python_obj(self, data: Any) -> KIARA_MODEL_CLS:

        if isinstance(data, self.__class__.python_class()):
            return data  # type: ignore

        data = self.create_model_from_python_obj(data)
        return data

    def _validate(self, data: KiaraModel) -> None:

        if not isinstance(data, self.__class__.python_class()):
            raise Exception(
                f"Invalid type '{type(data)}', must be: {self.__class__.python_class().__name__}, or subclass."
            )
