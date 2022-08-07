# -*- coding: utf-8 -*-
import orjson.orjson
from pydantic import Field
from rich.syntax import Syntax
from typing import TYPE_CHECKING, Any, Mapping, Type, Union

from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types.internal import InternalType
from kiara.models.render_value import RenderMetadata, RenderScene
from kiara.utils.class_loading import find_all_kiara_model_classes

if TYPE_CHECKING:
    from kiara.models.values.value import Value


class RenderInstructionTypeConfig(DataTypeConfig):

    kiara_model_id: str = Field(
        description="The id of the model backing this render (Python class must sub-class 'RenderInstruction').",
        # default="instance.render_instruction.table",
    )


class RenderInstructionDataType(InternalType[RenderScene, RenderInstructionTypeConfig]):
    """A value type to contain information about how to render a value in a specific render scenario."""

    _data_type_name = "render_instruction"

    def __init__(self, **type_config: Any):

        self._cls_cache: Union[Type[RenderScene], None] = None
        super().__init__(**type_config)

    @classmethod
    def python_class(cls) -> Type:
        return RenderScene

    @classmethod
    def data_type_config_class(cls) -> Type[RenderInstructionTypeConfig]:
        return RenderInstructionTypeConfig

    @property
    def model_cls(self) -> Type[RenderScene]:

        if self._cls_cache is not None:
            return self._cls_cache

        all_models = find_all_kiara_model_classes()
        if self.type_config.kiara_model_id not in all_models.keys():
            raise Exception(f"Invalid model id: {self.type_config.kiara_model_id}")

        model_cls = all_models[self.type_config.kiara_model_id]

        assert issubclass(model_cls, RenderScene)
        self._cls_cache = model_cls
        return self._cls_cache

    def parse_python_obj(self, data: Any) -> RenderScene:

        if data is None:
            return self.model_cls()
        elif isinstance(data, RenderScene):
            return data
        elif isinstance(data, Mapping):
            return self.model_cls(**data)
        else:
            raise ValueError(
                f"Can't parse data, invalid type '{type(data)}': must be subclass of 'KiaraModel' or Mapping."
            )

    def _validate(self, value: RenderScene) -> None:

        if not isinstance(value, RenderScene):
            raise Exception(f"Invalid type: {type(value)}.")

    def pretty_print_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        data: RenderScene = value.data

        ri_json = data.json(option=orjson.orjson.OPT_INDENT_2)
        return Syntax(ri_json, "json", background_color="default")


class RenderMetadataDataType(InternalType[RenderMetadata, DataTypeConfig]):
    """A value type to contain information about how to render a value in a specific render scenario."""

    _data_type_name = "render_metadata"

    def __init__(self, **type_config: Any):

        self._cls_cache: Union[Type[RenderMetadata], None] = None
        super().__init__(**type_config)

    @classmethod
    def python_class(cls) -> Type:
        return RenderMetadata

    def parse_python_obj(self, data: Any) -> RenderMetadata:

        if data is None:
            return RenderMetadata()
        elif isinstance(data, RenderMetadata):
            return data
        elif isinstance(data, Mapping):
            return RenderMetadata(**data)
        else:
            raise ValueError(
                f"Can't parse data, invalid type '{type(data)}': must be subclass of 'KiaraModel' or Mapping."
            )

    def _validate(self, value: Any) -> None:

        if not isinstance(value, RenderMetadata):
            raise Exception(f"Invalid type: {type(value)}.")

    def pretty_print_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        data: RenderMetadata = value.data

        ri_json = data.json(option=orjson.orjson.OPT_INDENT_2)
        return Syntax(ri_json, "json", background_color="default")