# -*- coding: utf-8 -*-
from pydantic import Field
from typing import Any, Mapping, Optional, Type

from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types.internal import InternalType
from kiara.models.render_value import RenderInstruction, RenderMetadata
from kiara.utils.class_loading import find_all_kiara_model_classes


class RenderInstructionTypeConfig(DataTypeConfig):

    kiara_model_id: str = Field(
        description="The id of the model backing this render (Python class must sub-class 'RenderInstruction').",
        default="instance.render_instruction.table",
    )


class RenderInstructionDataType(
    InternalType[RenderInstruction, RenderInstructionTypeConfig]
):
    """A value type to contain information about how to render a value in a specific render scenario."""

    _data_type_name = "render_instruction"

    def __init__(self, **type_config: Any):

        self._cls_cache: Optional[Type[RenderInstruction]] = None
        super().__init__(**type_config)

    @classmethod
    def python_class(cls) -> Type:
        return RenderInstruction

    @classmethod
    def data_type_config_class(cls) -> Type[RenderInstructionTypeConfig]:
        return RenderInstructionTypeConfig

    @property
    def model_cls(self) -> Type[RenderInstruction]:

        if self._cls_cache is not None:
            return self._cls_cache

        all_models = find_all_kiara_model_classes()
        if self.type_config.kiara_model_id not in all_models.keys():
            raise Exception(f"Invalid model id: {self.type_config.kiara_model_id}")

        model_cls = all_models[self.type_config.kiara_model_id]

        assert issubclass(model_cls, RenderInstruction)
        self._cls_cache = model_cls
        return self._cls_cache

    def parse_python_obj(self, data: Any) -> RenderInstruction:

        if data is None:
            return self.model_cls()
        elif isinstance(data, RenderInstruction):
            return data
        elif isinstance(data, Mapping):
            return self.model_cls(**data)
        else:
            raise ValueError(
                f"Can't parse data, invalid type '{type(data)}': must be subclass of 'KiaraModel' or Mapping."
            )

    def _validate(self, value: RenderInstruction) -> None:

        if not isinstance(value, RenderInstruction):
            raise Exception(f"Invalid type: {type(value)}.")


class RenderMetadataDataType(InternalType[RenderMetadata, DataTypeConfig]):
    """A value type to contain information about how to render a value in a specific render scenario."""

    _data_type_name = "render_metadata"

    def __init__(self, **type_config: Any):

        self._cls_cache: Optional[Type[RenderMetadata]] = None
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
