# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Any, ClassVar, Mapping, Type, Union

from pydantic import Field
from rich import box
from rich.syntax import Syntax
from rich.table import Table

from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types.internal import InternalType
from kiara.models.rendering import RenderScene, RenderValueResult
from kiara.utils.class_loading import find_all_kiara_model_classes
from kiara.utils.output import extract_renderable

if TYPE_CHECKING:
    from kiara.models.values.value import Value


class RenderSceneTypeConfig(DataTypeConfig):

    kiara_model_id: Union[str, None] = Field(
        description="The id of the model backing this render (Python class must sub-class 'RenderScene').",
        default=None,
    )


class RenderSceneDataType(InternalType[RenderScene, RenderSceneTypeConfig]):
    """A value type to contain information about how to render a value in a specific render scenario."""

    _data_type_name: ClassVar[str] = "render_scene"

    def __init__(self, **type_config: Any):

        self._cls_cache: Union[Type[RenderScene], None] = None
        super().__init__(**type_config)

    @classmethod
    def python_class(cls) -> Type:
        return RenderScene

    @classmethod
    def data_type_config_class(cls) -> Type[RenderSceneTypeConfig]:
        return RenderSceneTypeConfig

    @property
    def model_cls(self) -> Type[RenderScene]:

        if self._cls_cache is not None:
            return self._cls_cache

        kiara_model_id = self.type_config.kiara_model_id
        if not kiara_model_id:
            kiara_model_id = RenderScene._kiara_model_id

        if kiara_model_id == RenderScene._kiara_model_id:
            model_cls = RenderScene
        else:
            all_models = find_all_kiara_model_classes()
            if kiara_model_id not in all_models.keys():
                raise Exception(f"Invalid model id: {kiara_model_id}")
            # TODO: check type is right?
            model_cls = all_models[kiara_model_id]  # type: ignore

        assert issubclass(model_cls, RenderScene)
        self._cls_cache = model_cls
        return self._cls_cache

    def parse_python_obj(self, data: Any) -> RenderScene:

        if isinstance(data, RenderScene):
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

    def _pretty_print_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        data: RenderScene = value.data

        ri_json = data.model_dump_json(indent=2)
        return Syntax(ri_json, "json", background_color="default")


class RenderValueResultDataType(InternalType[RenderValueResult, DataTypeConfig]):
    """A value type to contain information about how to render a value in a specific render scenario."""

    _data_type_name: ClassVar[str] = "render_value_result"

    def __init__(self, **type_config: Any):

        self._cls_cache: Union[Type[RenderValueResult], None] = None
        super().__init__(**type_config)

    @classmethod
    def python_class(cls) -> Type:
        return RenderValueResult

    def parse_python_obj(self, data: Any) -> RenderValueResult:

        if data is None:
            raise ValueError(
                "Can't parse render_scene_result data: no source data provided (None)."
            )
        elif isinstance(data, RenderValueResult):
            return data
        elif isinstance(data, Mapping):
            return RenderValueResult(**data)
        else:
            raise ValueError(
                f"Can't parse data, invalid type '{type(data)}': must be subclass of 'RenderValueResult' or Mapping."
            )

    def _validate(self, value: Any) -> None:

        if not isinstance(value, RenderValueResult):
            raise Exception(f"Invalid type: {type(value)}.")

    def _pretty_print_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        data: RenderValueResult = value.data

        ri_json = data.model_dump_json(indent=2, exclude={"rendered"})
        rendered = extract_renderable(data.rendered)

        metadata = Syntax(ri_json, "json", background_color="default")
        table = Table(show_header=True, box=box.SIMPLE)
        table.add_column("Rendered item")
        table.add_column("Render metadata")
        table.add_row(rendered, metadata)
        return table
