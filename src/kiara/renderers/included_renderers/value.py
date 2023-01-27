# -*- coding: utf-8 -*-
import uuid
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Type, Union

from pydantic import Field

from kiara.models.rendering import RenderValueResult
from kiara.models.values.value import Value
from kiara.operations.included_core_operations.render_value import (
    RenderValueOperationType,
)
from kiara.renderers import (
    KiaraRenderer,
    KiaraRendererConfig,
    RenderInputsSchema,
    SourceTransformer,
)

if TYPE_CHECKING:
    from kiara.context import Kiara


class ValueTransformer(SourceTransformer):
    def __init__(self, kiara: "Kiara", target_type: str):

        self._kiara: Kiara = kiara
        self._target_type: str = target_type
        super().__init__()

    def retrieve_supported_python_classes(self) -> Iterable[Type]:

        return [Value, uuid.UUID, str]

    def validate_and_transform(self, source: Any) -> Union[Value, None]:

        return self._kiara.data_registry.get_value(source)

    def retrieve_supported_inputs_descs(self) -> Union[str, Iterable[str]]:
        op_type: RenderValueOperationType = self._kiara.operation_registry.get_operation_type("render_value")  # type: ignore
        ops = op_type.get_render_operations_for_target_type(
            target_type=self._target_type
        )

        return [
            f"a value of type '{x}'"
            for x in sorted(ops.keys())
            if x not in ["any", "none"]
        ]


class ValueRenderInputsSchema(RenderInputsSchema):
    render_config: Mapping[str, Any] = Field(
        description="The render config data.", default_factory=dict
    )
    include_metadata: bool = Field(
        description="Whether to include metadata.", default=False
    )
    include_data: bool = Field(
        description="Whether to include data (only applies when 'include_metadata' is set to 'True').",
        default=True,
    )


class ValueRendererConfig(KiaraRendererConfig):
    target_type: str = Field(description="The target type to render the value as.")


class ValueRenderer(
    KiaraRenderer[
        Value, ValueRenderInputsSchema, RenderValueResult, ValueRendererConfig
    ]
):
    _renderer_name = "value_renderer"
    _inputs_schema = ValueRenderInputsSchema
    _renderer_config_cls = ValueRendererConfig

    # _render_profiles: Mapping[str, Mapping[str, Any]] = {
    #     "terminal_renderable": {"target_type": "terminal_renderable"},
    #     "string": {"target_type": "string"},
    # }

    def get_renderer_alias(self) -> str:
        return f"value_to_{self.renderer_config.target_type}"

    def retrieve_supported_render_sources(self) -> str:
        return "value"

    def retrieve_supported_render_targets(self) -> Union[Iterable[str], str]:
        return f"value:{self.renderer_config.target_type}"

    def retrieve_source_transformers(self) -> Iterable[SourceTransformer]:
        return [
            ValueTransformer(
                kiara=self._kiara, target_type=self.renderer_config.target_type
            )
        ]

    def retrieve_doc(self) -> Union[str, None]:

        return f"Render a value (of a supported type) to a value of type '{self.renderer_config.target_type}'."

    def _render(self, instance: Value, render_config: ValueRenderInputsSchema) -> Any:

        target_type = self.renderer_config.target_type
        op_type: RenderValueOperationType = (
            self._kiara.operation_registry.get_operation_type("render_value")
        )  # type: ignore
        render_op = op_type.get_render_operation(
            source_type=instance.data_type_name, target_type=target_type
        )
        if render_op is None:
            raise Exception(
                f"Can't find render operation for source type '{instance.data_type_name}' to '{target_type}'."
            )

        result: Any = render_op.run(
            self._kiara,
            inputs={"value": instance, "render_config": render_config.render_config},
        )
        rendered: RenderValueResult = result["render_value_result"].data  # type: ignore

        if not render_config.include_metadata:
            result = rendered.rendered
        else:
            if not render_config.include_data:
                rendered.rendered = None
            result = rendered

        return result
