# -*- coding: utf-8 -*-
import uuid
from typing import Any, Iterable, Mapping, Type

from pydantic import Field

from kiara.models.rendering import RenderValueResult
from kiara.models.values.value import Value
from kiara.operations.included_core_operations.render_value import (
    RenderValueOperationType,
)
from kiara.renderers import KiaraRenderer, KiaraRendererConfig, RenderInputsSchema


class ValueRenderInputsSchema(RenderInputsSchema):
    render_config: Mapping[str, Any] = Field(
        description="The render config data.", default_factory=dict
    )


class ValueRendererConfig(KiaraRendererConfig):

    target_type: str = Field(description="The target type to render the value as.")


class ValueRendererTerminal(
    KiaraRenderer[
        Value, ValueRenderInputsSchema, RenderValueResult, ValueRendererConfig
    ]
):
    _renderer_name = "value_renderer"
    _inputs_schema = ValueRenderInputsSchema
    _renderer_config_cls = ValueRendererConfig

    @classmethod
    def retrieve_supported_source_types(self) -> Iterable[Type]:
        return [Value, str, uuid.UUID]

    _render_profiles: Mapping[str, Mapping[str, Any]] = {
        "terminal_renderable": {"target_type": "terminal_renderable"},
        "string": {"target_type": "string"},
    }

    def _render(
        self, instance: Value, render_config: ValueRenderInputsSchema
    ) -> RenderValueResult:

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

        result = render_op.run(
            self._kiara,
            inputs={"value": instance, "render_config": render_config.render_config},
        )
        rendered: RenderValueResult = result["render_value_result"].data  # type: ignore
        return rendered
