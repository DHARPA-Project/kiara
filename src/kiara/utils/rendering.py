# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Any, Mapping, MutableMapping

from kiara.exceptions import KiaraException

if TYPE_CHECKING:
    from kiara.models.module.pipeline import Pipeline


def create_pipeline_render_inputs(
    pipeline: "Pipeline", pipeline_inputs_user: Mapping[str, Any]
) -> Mapping[str, Any]:
    from kiara.defaults import SpecialValue

    invalid = []
    for field_name in pipeline_inputs_user.keys():
        if field_name not in pipeline.pipeline_inputs_schema.keys():
            invalid.append(field_name)

    if invalid:
        msg = "Valid pipeline inputs:\n"
        for field_name, field in pipeline.pipeline_inputs_schema.items():
            msg = f"{msg}  - *{field_name}*: {field.doc.description}\n"
        raise KiaraException(
            msg=f"Invalid pipeline inputs: {', '.join(invalid)}.", details=msg
        )

    pipeline_inputs = {}
    for field_name, schema in pipeline.pipeline_inputs_schema.items():
        if field_name in pipeline_inputs_user.keys():
            value = pipeline_inputs_user[field_name]
        elif schema.default not in [SpecialValue.NOT_SET]:
            if callable(schema.default):
                value = schema.default()
            else:
                value = schema.default
        elif not schema.is_required():
            value = None
        else:
            value = "<TODO_SET_INPUT>"

        if isinstance(value, str):
            value = f'"{value}"'
        pipeline_inputs[field_name] = value

    inputs: MutableMapping[str, Any] = {}
    inputs["pipeline"] = pipeline
    inputs["pipeline_inputs"] = pipeline_inputs
    return inputs
