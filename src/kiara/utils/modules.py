# -*- coding: utf-8 -*-
import typing

from kiara.data.values import ValueSchema
from kiara.utils import check_valid_field_names

if typing.TYPE_CHECKING:
    from kiara import Kiara


def create_schemas(
    schema_config: typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ],
    kiara: "Kiara",
) -> typing.Mapping[str, ValueSchema]:

    invalid = check_valid_field_names(*schema_config.keys())
    if invalid:
        raise Exception(
            f"Can't assemble schema, contains invalid input field name(s): {', '.join(invalid)}"
        )

    result = {}
    for k, v in schema_config.items():

        if isinstance(v, ValueSchema):
            result[k] = v
        elif isinstance(v, typing.Mapping):
            schema = ValueSchema(**v)
            schema.validate_types(kiara)
            result[k] = schema
        else:
            raise Exception(
                f"Invalid return type '{v.__class__}' for field '{k}' when trying to create schema."
            )

    return result


def overlay_constants_and_defaults(
    schemas: typing.Mapping[str, ValueSchema],
    defaults: typing.Mapping[str, typing.Any],
    constants: typing.Mapping[str, typing.Any],
):

    for k, v in schemas.items():

        default_value = defaults.get(k, None)
        constant_value = constants.get(k, None)

        # value_to_test = None
        if default_value is not None and constant_value is not None:
            raise Exception(
                f"Module configuration error. Value '{k}' set in both 'constants' and 'defaults', this is not allowed."
            )

        # TODO: perform validation for constants/defaults

        if default_value is not None:
            schemas[k].default = default_value
