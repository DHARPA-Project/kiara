# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import copy
from typing import Any, Dict, Mapping, Optional, Union

from kiara.defaults import INVALID_VALUE_NAMES, SpecialValue
from kiara.models.values.value_schema import ValueSchema
from kiara.utils import check_valid_field_names


def create_schema_dict(
    schema_config: Mapping[str, Union[ValueSchema, Mapping[str, Any]]],
) -> Mapping[str, ValueSchema]:

    invalid = check_valid_field_names(*schema_config.keys())
    if invalid:
        raise Exception(
            f"Can't assemble schema because it contains invalid input field name(s) '{', '.join(invalid)}'. Change the input schema to not contain any of the reserved keywords: {', '.join(INVALID_VALUE_NAMES)}"
        )

    result = {}
    for k, v in schema_config.items():

        if isinstance(v, ValueSchema):
            result[k] = v
        elif isinstance(v, Mapping):
            schema = ValueSchema(**v)
            result[k] = schema
        else:
            if v is None:
                msg = "None"
            else:
                msg = v.__class__
            raise Exception(
                f"Invalid return type '{msg}' for field '{k}' when trying to create schema."
            )

    return result


def overlay_constants_and_defaults(
    schemas: Mapping[str, ValueSchema],
    defaults: Mapping[str, Any],
    constants: Mapping[str, Any],
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

        if constant_value is not None:
            schemas[k].default = constant_value
            schemas[k].is_constant = True

    input_schemas = {}
    constants = {}
    for k, v in schemas.items():
        if v.is_constant:
            constants[k] = v
        else:
            input_schemas[k] = v

    return input_schemas, constants


def augment_values(
    values: Mapping[str, Any],
    schemas: Mapping[str, ValueSchema],
    constants: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:

    # TODO: check if extra fields were provided

    if constants:
        for k, v in constants.items():
            if k in values.keys():
                raise Exception(f"Invalid input: value provided for constant '{k}'")

    values_new = {}
    for field_name, schema in schemas.items():

        if field_name not in values.keys():
            if constants and field_name in constants:
                values_new[field_name] = copy.deepcopy(constants[field_name])
            elif schema.default != SpecialValue.NOT_SET:
                if callable(schema.default):
                    values_new[field_name] = schema.default()
                else:
                    values_new[field_name] = copy.deepcopy(schema.default)
            else:
                values_new[field_name] = SpecialValue.NOT_SET
        else:
            value = values[field_name]
            if value is None:
                value = SpecialValue.NO_VALUE
            values_new[field_name] = value

    return values_new
