# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import copy
import uuid
from typing import TYPE_CHECKING, Any, Dict, Mapping, Union

from kiara.defaults import (
    DEFAULT_NO_DESC_VALUE,
    INVALID_VALUE_NAMES,
    NONE_VALUE_ID,
    SpecialValue,
)
from kiara.models.values.value_schema import ValueSchema
from kiara.utils import check_valid_field_names

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.interfaces.python_api import KiaraAPI
    from kiara.models.values.value import ValueMapReadOnly
    from kiara.registries.data import ValueLink


def construct_valuemap(
    kiara_api: "KiaraAPI",
    values: Mapping[str, Union[uuid.UUID, None, str, "ValueLink"]],
) -> "ValueMapReadOnly":

    value_items = {}
    schemas = {}
    for field_name, value_id in values.items():
        if value_id is None:
            value_id = NONE_VALUE_ID

        value = kiara_api.get_value(value=value_id)
        value_items[field_name] = value
        schemas[field_name] = value.value_schema

    from kiara.models.values.value import ValueMapReadOnly

    return ValueMapReadOnly(value_items=value_items, values_schema=schemas)


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
            _v = dict(v)
            if "doc" not in _v.keys():
                _v["doc"] = DEFAULT_NO_DESC_VALUE
            schema = ValueSchema(**_v)

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
    constants: Union[Mapping[str, ValueSchema], None] = None,
) -> Dict[str, Any]:

    # TODO: check if extra fields were provided

    if constants:
        for k, v in constants.items():
            if k in values.keys():
                raise Exception(f"Invalid input: value provided for constant '{k}'")

    values_new = {}

    if constants:
        for field_name, schema in constants.items():
            v = schema.default
            assert v not in [None, SpecialValue.NO_VALUE, SpecialValue.NOT_SET]
            if callable(v):
                values_new[field_name] = v()
            else:
                values_new[field_name] = copy.deepcopy(v)

    for field_name, schema in schemas.items():

        if field_name in values_new.keys():
            raise Exception(
                f"Duplicate field '{field_name}', this is most likely a bug."
            )

        val = values.get(field_name, None)
        use_default = False
        if val is None:
            use_default = True
        elif hasattr(val, "is_set"):
            if not val.is_set:  # type: ignore
                use_default = True
        if use_default:
            if schema.default != SpecialValue.NOT_SET:
                if callable(schema.default):
                    values_new[field_name] = schema.default()
                else:
                    values_new[field_name] = copy.deepcopy(schema.default)
            else:
                values_new[field_name] = SpecialValue.NOT_SET
        else:
            value = values[field_name]
            assert value is not None

            values_new[field_name] = value

    return values_new


def extract_raw_values(kiara: "Kiara", **value_ids: uuid.UUID) -> Dict[str, Any]:

    result = {}
    for field_name, value_id in value_ids.items():
        result[field_name] = extract_raw_value(kiara=kiara, value_id=value_id)
    return result


# def extract_raw_value(kiara: "Kiara", value_id: uuid.UUID):
#     value = kiara.data_registry.get_value(value_id=value_id)
#     if value.pedigree != ORPHAN:
#         return f"value:{value_id}"
#     else:
#         return value.data


def extract_raw_value(kiara: "Kiara", value_id: uuid.UUID):
    value = kiara.data_registry.get_value(value=value_id)

    # TODO: check without import
    from kiara.models.values.value import ORPHAN

    if value.pedigree != ORPHAN:
        # TODO: find alias?
        return f'"value:{value_id}"'
    else:
        if value.value_schema.type == "string":
            return f'"{value.data}"'
        elif value.value_schema.type == "list":
            return value.data.list_data
        else:
            return value.data
