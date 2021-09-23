# -*- coding: utf-8 -*-
import os
import typing

from kiara.data.values import ValueSchema
from kiara.defaults import INVALID_VALUE_NAMES
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
            f"Can't assemble schema because it contains invalid input field name(s) '{', '.join(invalid)}'. Change the input schema to not contain any of the reserved keywords: {', '.join(INVALID_VALUE_NAMES)}"
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


def find_file_for_module(
    module_name: str, kiara: typing.Optional["Kiara"] = None
) -> str:
    """Find the python file a module belongs to."""

    if kiara is None:
        from kiara.kiara import Kiara

        kiara = Kiara.instance()

    m_cls = kiara.get_module_class(module_type=module_name)
    python_module = m_cls.get_type_metadata().python_class.get_module()

    # TODO: some sanity checks
    if python_module.__file__.endswith("__init__.py"):
        extra_bit = (
            python_module.__name__.replace(".", os.path.sep)
            + os.path.sep
            + "__init__.py"
        )
    else:
        extra_bit = python_module.__name__.replace(".", os.path.sep) + ".py"
    python_file_path = python_module.__file__[0 : -len(extra_bit)]  # noqa

    return python_file_path


def find_all_module_python_files(
    kiara: typing.Optional["Kiara"] = None,
) -> typing.Set[str]:

    if kiara is None:
        from kiara.kiara import Kiara

        kiara = Kiara.instance()

    all_paths = set()
    for module_name in kiara.available_non_pipeline_module_types:
        path = find_file_for_module(module_name=module_name, kiara=kiara)
        all_paths.add(path)
    return all_paths
