# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import copy
import dpath.util
import importlib
import inspect
import json
import logging
import os
import re
import typing
import yaml
from io import StringIO
from networkx import Graph
from pathlib import Path
from pkgutil import iter_modules
from pydantic.schema import (
    get_flat_models_from_model,
    get_model_name_map,
    model_process_schema,
)
from rich import box
from rich.console import ConsoleRenderable, RichCast
from rich.table import Table
from ruamel.yaml import YAML
from slugify import slugify
from types import ModuleType
from typing import Union

from kiara.defaults import INVALID_VALUE_NAMES, SpecialValue

if typing.TYPE_CHECKING:
    from kiara.data.values import ValueSchema
    from kiara.module_config import ModuleTypeConfigSchema

log = logging.getLogger("kiara")
CAMEL_TO_SNAKE_REGEX = re.compile(r"(?<!^)(?=[A-Z])")


def is_debug() -> bool:

    debug = os.environ.get("DEBUG", None)
    if isinstance(debug, str) and debug.lower() == "true":
        return True
    else:
        return False


def is_develop() -> bool:

    debug = os.environ.get("DEVELOP", None)
    if isinstance(debug, str) and debug.lower() == "true":
        return True
    else:
        return False


def log_message(msg: str):

    if is_debug():
        log.warning(msg)
    else:
        log.debug(msg)


def is_rich_renderable(item: typing.Any):
    return isinstance(item, (ConsoleRenderable, RichCast, str))


def get_data_from_file(
    path: Union[str, Path], content_type: typing.Optional[str] = None
) -> typing.Any:

    if isinstance(path, str):
        path = Path(os.path.expanduser(path))

    content = path.read_text()

    if content_type:
        assert content_type in ["json", "yaml"]
    else:
        if path.name.endswith(".json"):
            content_type = "json"
        elif path.name.endswith(".yaml") or path.name.endswith(".yml"):
            content_type = "yaml"
        else:
            raise ValueError(
                "Invalid data format, only 'json' or 'yaml' are supported currently."
            )

    if content_type == "json":
        data = json.loads(content)
    else:
        data = yaml.safe_load(content)

    return data


def print_ascii_graph(graph: Graph):

    try:
        from asciinet import graph_to_ascii
    except:  # noqa
        print(
            "\nCan't print graph on terminal, package 'asciinet' not available. Please install it into the current virtualenv using:\n\npip install 'git+https://github.com/cosminbasca/asciinet.git#egg=asciinet&subdirectory=pyasciinet'"
        )
        return

    try:
        from asciinet._libutil import check_java

        check_java("Java ")
    except Exception as e:
        print(e)
        print(
            "\nJava is currently necessary to print ascii graph. This might change in the future, but to use this functionality please install a JRE."
        )
        return

    print(graph_to_ascii(graph))


_AUTO_MODULE_ID: typing.Dict[str, int] = {}


def get_auto_workflow_alias(module_type: str, use_incremental_ids: bool = False) -> str:
    """Return an id for a workflow obj of a provided module class.

    If 'use_incremental_ids' is set to True, a unique id is returned.

    Args:
        module_type (str): the name of the module type
        use_incremental_ids (bool): whether to return a unique (incremental) id

    Returns:
        str: a module id
    """

    if not use_incremental_ids:
        return module_type

    nr = _AUTO_MODULE_ID.setdefault(module_type, 0)
    _AUTO_MODULE_ID[module_type] = nr + 1

    return f"{module_type}_{nr}"


def create_table_from_config_class(
    config_cls: typing.Type["ModuleTypeConfigSchema"],
    remove_pipeline_config: bool = False,
) -> Table:

    table = Table(box=box.HORIZONTALS, show_header=False)
    table.add_column("Field name", style="i")
    table.add_column("Type")
    table.add_column("Description")
    flat_models = get_flat_models_from_model(config_cls)
    model_name_map = get_model_name_map(flat_models)
    m_schema, _, _ = model_process_schema(config_cls, model_name_map=model_name_map)
    fields = m_schema["properties"]

    for field_name, details in fields.items():
        if remove_pipeline_config and field_name in [
            "steps",
            "input_aliases",
            "output_aliases",
            "doc",
        ]:
            continue

        type_str = "-- n/a --"
        if "type" in details.keys():
            type_str = details["type"]
        table.add_row(field_name, type_str, details.get("description", "-- n/a --"))

    return table


def create_table_from_field_schemas(
    _add_default: bool = True,
    _add_required: bool = True,
    _show_header: bool = False,
    _constants: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    **fields: "ValueSchema",
):

    table = Table(box=box.SIMPLE, show_header=_show_header)
    table.add_column("Field name", style="i")
    table.add_column("Type")
    table.add_column("Description")

    if _add_required:
        table.add_column("Required")
    if _add_default:
        if _constants:
            table.add_column("Default / Constant")
        else:
            table.add_column("Default")

    for field_name, schema in fields.items():

        row = [field_name, schema.type, schema.doc]

        if _add_required:
            req = schema.is_required()
            if not req:
                req_str = "no"
            else:
                if schema.default in [
                    None,
                    SpecialValue.NO_VALUE,
                    SpecialValue.NOT_SET,
                ]:
                    req_str = "[b]yes[b]"
                else:
                    req_str = "no"
            row.append(req_str)

        if _add_default:
            if _constants and field_name in _constants.keys():
                d = f"[b]{_constants[field_name]}[/b] (constant)"
            else:
                if schema.default in [
                    None,
                    SpecialValue.NO_VALUE,
                    SpecialValue.NOT_SET,
                ]:
                    d = "-- no default --"
                else:
                    d = str(schema.default)
            row.append(d)

        table.add_row(*row)

    return table


def dict_from_cli_args(
    *args: str, list_keys: typing.Optional[typing.Iterable[str]] = None
) -> typing.Dict[str, typing.Any]:

    if not args:
        return {}

    config: typing.Dict[str, typing.Any] = {}
    for arg in args:
        if "=" in arg:
            key, value = arg.split("=", maxsplit=1)
            try:
                _v = json.loads(value)
            except Exception:
                _v = value
            part_config = {key: _v}
        elif os.path.isfile(os.path.realpath(os.path.expanduser(arg))):
            path = os.path.realpath(os.path.expanduser(arg))
            part_config = get_data_from_file(path)
            assert isinstance(part_config, typing.Mapping)
        else:
            try:
                part_config = json.loads(arg)
                assert isinstance(part_config, typing.Mapping)
            except Exception:
                raise Exception(f"Could not parse argument into data: {arg}")

        if list_keys is None:
            list_keys = []

        for k, v in part_config.items():
            if k in list_keys:
                config.setdefault(k, []).append(v)
            else:
                if k in config.keys():
                    log.warning(f"Duplicate key '{k}', overwriting old value with: {v}")
                config[k] = v
    return config


def camel_case_to_snake_case(camel_text: str, repl: str = "_"):
    return CAMEL_TO_SNAKE_REGEX.sub(repl, camel_text).lower()


class StringYAML(YAML):
    def dump(self, data, stream=None, **kw):
        inefficient = False
        if stream is None:
            inefficient = True
            stream = StringIO()
        YAML.dump(self, data, stream, **kw)
        if inefficient:
            return stream.getvalue()


def _get_all_subclasses(
    cls: typing.Type, ignore_abstract: bool = False
) -> typing.Iterable[typing.Type]:

    result = []
    for subclass in cls.__subclasses__():
        if ignore_abstract and inspect.isabstract(subclass):
            continue
        result.append(subclass)
        result.extend(_get_all_subclasses(subclass))

    return result


def _import_modules_recursively(module: ModuleType):

    if not hasattr(module, "__path__"):
        return

    for submodule in iter_modules(module.__path__):  # type: ignore

        submodule_mod = importlib.import_module(f"{module.__name__}.{submodule.name}")
        if hasattr(submodule_mod, "__path__"):
            _import_modules_recursively(submodule_mod)


def check_valid_field_names(*field_names) -> typing.List[str]:
    """Check whether the provided field names are all valid.

    Returns:
        an iterable of strings with invalid field names
    """

    return [x for x in field_names if x in INVALID_VALUE_NAMES or x.startswith("_")]


def create_valid_identifier(text: str):

    return slugify(text, separator="_")


def merge_dicts(
    *dicts: typing.Mapping[str, typing.Any]
) -> typing.Dict[str, typing.Any]:

    if not dicts:
        return {}

    current: typing.Dict[str, typing.Any] = {}
    for d in dicts:
        dpath.util.merge(current, copy.deepcopy(d))

    return current


string_types = (type(b""), type(""))
