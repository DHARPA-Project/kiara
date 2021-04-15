# -*- coding: utf-8 -*-
import inspect
import json
import logging
import os
import re
import sys
import typing
import yaml
from io import StringIO
from networkx import Graph
from pathlib import Path
from pydantic.schema import (
    get_flat_models_from_model,
    get_model_name_map,
    model_process_schema,
)
from rich import box
from rich.table import Table
from ruamel.yaml import YAML
from stevedore import ExtensionManager
from typing import Union

if typing.TYPE_CHECKING:
    from kiara.config import KiaraModuleConfig, PipelineModuleConfig
    from kiara.data.values import ValueSchema
    from kiara.module import KiaraModule

log = logging.getLogger("kiara")
CAMEL_TO_SNAKE_REGEX = re.compile(r"(?<!^)(?=[A-Z])")


def get_data_from_file(path: Union[str, Path]) -> typing.Any:

    if isinstance(path, str):
        path = Path(os.path.expanduser(path))

    content = path.read_text()

    if path.name.endswith(".json"):
        content_type = "json\n"
    elif path.name.endswith(".yaml") or path.name.endswith(".yml"):
        content_type = "yaml\n"
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
            "\nJava is currently necessary to print ascii graphs. This might change in the future, but to use this functionality please install a JRE."
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


def find_kiara_modules() -> typing.Dict[str, typing.Type["KiaraModule"]]:

    log2 = logging.getLogger("stevedore")
    out_hdlr = logging.StreamHandler(sys.stdout)
    out_hdlr.setFormatter(logging.Formatter("kiara module plugin error -> %(message)s"))
    out_hdlr.setLevel(logging.INFO)
    log2.addHandler(out_hdlr)
    log2.setLevel(logging.INFO)

    log.debug("Loading kiara modules...")

    mgr = ExtensionManager(
        namespace="kiara.modules", invoke_on_load=False, propagate_map_exceptions=True
    )

    result = {}
    for plugin in mgr:
        name = plugin.name
        ep = plugin.entry_point
        module_cls = ep.load()
        result[name] = module_cls

    return result


def create_table_from_config_class(
    config_cls: typing.Type["KiaraModuleConfig"], remove_pipeline_config: bool = False
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

        table.add_row(
            field_name, details["type"], details.get("description", "-- n/a --")
        )

    return table


def create_table_from_field_schemas(**fields: "ValueSchema"):

    table = Table(box=box.SIMPLE, show_header=False)
    table.add_column("Field name", style="i")
    table.add_column("Value type")
    table.add_column("Default")

    for field_name, schema in fields.items():
        d = "-- no default --" if schema.default is None else str(schema.default)
        table.add_row(field_name, schema.type, d)  # type: ignore

    return table


def module_config_from_cli_args(*args: str) -> typing.Dict[str, typing.Any]:

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
                raise Exception(f"Could not parse argument into module config: {arg}")

        for k, v in part_config.items():
            if k in config.keys():
                log.warning(
                    f"Duplicate config key '{k}', overwriting old value with: {v}"
                )
            config[k] = v
    return config


def get_doc_for_module_class(module_cls: typing.Type["KiaraModule"]):

    from kiara import PipelineModule

    if module_cls == PipelineModule or not module_cls.is_pipeline():

        doc = module_cls.__doc__
        if not doc:
            doc = "-- n/a --"
        else:
            doc = inspect.cleandoc(doc)
    else:
        bpc: "PipelineModuleConfig" = module_cls._base_pipeline_config  # type: ignore
        doc = bpc.doc
    return doc


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
