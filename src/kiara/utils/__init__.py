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
import orjson
import os
import re
import structlog
import traceback
from io import StringIO
from pathlib import Path
from pkgutil import iter_modules
from rich.console import ConsoleRenderable, RichCast
from ruamel.yaml import YAML
from slugify import slugify
from types import ModuleType
from typing import Any, Dict, Iterable, List, Mapping, Optional, Type, TypeVar, Union

from kiara.defaults import INVALID_VALUE_NAMES

yaml = YAML(typ="safe")

logger = structlog.get_logger()

CAMEL_TO_SNAKE_REGEX = re.compile(r"(?<!^)(?=[A-Z])")

WORD_REGEX_PATTERN = re.compile("[^A-Za-z]+")


def is_debug() -> bool:

    debug = os.environ.get("DEBUG", "")
    if debug.lower() == "true":
        return True
    else:
        return False


def is_develop() -> bool:

    debug = os.environ.get("DEVELOP", "")
    if debug.lower() == "true":
        return True
    else:
        return False


def log_exception(exc: Exception):

    if is_debug():
        traceback.print_exc()


def log_message(msg: str, **data):

    if is_debug():
        logger.debug(msg, **data)
    # else:
    #     logger.debug(msg, **data)


def is_rich_renderable(item: Any):
    return isinstance(item, (ConsoleRenderable, RichCast, str))


def get_data_from_file(
    path: Union[str, Path], content_type: Optional[str] = None
) -> Any:

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
        data = yaml.load(content)

    return data


_AUTO_MODULE_ID: Dict[str, int] = {}


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


def dict_from_cli_args(
    *args: str, list_keys: Optional[Iterable[str]] = None
) -> Dict[str, Any]:

    if not args:
        return {}

    config: Dict[str, Any] = {}
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
            assert isinstance(part_config, Mapping)
        else:
            try:
                part_config = json.loads(arg)
                assert isinstance(part_config, Mapping)
            except Exception:
                raise Exception(f"Could not parse argument into data: {arg}")

        if list_keys is None:
            list_keys = []

        for k, v in part_config.items():
            if k in list_keys:
                config.setdefault(k, []).append(v)
            else:
                if k in config.keys():
                    logger.warning("duplicate.key", old_value=k, new_value=v)
                config[k] = v
    return config


def camel_case_to_snake_case(camel_text: str, repl: str = "_"):
    return CAMEL_TO_SNAKE_REGEX.sub(repl, camel_text).lower()


def to_camel_case(text: str) -> str:

    words = WORD_REGEX_PATTERN.split(text)
    return "".join(w.title() for i, w in enumerate(words))


class StringYAML(YAML):
    def dump(self, data, stream=None, **kw):
        inefficient = False
        if stream is None:
            inefficient = True
            stream = StringIO()
        YAML.dump(self, data, stream, **kw)
        if inefficient:
            return stream.getvalue()


SUBCLASS_TYPE = TypeVar("SUBCLASS_TYPE")


def _get_all_subclasses(
    cls: Type[SUBCLASS_TYPE], ignore_abstract: bool = False
) -> Iterable[Type[SUBCLASS_TYPE]]:

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

        try:
            submodule_mod = importlib.import_module(
                f"{module.__name__}.{submodule.name}"
            )
            if hasattr(submodule_mod, "__path__"):
                _import_modules_recursively(submodule_mod)
        except Exception as e:
            logger.error(
                "ignore.python_module",
                module=f"{module.__name__}.{submodule.name}",
                reason=str(e),
                base_module=str(module),
            )


def check_valid_field_names(*field_names) -> List[str]:
    """Check whether the provided field names are all valid.

    Returns:
        an iterable of strings with invalid field names
    """

    return [x for x in field_names if x in INVALID_VALUE_NAMES or x.startswith("_")]


def create_valid_identifier(text: str):

    return slugify(text, separator="_")


def merge_dicts(*dicts: Mapping[str, Any]) -> Dict[str, Any]:

    if not dicts:
        return {}

    current: Dict[str, Any] = {}
    for d in dicts:
        dpath.util.merge(current, copy.deepcopy(d))

    return current


def find_free_id(
    stem: str,
    current_ids: Iterable[str],
    sep="_",
) -> str:
    """Find a free var (or other name) based on a stem string, based on a list of provided existing names.

    Args:
        stem (str): the base string to use
        current_ids (Iterable[str]): currently existing names
        method (str): the method to create new names (allowed: 'count' -- for now)
        method_args (dict): prototing_config for the creation method

    Returns:
        str: a free name
    """

    start_count = 1
    if stem not in current_ids:
        return stem

    i = start_count

    # new_name = None
    while True:
        new_name = f"{stem}{sep}{i}"
        if new_name in current_ids:
            i = i + 1
            continue
        break
    return new_name


string_types = (type(b""), type(""))


def orjson_dumps(v, *, default=None, **args):
    # orjson.dumps returns bytes, to match standard json.dumps we need to decode

    try:
        return orjson.dumps(v, default=default, **args).decode()
    except Exception as e:
        if is_debug():
            print(f"Error dumping json data: {e}")
            from kiara import dbg

            dbg(v)

        raise e


def first_line(text: str):

    if "\n" in text:
        return text.split("\n")[0].strip()
    else:
        return text
