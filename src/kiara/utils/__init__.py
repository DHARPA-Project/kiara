# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import inspect
import os
import re
import structlog
import sys
from rich.traceback import Traceback
from typing import TYPE_CHECKING, Dict, Iterable, List, Type, TypeVar

from kiara.defaults import INVALID_VALUE_NAMES

if TYPE_CHECKING:
    from kiara.utils.develop import KiaraDevSettings

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

    develop = os.environ.get("DEVELOP", "")
    if not develop:
        develop = os.environ.get("DEV", "")

    if develop and develop.lower() != "false":
        return True

    return False


def get_dev_config() -> "KiaraDevSettings":

    from kiara.utils.develop import KIARA_DEV_SETTINGS

    return KIARA_DEV_SETTINGS


def is_jupyter() -> bool:

    try:
        get_ipython  # type: ignore
    except NameError:
        return False
    ipython = get_ipython()  # type: ignore  # noqa
    shell = ipython.__class__.__name__
    if shell == "TerminalInteractiveShell":
        return False
    elif "google.colab" in str(ipython.__class__) or shell == "ZMQInteractiveShell":
        return True
    else:
        return False


def log_exception(exc: Exception):

    if is_debug():
        logger.error(exc)

    if is_develop():
        from kiara.utils.develop import DetailLevel

        config = get_dev_config()
        if config.log.exc in [DetailLevel.NONE, "none"]:
            return

        show_locals = config.log.exc in [DetailLevel.FULL, "full"]

        from kiara.interfaces import get_console
        from kiara.utils.develop import log_dev_message

        exc_info = sys.exc_info()

        if not exc_info or not exc_info[0]:
            # TODO: create exc_info from exception?
            if not is_debug():
                logger.error(exc)
        else:
            console = get_console()
            log_dev_message(
                Traceback.from_exception(
                    type(exc_info[0]), exc_info[1], traceback=exc_info[2], show_locals=show_locals, width=console.width - 4  # type: ignore
                ),
                title="Exception details",
            )


def log_message(msg: str, **data):

    if is_debug():
        logger.debug(msg, **data)
    # else:
    #     logger.debug(msg, **data)


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


def camel_case_to_snake_case(camel_text: str, repl: str = "_"):
    return CAMEL_TO_SNAKE_REGEX.sub(repl, camel_text).lower()


def to_camel_case(text: str) -> str:

    words = WORD_REGEX_PATTERN.split(text)
    return "".join(w.title() for i, w in enumerate(words))


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


def check_valid_field_names(*field_names) -> List[str]:
    """Check whether the provided field names are all valid.

    Returns:
        an iterable of strings with invalid field names
    """

    return [x for x in field_names if x in INVALID_VALUE_NAMES or x.startswith("_")]


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


def first_line(text: str):

    if "\n" in text:
        return text.split("\n")[0].strip()
    else:
        return text
