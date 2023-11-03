# -*- coding: utf-8 -*-
#  Copyright (c) 2019, Markus Binsteiner
#
# Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
# as well as
# Parity Public License, version 7.0.0 (see https://paritylicense.com/)

import typing
from typing import Any, Mapping, Sequence, Set, Union

import regex as re
import structlog
from regex.regex import Pattern

log = structlog.getLogger()


def create_var_regex(
    delimiter_start: Union[str, None] = None, delimiter_end: Union[str, None] = None
) -> Pattern:

    if delimiter_start is None:
        delimiter_start = "\\$\\{"

    # TODO: make this smarter
    if delimiter_end is None:
        delimiter_end = "\\}"

    regex = re.compile(delimiter_start + "\\s*(.+?)\\s*" + delimiter_end)
    return regex


def find_var_names_in_obj(
    template_obj: Any,
    delimiter: Union[Pattern, str, None] = None,
    delimiter_end: Union[str, None] = None,
) -> Set[str]:

    if isinstance(delimiter, Pattern):
        regex = delimiter
    else:
        regex = create_var_regex(delimiter_start=delimiter, delimiter_end=delimiter_end)

    var_names = find_regex_matches_in_obj(template_obj, regex=regex)

    return var_names


def replace_var_names_in_obj(
    template_obj: Any,
    repl_dict: typing.Mapping[str, Any],
    delimiter: Union[Pattern, str, None] = None,
    delimiter_end: Union[str, None] = None,
    ignore_missing_keys: bool = False,
) -> Any:

    if isinstance(delimiter, Pattern):
        regex = delimiter
    else:
        regex = create_var_regex(delimiter_start=delimiter, delimiter_end=delimiter_end)

    if not template_obj:
        return template_obj

    if isinstance(template_obj, Mapping):
        result: Any = {}
        for k, v in template_obj.items():
            key = replace_var_names_in_obj(
                template_obj=k,
                repl_dict=repl_dict,
                delimiter=regex,
                ignore_missing_keys=ignore_missing_keys,
            )
            value = replace_var_names_in_obj(
                template_obj=v,
                repl_dict=repl_dict,
                delimiter=regex,
                ignore_missing_keys=ignore_missing_keys,
            )
            result[key] = value
    elif isinstance(template_obj, str):
        result = replace_var_names_in_string(
            template_obj,
            repl_dict=repl_dict,
            regex=regex,
            ignore_missing_keys=ignore_missing_keys,
        )
    elif isinstance(template_obj, Sequence):
        result = []
        for item in template_obj:
            r = replace_var_names_in_obj(
                item,
                repl_dict=repl_dict,
                delimiter=regex,
                ignore_missing_keys=ignore_missing_keys,
            )
            result.append(r)
    else:
        result = template_obj

    return result


def replace_var_names_in_string(
    template_string: str,
    repl_dict: typing.Mapping[str, Any],
    regex: Pattern,
    ignore_missing_keys: bool = False,
) -> str:
    def sub(match):

        key = match.groups()[0]

        if key not in repl_dict.keys():
            if not ignore_missing_keys:
                raise Exception(
                    f"Can't insert variable '{key}'. Key not in provided input values, available keys: {', '.join(repl_dict.keys())}"
                )
            else:
                return match[0]
        else:
            result = repl_dict[key]
            return result

    result: str = regex.sub(sub, template_string)
    return result


def find_regex_matches_in_obj(
    source_obj: Any, regex: Pattern, current: Union[Set[str], None] = None
) -> Set[str]:

    if current is None:
        current = set()

    if not source_obj:
        return current

    if isinstance(source_obj, Mapping):
        for k, v in source_obj.items():
            find_regex_matches_in_obj(k, regex=regex, current=current)
            find_regex_matches_in_obj(v, regex=regex, current=current)
    elif isinstance(source_obj, str):

        matches = regex.findall(source_obj)
        current.update(matches)

    elif isinstance(source_obj, Sequence):

        for item in source_obj:
            find_regex_matches_in_obj(item, regex=regex, current=current)

    return current
