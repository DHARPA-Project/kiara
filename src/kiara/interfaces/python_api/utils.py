# -*- coding: utf-8 -*-
import structlog
from typing import Any, Dict, Iterable, List, Mapping, Union

logger = structlog.getLogger()


def create_save_config(
    field_names: Union[str, Iterable[str]],
    aliases: Union[None, str, Iterable[str], Mapping[str, Any]],
) -> Dict[str, List[str]]:

    if isinstance(field_names, str):
        field_names = [field_names]

    if aliases is None:
        alias_map: Dict[str, List[str]] = {}
    elif isinstance(aliases, str):
        alias_map = {}
        for field_name in field_names:
            alias_map[field_name] = [f"{aliases}.{field_name}"]
    elif isinstance(aliases, Mapping):
        alias_map = {}
        for field_name in aliases.keys():
            if field_name in field_names:
                if isinstance(aliases[field_name], str):
                    alias_map[field_name] = [aliases[field_name]]
                else:
                    alias_map[field_name] = sorted(aliases[field_name])
            else:
                logger.warning(
                    "ignore.field_alias",
                    ignored_field_name=field_name,
                    reason="field name not in results",
                    available_field_names=sorted(field_names),
                )
                continue
    else:
        raise Exception(
            f"Invalid type '{type(aliases)}' for aliases parameter, must be string or mapping."
        )

    return alias_map
