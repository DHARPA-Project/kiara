# -*- coding: utf-8 -*-
from typing import Any, Mapping


def module_config_is_empty(config: Mapping[str, Any]):

    c = dict(config)
    d = c.pop("defaults", None)
    if d:
        return False
    constants = c.pop("constants", None)
    if constants:
        return False

    return False if c else True
