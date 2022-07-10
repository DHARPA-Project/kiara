# -*- coding: utf-8 -*-
import copy
import dpath.util
from typing import Any, Dict, Mapping


def merge_dicts(*dicts: Mapping[str, Any]) -> Dict[str, Any]:

    if not dicts:
        return {}

    current: Dict[str, Any] = {}
    for d in dicts:
        dpath.util.merge(current, copy.deepcopy(d))

    return current
