# -*- coding: utf-8 -*-
import copy
import dpath
from typing import Any, Dict, Mapping


def merge_dicts(*dicts: Mapping[str, Any]) -> Dict[str, Any]:

    if not dicts:
        return {}

    current: Dict[str, Any] = {}
    for d in dicts:
        dpath.merge(current, dict(copy.deepcopy(d)))

    return current
