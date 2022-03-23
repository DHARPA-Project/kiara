# -*- coding: utf-8 -*-
from deepdiff import DeepHash
from typing import Any

from kiara.defaults import KIARA_HASH_FUNCTION


def compute_hash(obj: Any) -> int:

    h = DeepHash(obj, hasher=KIARA_HASH_FUNCTION)
    return h[obj]
