# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from deepdiff import DeepHash
from typing import Any

from kiara.defaults import KIARA_HASH_FUNCTION


def compute_hash(obj: Any) -> int:

    h = DeepHash(obj, hasher=KIARA_HASH_FUNCTION)
    return h[obj]
