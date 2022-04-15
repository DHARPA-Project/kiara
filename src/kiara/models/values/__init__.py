# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from enum import Enum


class ValueStatus(Enum):

    UNKNONW = "unknown"
    NOT_SET = "not set"
    NONE = "none"
    DEFAULT = "default"
    SET = "set"
