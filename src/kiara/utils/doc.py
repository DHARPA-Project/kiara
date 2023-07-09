# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import typing
from inspect import cleandoc

from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.utils import first_line


def extract_doc_from_cls(cls: typing.Type, only_first_line: bool = False):

    doc = cls.__doc__
    if not doc:
        doc = DEFAULT_NO_DESC_VALUE
    else:
        doc = cleandoc(doc)

    if only_first_line:
        return first_line(doc)
    else:
        return doc.strip()


def extract_doc_from_func(func: typing.Callable, only_first_line: bool = False):

    doc = func.__doc__
    if not doc:
        doc = DEFAULT_NO_DESC_VALUE
    else:
        doc = cleandoc(doc)

    if only_first_line:
        return first_line(doc)
    else:
        return doc.strip()
