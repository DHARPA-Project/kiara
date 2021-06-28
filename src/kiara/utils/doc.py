# -*- coding: utf-8 -*-
import typing
from inspect import cleandoc

from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.utils.output import first_line


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
