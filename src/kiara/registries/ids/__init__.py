# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import structlog
import uuid
from typing import Any, Dict, Optional, Type
from weakref import WeakValueDictionary

from kiara.utils import is_debug, is_develop

logger = structlog.getLogger()


class NO_TYPE_MARKER(object):
    pass


class IdRegistry(object):
    def __init__(self):
        self._ids: Dict[uuid.UUID, Dict[Type, Dict[str, Any]]] = {}
        self._objs: Dict[uuid.UUID, WeakValueDictionary[Type, Any]] = {}

    def generate(
        self,
        id: Optional[uuid.UUID] = None,
        obj_type: Optional[Type] = None,
        obj: Optional[Any] = None,
        **metadata: Any
    ):

        if id is None:
            id = uuid.uuid4()

        if is_debug() or is_develop():

            # logger.debug("generate.id", id=id, metadata=metadata)
            if obj_type is None:
                if obj:
                    obj_type = obj.__class__
                else:
                    obj_type = NO_TYPE_MARKER
            self._ids.setdefault(id, {}).setdefault(obj_type, {}).update(metadata)
            if obj:
                self._objs.setdefault(id, WeakValueDictionary())[obj_type] = obj

        return id

    def update_metadata(
        self,
        id: uuid.UUID,
        obj_type: Optional[Type] = None,
        obj: Optional[Any] = None,
        **metadata
    ):

        if not is_debug() and not is_develop():
            return

        if obj_type is None:
            if obj:
                obj_type = obj.__class__
            else:
                obj_type = NO_TYPE_MARKER
        self._ids.setdefault(id, {}).setdefault(obj_type, {}).update(metadata)
        if obj:
            self._objs.setdefault(id, WeakValueDictionary())[obj_type] = obj


ID_REGISTRY = IdRegistry()
