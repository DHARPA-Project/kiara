# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import importlib
from pydantic.fields import Field, PrivateAttr
from types import ModuleType
from typing import TYPE_CHECKING, Any, Dict, Type

from kiara.models import KiaraModel
from kiara.models.documentation import ContextMetadataModel

if TYPE_CHECKING:
    pass


class PythonClass(KiaraModel):
    """Python class and module information."""

    @classmethod
    def from_class(cls, item_cls: Type, attach_context_metadata: bool = False):

        cls_name = item_cls.__name__
        module_name = item_cls.__module__

        if module_name == "builtins":
            full_name = cls_name
        else:
            full_name = f"{item_cls.__module__}.{item_cls.__name__}"

        conf: Dict[str, Any] = {
            "python_class_name": cls_name,
            "python_module_name": module_name,
            "full_name": full_name,
        }

        if attach_context_metadata:
            raise NotImplementedError()
            ctx_md = ContextMetadataModel.from_class(item_cls=item_cls)
            conf["items"] = ctx_md

        result = PythonClass.construct(**conf)
        result._cls_cache = item_cls
        return result

    python_class_name: str = Field(description="The name of the Python class.")
    python_module_name: str = Field(
        description="The name of the Python module this class lives in."
    )
    full_name: str = Field(description="The full class namespace.")

    # context_metadata: Optional[ContextMetadataModel] = Field(
    #     description="Context metadata for the class.", default=None
    # )

    _module_cache: ModuleType = PrivateAttr(default=None)
    _cls_cache: Type = PrivateAttr(default=None)

    def _retrieve_id(self) -> str:
        return self.full_name

    def _retrieve_category_id(self) -> str:
        return "metadata.python_class"

    def _retrieve_data_to_hash(self) -> Any:
        return self.full_name

    def get_class(self) -> Type:

        if self._cls_cache is None:
            m = self.get_python_module()
            self._cls_cache = getattr(m, self.python_class_name)
        return self._cls_cache

    def get_python_module(self) -> ModuleType:
        if self._module_cache is None:
            self._module_cache = importlib.import_module(self.python_module_name)
        return self._module_cache
