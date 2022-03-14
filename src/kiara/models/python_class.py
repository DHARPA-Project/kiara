# -*- coding: utf-8 -*-
import importlib
from pydantic.fields import Field, PrivateAttr
from types import ModuleType
from typing import Any, ClassVar, Dict, Type

from kiara.models import KiaraModel


class PythonClass(KiaraModel):
    """Python class and module information."""

    @classmethod
    def from_class(cls, item_cls: Type):

        cls_name = item_cls.__name__
        module_name = item_cls.__module__
        if module_name == "builtins":
            full_name = cls_name
        else:
            full_name = f"{item_cls.__module__}.{item_cls.__name__}"

        conf: Dict[str, Any] = {
            "class_name": cls_name,
            "module_name": module_name,
            "full_name": full_name
        }
        result = PythonClass(**conf)
        result._cls_cache = item_cls
        return result


    class_name: str = Field(description="The name of the Python class.")
    module_name: str = Field(
        description="The name of the Python module this class lives in."
    )
    full_name: str = Field(description="The full class namespace.")

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
            m = self.get_module()
            self._cls_cache = getattr(m, self.class_name)
        return self._cls_cache

    def get_module(self) -> ModuleType:
        if self._module_cache is None:
            self._module_cache = importlib.import_module(self.module_name)
        return self._module_cache
