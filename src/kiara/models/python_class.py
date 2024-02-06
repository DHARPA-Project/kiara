# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import importlib
import inspect
from types import ModuleType
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Type

from pydantic.fields import Field, PrivateAttr

from kiara.models import KiaraModel
from kiara.models.documentation import ContextMetadataModel
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import KiaraModule

if TYPE_CHECKING:
    pass


class PythonClass(KiaraModel):
    """Python class and module information."""

    _kiara_model_id: ClassVar = "instance.wrapped_python_class"

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

        result = PythonClass(**conf)
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
    _src_cache: str = PrivateAttr(default=None)

    def _retrieve_id(self) -> str:
        return self.full_name

    def _retrieve_data_to_hash(self) -> Any:
        return self.full_name

    def get_class(self) -> Type:

        if self._cls_cache is None:
            m = self.get_python_module()
            self._cls_cache = getattr(m, self.python_class_name)
        return self._cls_cache

    def get_source_code(self) -> str:

        if self._src_cache is None:
            self._src_cache = inspect.getsource(self.get_class())
        return self._src_cache

    def get_python_module(self) -> ModuleType:
        if self._module_cache is None:
            self._module_cache = importlib.import_module(self.python_module_name)
        return self._module_cache


class KiaraModuleInstance(PythonClass):

    _kiara_model_id: ClassVar[str] = "metadata.kiara_module_class"

    @classmethod
    def from_module(cls, module: "KiaraModule"):

        item_cls = module.__class__

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

        conf["module_config"] = module.config.model_dump()
        conf["inputs_schema"] = module.inputs_schema
        conf["outputs_schema"] = module.outputs_schema

        result = KiaraModuleInstance(**conf)
        result._cls_cache = item_cls
        result._module_instance_cache = module
        return result

    module_config: Dict[str, Any] = Field(description="The module config.")
    inputs_schema: Dict[str, ValueSchema] = Field(
        description="The schema for the module input(s)."
    )
    outputs_schema: Dict[str, ValueSchema] = Field(
        description="The schema for the module output(s)."
    )

    _module_instance_cache: "KiaraModule" = PrivateAttr(default=None)

    def get_kiara_module_instance(self) -> "KiaraModule":

        if self._module_instance_cache is not None:
            return self._module_instance_cache

        m_cls = self.get_class()
        self._module_instance_cache = m_cls(module_config=self.module_config)
        return self._module_instance_cache
