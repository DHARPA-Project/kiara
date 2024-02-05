# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from typing import TYPE_CHECKING, Any, ClassVar, Dict, Mapping, Union

from pydantic import ConfigDict, PrivateAttr
from pydantic.fields import Field
from pydantic.main import BaseModel
from pydantic_core import PydanticUndefined
from rich import box
from rich.console import RenderableType
from rich.table import Table

from kiara.models import KiaraModel

if TYPE_CHECKING:
    pass


class KiaraModuleConfig(KiaraModel):
    """
    Base class that describes the configuration a [``KiaraModule``][kiara.module.KiaraModule] class accepts.

    This is stored in the ``_config_cls`` class attribute in each ``KiaraModule`` class.

    There are two config options every ``KiaraModule`` supports:

     - ``constants``, and
     - ``defaults``

     Constants are pre-set inputs, and users can't change them and an error is thrown if they try. Defaults are default
     values that override the schema defaults, and those can be overwritten by users. If both a constant and a default
     value is set for an input field, an error is thrown.
    """

    _kiara_model_id: ClassVar = "instance.module_config"

    @classmethod
    def requires_config(cls, config: Union[Mapping[str, Any], None] = None) -> bool:
        """Return whether this class can be used as-is, or requires configuration before an instance can be created."""

        for field_name, field in cls.model_fields.items():

            if not field.is_required():
                continue

            if (
                field.default in [None, PydanticUndefined]
                and field.default_factory is None
            ):
                if config:
                    if config.get(field_name, None) is None:
                        return True
                else:
                    return True
        return False

    _config_hash: str = PrivateAttr(default=None)
    constants: Dict[str, Any] = Field(
        default_factory=dict, description="Value constants for this module."
    )
    defaults: Dict[str, Any] = Field(
        default_factory=dict, description="Value defaults for this module."
    )
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    def get(self, key: str) -> Any:
        """Get the value for the specified configuation key."""
        if key not in self.model_fields:
            raise Exception(
                f"No config value '{key}' in module config class '{self.__class__.__name__}'."
            )

        return getattr(self, key)

    def create_renderable(self, **config: Any) -> RenderableType:

        my_table = Table(box=box.MINIMAL, show_header=False)
        my_table.add_column("Field name", style="i")
        my_table.add_column("Value")
        for field in self.model_fields:
            attr = getattr(self, field)
            if isinstance(attr, str):
                attr_str = attr
            elif hasattr(attr, "create_renderable"):
                attr_str = attr.create_renderable()
            elif isinstance(attr, BaseModel):
                attr_str = attr.model_dump_json(indent=2)
            else:
                attr_str = str(attr)
            my_table.add_row(field, attr_str)

        return my_table


# def calculate_class_doc_url(base_url: str, module_type_name: str):
#
#     if base_url.endswith("/"):
#         base_url = base_url[0:-1]
#
#     module_type_name = module_type_name.replace(".", "")
#     url = f"{base_url}/latest/modules_list/#{module_type_name}"
#
#     return url


# def calculate_class_source_url(
#     base_url: str, python_class_info: PythonClass, branch: str = "main"
# ):
#
#     if base_url.endswith("/"):
#         base_url = base_url[0:-1]
#
#     m = python_class_info.get_python_module()
#     m_file = m.__file__
#     assert m_file is not None
#
#     base_url = f"{base_url}/blob/{branch}/src/{python_class_info.python_module_name.replace('.', '/')}"
#
#     if m_file.endswith("__init__.py"):
#         url = f"{base_url}/__init__.py"
#     else:
#         url = f"{base_url}.py"
#
#     return url
