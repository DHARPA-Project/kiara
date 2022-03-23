# -*- coding: utf-8 -*-
import orjson
from deepdiff import DeepHash
from pydantic import Extra, Field, PrivateAttr
from rich import box
from rich.console import RenderableType
from rich.syntax import Syntax
from rich.table import Table
from typing import Any, Dict, Mapping, Optional, TYPE_CHECKING

from kiara.defaults import MODULE_CONFIG_CATEGORY_ID, MODULE_CONFIG_SCHEMA_CATEGORY_ID, KIARA_HASH_FUNCTION, \
    NO_MODULE_TYPE
from kiara.models import KiaraModel
from kiara.utils import orjson_dumps

if TYPE_CHECKING:
    pass


class KiaraModuleConfig(KiaraModel):
    """Base class that describes the configuration a [``KiaraModule``][kiara.module.KiaraModule] class accepts.

    This is stored in the ``_config_cls`` class attribute in each ``KiaraModule`` class.

    There are two config options every ``KiaraModule`` supports:

     - ``constants``, and
     - ``defaults``

     Constants are pre-set inputs, and users can't change them and an error is thrown if they try. Defaults are default
     values that override the schema defaults, and those can be overwritten by users. If both a constant and a default
     value is set for an input field, an error is thrown.
    """

    @classmethod
    def requires_config(cls, config: Optional[Mapping[str, Any]] = None) -> bool:
        """Return whether this class can be used as-is, or requires configuration before an instance can be created."""

        for field_name, field in cls.__fields__.items():
            if field.required and field.default is None:
                if config:
                    if config.get(field_name, None) is None:
                        return True
                else:
                    return True
        return False

    _config_hash: str = PrivateAttr(default=None)
    constants: Dict[str, Any] = Field(
        default_factory=dict, description="ValueOrm constants for this module."
    )
    defaults: Dict[str, Any] = Field(
        default_factory=dict, description="ValueOrm defaults for this module."
    )

    class Config:
        extra = Extra.forbid
        validate_assignment = True

    def get(self, key: str) -> Any:
        """Get the value for the specified configuation key."""

        if key not in self.__fields__:
            raise Exception(
                f"No config value '{key}' in module config class '{self.__class__.__name__}'."
            )

        return getattr(self, key)

    def _retrieve_id(self) -> int:
        return self.model_data_hash

    def _retrieve_category_id(self) -> str:
        return MODULE_CONFIG_SCHEMA_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:

        return self.dict()

    def create_renderable(self, **config: Any) -> RenderableType:

        my_table = Table(box=box.MINIMAL, show_header=False)
        my_table.add_column("Field name", style="i")
        my_table.add_column("ValueOrm")
        for field in self.__fields__:
            my_table.add_row(field, getattr(self, field))

        return my_table

    def __eq__(self, other):

        if self.__class__ != other.__class__:
            return False

        return self.model_data_hash == other.model_data_hash

    def __hash__(self):

        return self.model_data_hash


class Manifest(KiaraModel):
    """A class to hold the type and configuration for a module instance."""

    class Config:
        extra = Extra.forbid
        validate_all = True

    _manifest_data: Optional[Dict[str, Any]] = PrivateAttr(default=None)
    _manifest_hash: Optional[int] = PrivateAttr(default=None)

    module_type: str = Field(description="The module type.")
    module_config: Dict[str, Any] = Field(
        default_factory=dict, description="The configuration for the module."
    )
    # python_class: PythonClass = Field(description="The python class that implements this module.")
    # doc: DocumentationMetadataModel = Field(
    #     description="Documentation for this module instance.", default=None
    # )

    @property
    def manifest_data(self):
        """The configuration data for this module instance."""
        if self._manifest_data is not None:
            return self._manifest_data

        self._manifest_data = {
            "module_type": self.module_type,
            "module_config": self.module_config,
        }
        return self._manifest_data

    def manifest_data_as_json(self):

        return self.json(include={"module_type", "module_config"})

    @property
    def manifest_hash(self) -> int:
        """The hash for the inherent module config (composted of type and render_config data).

        Not that this can (but might not) be different to the `model_data_hash`.
        """

        if self._manifest_hash is not None:
            return self._manifest_hash

        h = DeepHash(self.manifest_data, hasher=KIARA_HASH_FUNCTION)
        self._manifest_hash = h[self.manifest_data]
        return self._manifest_hash

    def _retrieve_data_to_hash(self) -> Any:
        return self.manifest_data

    def _retrieve_id(self) -> str:
        return str(self.model_data_hash)

    def _retrieve_category_id(self) -> str:
        return MODULE_CONFIG_CATEGORY_ID

    def create_renderable(self, **config: Any) -> RenderableType:
        """Create a renderable for this module configuration."""

        data = self.dict(exclude_none=True)
        conf = Syntax(
            orjson_dumps(data, option=orjson.OPT_INDENT_2),
            "json",
            background_color="default",
        )
        return conf

    def __repr__(self):

        return f"{self.__class__.__name__}(module_type={self.module_type}, module_config={self.module_config})"

    def __str__(self):

        return self.__repr__()


class LoadConfig(Manifest):

    inputs: Dict[str, Any] = Field(description="The inputs to use to re-load the previously persisted value.")
    output_name: str = Field(description="The name of the field that contains the persisted value details.")

    def _retrieve_data_to_hash(self) -> Any:
        return self.dict()

    def __repr__(self):

        return f"{self.__class__.__name__}(module_type={self.module_type}, output_name={self.output_name})"

    def __str__(self):
        return self.__repr__()
