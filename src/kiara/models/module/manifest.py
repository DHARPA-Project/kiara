# -*- coding: utf-8 -*-
import orjson
import uuid
from deepdiff import DeepHash
from pydantic import Extra, Field, PrivateAttr, validator
from rich.console import RenderableType
from rich.syntax import Syntax
from typing import Any, Dict, Mapping, Optional

from kiara.defaults import (
    KIARA_HASH_FUNCTION,
    MODULE_CONFIG_CATEGORY_ID,
    NO_MODULE_TYPE,
    NONE_VALUE_ID,
)
from kiara.models import KiaraModel
from kiara.utils import orjson_dumps


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


class InputsManifest(Manifest):

    inputs: Mapping[str, uuid.UUID] = Field(
        description="A map of all the input fields and value references."
    )
    _inputs_hash: Optional[int] = PrivateAttr(default=None)
    _jobs_hash: Optional[int] = PrivateAttr(default=None)

    @property
    def jobs_hash(self) -> int:

        if self._jobs_hash is not None:
            return self._jobs_hash

        obj = {"manifest": self.manifest_hash, "inputs": self.inputs_hash}
        h = DeepHash(obj, hasher=KIARA_HASH_FUNCTION)
        self._jobs_hash = h[obj]
        return self._jobs_hash

    @validator("inputs")
    def replace_none_values(cls, value):
        result = {}
        for k, v in value.items():
            if v is None:
                v = NONE_VALUE_ID
            result[k] = v
        return result

    @property
    def inputs_hash(self) -> int:
        if self._inputs_hash is not None:
            return self._inputs_hash

        if self.module_type == NO_MODULE_TYPE and not self.inputs:
            self._inputs_hash = 0
        else:
            h = DeepHash(self.inputs, hasher=KIARA_HASH_FUNCTION)
            self._inputs_hash = h[self.inputs]
        return self._inputs_hash


class LoadConfig(Manifest):

    inputs: Dict[str, Any] = Field(
        description="The inputs to use to re-load the previously persisted value."
    )
    output_name: str = Field(
        description="The name of the field that contains the persisted value details."
    )

    def _retrieve_data_to_hash(self) -> Any:
        return self.dict()

    def __repr__(self):

        return f"{self.__class__.__name__}(module_type={self.module_type}, output_name={self.output_name})"

    def __str__(self):
        return self.__repr__()