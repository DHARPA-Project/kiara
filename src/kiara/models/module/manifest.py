# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import orjson
import uuid
from multiformats import CID
from pydantic import Extra, Field, PrivateAttr, validator
from rich.console import RenderableType
from rich.syntax import Syntax
from typing import TYPE_CHECKING, Any, Mapping, Union

from kiara.defaults import INVALID_HASH_MARKER, NONE_VALUE_ID
from kiara.models import KiaraModel
from kiara.utils.hashing import compute_cid
from kiara.utils.json import orjson_dumps

if TYPE_CHECKING:
    from kiara.registries.data import DataRegistry


class Manifest(KiaraModel):
    """A class to hold the type and configuration for a module instance."""

    _kiara_model_id = "instance.manifest"

    class Config:
        extra = Extra.forbid
        validate_all = True

    _manifest_data: Union[Mapping[str, Any], None] = PrivateAttr(default=None)
    _manifest_cid: Union[CID, None] = PrivateAttr(default=None)

    module_type: str = Field(description="The module type.")
    module_config: Mapping[str, Any] = Field(
        default_factory=dict, description="The configuration for the module."
    )
    is_resolved: bool = Field(
        description="Whether the configuration of this module was augmented with the module type defaults etc.",
        default=False,
    )
    # python_class: PythonClass = Field(description="The python class that implements this module.")
    # doc: DocumentationMetadataModel = Field(
    #     description="Documentation for this module instance.", default=None
    # )

    # @validator("module_config")
    # def _validate_module_config(cls, value):
    #
    #     return value

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

    @property
    def manifest_cid(self) -> CID:
        if self._manifest_cid is not None:
            return self._manifest_cid

        _, self._manifest_cid = compute_cid(self.manifest_data)
        return self._manifest_cid

    @property
    def manifest_hash(self) -> str:
        return str(self.manifest_cid)

    def manifest_data_as_json(self):

        return self.json(include={"module_type", "module_config"})

    def _retrieve_data_to_hash(self) -> Any:
        return self.manifest_data

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

    _kiara_model_id = "instance.manifest_with_inputs"

    inputs: Mapping[str, uuid.UUID] = Field(
        description="A map of all the input fields and value references."
    )
    _inputs_cid: Union[CID, None] = PrivateAttr(default=None)
    _jobs_cid: Union[CID, None] = PrivateAttr(default=None)
    _inputs_data_cid: Union[bool, CID, None] = PrivateAttr(default=None)

    @validator("inputs")
    def replace_none_values(cls, value):
        result = {}
        for k, v in value.items():
            if v is None:
                v = NONE_VALUE_ID
            result[k] = v
        return result

    @property
    def job_hash(self) -> str:

        return str(self.job_cid)

    @property
    def job_cid(self) -> CID:

        if self._jobs_cid is not None:
            return self._jobs_cid

        obj = {"manifest": self.manifest_cid, "inputs": self.inputs_cid}
        _, self._jobs_cid = compute_cid(data=obj)
        return self._jobs_cid

    @property
    def inputs_cid(self) -> CID:
        if self._inputs_cid is not None:
            return self._inputs_cid

        _, cid = compute_cid(data={k: v.bytes for k, v in self.inputs.items()})
        self._inputs_cid = cid
        return self._inputs_cid

    @property
    def inputs_hash(self) -> str:
        return str(self.inputs_cid)

    def calculate_inputs_data_cid(
        self, data_registry: "DataRegistry"
    ) -> Union[CID, None]:

        if self._inputs_data_cid is not None:
            if self._inputs_data_cid is False:
                return None
            return self._inputs_data_cid  # type: ignore

        data_hashes = {}
        invalid = False

        for k, v in self.inputs.items():
            value = data_registry.get_value(v)
            if value.value_hash == INVALID_HASH_MARKER:
                invalid = True
                break
            data_hashes[k] = CID.decode(value.value_hash)

        if invalid:
            self._inputs_data_cid = False
            return None

        _, cid = compute_cid(data=data_hashes)
        self._inputs_data_cid = cid
        return cid
