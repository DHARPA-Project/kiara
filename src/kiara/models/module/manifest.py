# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import uuid
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Mapping, Tuple, Union

import orjson
from dag_cbor import IPLDKind
from multiformats import CID
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator
from rich.console import RenderableType
from rich.syntax import Syntax
from rich.table import Table

from kiara.defaults import INVALID_HASH_MARKER, NONE_VALUE_ID
from kiara.exceptions import KiaraException
from kiara.models import KiaraModel
from kiara.utils.develop import log_dev_message
from kiara.utils.hashing import compute_cid
from kiara.utils.json import orjson_dumps
from kiara.utils.pipelines import extract_data_to_hash_from_pipeline_config

if TYPE_CHECKING:
    from kiara.registries.data import DataRegistry


class Manifest(KiaraModel):
    """A class to hold the type and configuration for a module instance."""

    _kiara_model_id: ClassVar = "instance.manifest"
    model_config = ConfigDict(extra="forbid", validate_default=True)

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

    @field_validator("module_config")
    @classmethod
    def validate_module_config(cls, value):
        if isinstance(value, BaseModel):
            raise ValueError(f"Invalid module config type: {type(value)}")

        return value

    @property
    def manifest_data(self):
        """The configuration data for this module instance."""
        if self._manifest_data is not None:
            return self._manifest_data

        mc = extract_data_to_hash_from_pipeline_config(self.module_config)

        self._manifest_data = {
            "module_type": self.module_type,
            "module_config": mc,
        }
        return self._manifest_data

    @property
    def manifest_cid(self) -> CID:

        if self._manifest_cid is not None:
            return self._manifest_cid

        if not self.is_resolved:

            msg = "Cannot calculate manifest CID for unresolved manifest."
            item = Syntax(
                self.model_dump_json(indent=2),
                "json",
                background_color="default",
            )
            table = Table(show_header=False)
            table.add_column("key")
            table.add_column("value")

            table.add_row("", msg)
            table.add_row()
            table.add_row("type", str(type(self)))
            table.add_row()
            table.add_row("manifest", item)

            log_dev_message(table, title="cid computation error")

            raise KiaraException(msg=msg)

        _, self._manifest_cid = compute_cid(self.manifest_data)
        return self._manifest_cid

    @property
    def manifest_hash(self) -> str:
        return str(self.manifest_cid)

    def manifest_data_as_json(self):

        return self.model_dump_json(include={"module_type", "module_config"})

    def _retrieve_data_to_hash(self) -> Any:

        return self.manifest_data

    def create_renderable(self, **config: Any) -> RenderableType:
        """Create a renderable for this module configuration."""
        data = self.model_dump(exclude_none=True)
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

    _kiara_model_id: ClassVar = "instance.manifest_with_inputs"

    inputs: Mapping[str, uuid.UUID] = Field(
        description="A map of all the input fields and value references."
    )
    _inputs_cid: Union[CID, None] = PrivateAttr(default=None)
    # _inputs_hash: Union[str, None] = PrivateAttr(default=None)
    _jobs_cid: Union[CID, None] = PrivateAttr(default=None)
    _inputs_data_cid: Union[CID, None] = PrivateAttr(default=None)
    _input_data_contains_invalid: Union[bool, None] = PrivateAttr(default=None)

    @field_validator("inputs")
    @classmethod
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

        obj: IPLDKind = {"manifest": self.manifest_cid, "inputs": self.inputs_cid}
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
    def input_ids_hash(self) -> str:

        return str(self.inputs_cid)

    def calculate_inputs_data_cid(
        self, data_registry: "DataRegistry"
    ) -> Tuple[CID, bool]:
        """Calculates the cid of the data hashes contained in this inputs manifest.

        This returns two values in a tuple: the first value is the cid where 'invalid hash markes' (used when a value is  not set) is set to 'None', the second one indicates whether such an
        invalid hash marker was encountered.

        This might be important to know, because if the interface of the module in question changed (which is possible for those types of fields), the computed input might not be valid anymore and would need to be re-computed.
        """

        if self._inputs_data_cid is not None:
            return (self._inputs_data_cid, self._input_data_contains_invalid)  # type: ignore

        data_hashes: Dict[str, Any] = {}
        invalid = False

        for k, v in self.inputs.items():
            value = data_registry.get_value(v)
            if value.value_hash == INVALID_HASH_MARKER:
                invalid = True
                data_hashes[k] = None
            else:
                data_hashes[k] = CID.decode(value.value_hash)

        _, cid = compute_cid(data=data_hashes)
        self._input_data_contains_invalid = invalid
        self._inputs_data_cid = cid
        return (cid, invalid)
