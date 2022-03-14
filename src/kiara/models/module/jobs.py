# -*- coding: utf-8 -*-
import logging
import uuid
from datetime import datetime
from enum import Enum

from deepdiff import DeepHash
from pydantic.fields import Field, PrivateAttr
from pydantic.main import BaseModel
from typing import Any, Dict, MutableMapping, Mapping, Optional

from kiara.defaults import JOB_CONFIG_TYPE_CATEGORY_ID, JOB_RECORD_TYPE_CATEGORY_ID, KIARA_HASH_FUNCTION
from kiara.models.module.manifest import Manifest
from kiara.models.values.value import Value


class JobStatus(Enum):

    CREATED = "__job_created__"
    STARTED = "__job_started__"
    SUCCESS = "__job_success__"
    FAILED = "__job_failed__"


class LogMessage(BaseModel):

    timestamp: datetime = Field(
        description="The time the message was logged.", default_factory=datetime.now
    )
    log_level: int = Field(description="The log level.")
    msg: str = Field(description="The log message")


class JobLog(BaseModel):

    log: Dict[int, LogMessage] = Field(
        description="The logs for this job.", default_factory=dict
    )
    percent_finished: int = Field(
        description="Describes how much of the job is finished. A negative number means the module does not support progress tracking.",
        default=-1,
    )

    def add_log(self, msg: str, log_level: int = logging.DEBUG):

        _msg = LogMessage(msg=msg, log_level=log_level)
        self.log[len(self.log)] = _msg


class JobConfig(Manifest):

    inputs: Dict[str, Value] = Field(
        description="The inputs to use when running this module.", default_factory=dict
    )
    _inputs_hash: Optional[int] = PrivateAttr(default=None)

    def _retrieve_id(self) -> str:
        return str(self.model_data_hash)

    def _retrieve_category_id(self) -> str:
        return JOB_CONFIG_TYPE_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return {
            "module_config": self.manifest_data,
            "inputs": {k: v.value_id for k, v in self.inputs.items()}
        }

    @property
    def inputs_hash(self) -> int:

        if self._inputs_hash is not None:
            return self._inputs_hash

        obj = {k: v.value_id for k, v in self.inputs.items()}
        h = DeepHash(obj, hasher=KIARA_HASH_FUNCTION)
        self._inputs_hash = h[obj]
        return self._inputs_hash

class JobRecord(Manifest):

    @classmethod
    def from_manifest(cls, manifest: Manifest, inputs: Mapping[str, Value], outputs: Mapping[str, Value]):

        return JobRecord(module_type=manifest.module_type, module_config=manifest.module_config, inputs={k: v for k, v.value_id in inputs.items()}, outputs={k: v.value_id for k, v in outputs.items()})

    inputs: Dict[str, uuid.UUID] = Field(
        description="The inputs to use when running this module.", default_factory=dict
    )
    outputs: Dict[str, uuid.UUID] = Field(description="References to the job outputs.")
    _inputs_hash: Optional[int] = PrivateAttr(default=None)
    _outputs_hash: Optional[int] = PrivateAttr(default=None)

    def _retrieve_category_id(self) -> str:
        return JOB_RECORD_TYPE_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return {
            "manifest_hash": self.manifest_hash,
            "inputs": self.inputs,
            "outputs": self.outputs
            }

    @property
    def inputs_hash(self) -> int:

        if self._inputs_hash is not None:
            return self._inputs_hash

        obj = self.inputs
        h = DeepHash(obj, hasher=KIARA_HASH_FUNCTION)
        self._inputs_hash = h[obj]
        return self._inputs_hash

    @property
    def outputs_hash(self) -> int:

        if self._outputs_hash is not None:
            return self._outputs_hash

        obj = self.outputs
        h = DeepHash(obj, hasher=KIARA_HASH_FUNCTION)
        self._outputs_hash = h[obj]
        return self._outputs_hash

class DeserializeConfig(JobConfig):

    output_name: str = Field(description="The name of the output field for the value.")
