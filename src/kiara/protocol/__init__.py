# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import abc
import orjson
import typing
import uuid
from pydantic import BaseModel, Field

from kiara.module_config import ModuleConfig
from kiara.processing import JobStatus
from kiara.utils import create_model, orjson_dumps


class KiaraMsg(BaseModel, abc.ABC):
    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps

    @classmethod
    def from_msg(cls, msg: typing.Tuple[bytes]) -> "KiaraMsg":
        msg_type, payload = msg
        msg_type = msg_type.decode()
        payload = orjson.loads(payload.decode())

        if msg_type == "processing_request":
            result = create_model(ProcessingRequestMsg, **payload)
        else:
            raise NotImplementedError()
        return result

    @abc.abstractmethod
    def to_msg(self) -> str:
        pass

    def create_msg(self) -> typing.Tuple[bytes]:
        msg = self.to_msg()
        result = (self.msg_type.encode(), orjson.dumps(msg))
        return result

    def to_msg(self) -> typing.Dict[str, typing.Any]:
        return self.dict()


class ProcessingRequestMsg(KiaraMsg):

    msg_type: typing.Literal["processing_request"]
    # payload_id: str = Field(description="The unique id of the payload")
    # payload_category: str = Field(description="An identifier of the payload category.")
    payload: ModuleConfig = Field(
        description="The type and configuration of the module to use in processing."
    )

    def to_msg(self) -> typing.Dict[str, typing.Any]:
        return self.dict()


class ResultStatus(BaseModel):

    value_id: uuid.UUID = Field(description="The id of the result value.")
    status: JobStatus = Field(
        description="The status of the processing job that is computing this value."
    )


class ProcessingStatus(BaseModel):

    status_id: uuid.UUID = Field(description="The id of the processing job.")
    status: JobStatus = Field(description="The status of the job.")
    result_ids: typing.Dict[str, ResultStatus] = Field(
        description="References to the result value ids."
    )


class ProcesssingStatusMsg(KiaraMsg):

    msg_type: typing.Literal["processing_request_status"]
    payload: ProcessingStatus = Field(
        description="Status information for a processing request."
    )
