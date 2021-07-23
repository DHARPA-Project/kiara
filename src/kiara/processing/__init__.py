# -*- coding: utf-8 -*-
import abc
import logging
import typing
import uuid
import zmq
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, BaseSettings, Field, validator
from zmq import Context, Socket

from kiara.data import PipelineValues, ValueSet
from kiara.exceptions import KiaraProcessingException
from kiara.utils import is_debug

try:
    from typing import Literal
except Exception:
    from typing_extensions import Literal  # type: ignore

if typing.TYPE_CHECKING:
    from kiara import KiaraModule


class ProcessorConfig(BaseSettings):

    module_processor_type: Literal["synchronous", "multi-threaded"] = "synchronous"


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

    log: typing.Dict[int, LogMessage] = Field(
        description="The logs for this job.", default_factory=dict
    )
    percent_finished: int = Field(
        description="Describes how much of the job is finished. A negative number means the module does not support progress tracking.",
        default=-1,
    )

    def add_log(self, msg: str, log_level: int = logging.DEBUG):

        _msg = LogMessage(msg=msg, log_level=log_level)
        self.log[len(self.log)] = _msg


class Job(BaseModel):
    @classmethod
    def create_event_msg(cls, job: "Job"):

        topic = job.status.value[2:-2]

        payload = f"{topic} {job.json()}"
        return payload

    class Config:
        use_enum_values = True

    id: str = Field(description="The id of the job.")
    pipeline_id: str = Field(description="The id of the pipeline this jobs runs for.")
    pipeline_name: str = Field(description="The name/type of the pipeline.")
    step_id: str = Field(description="The id of the step within the pipeline.")
    module_type: str = Field(description="The module type name.")
    module_config: typing.Dict[str, typing.Any] = Field(
        description="The module configuration."
    )
    inputs: PipelineValues = Field(description="The input values.")
    outputs: PipelineValues = Field(description="The output values.")
    job_log: JobLog = Field(
        description="Details about the job progress.", default_factory=JobLog
    )
    submitted: datetime = Field(
        description="When the job was submitted.", default_factory=datetime.now
    )
    started: typing.Optional[datetime] = Field(
        description="When the job was started.", default=None
    )
    finished: typing.Optional[datetime] = Field(
        description="When the job was finished.", default=None
    )
    status: JobStatus = Field(
        description="The current status of the job.",
        default=JobStatus.CREATED,
    )
    error: typing.Optional[str] = Field(description="Potential error message.")

    @validator("status")
    def _validate_status(cls, v):

        if isinstance(v, int):
            if v < 0 or v > 100:
                raise ValueError(
                    "Status must be a status string, or an integer between 0 and 100."
                )

        return v


class ModuleProcessor(abc.ABC):
    @classmethod
    def from_config(
        cls,
        config: typing.Union[None, typing.Mapping[str, typing.Any], ProcessorConfig],
    ) -> "ModuleProcessor":

        from kiara.processing.parallel import ThreadPoolProcessorConfig
        from kiara.processing.synchronous import SynchronousProcessorConfig

        if not config:
            config = SynchronousProcessorConfig(module_processor_type="synchronous")
        if isinstance(config, typing.Mapping):
            processor_type = config.get("module_processor_type", None)
            if not processor_type:
                raise ValueError("No 'module_processor_type' provided: {config}")
            if processor_type == "synchronous":
                config = SynchronousProcessorConfig(**config)
            elif processor_type == "multi-threaded":
                config = ThreadPoolProcessorConfig()
            else:
                raise ValueError(f"Invalid processor type: {processor_type}")

        if isinstance(config, SynchronousProcessorConfig):
            from kiara.processing.synchronous import SynchronousProcessor

            proc: ModuleProcessor = SynchronousProcessor(
                **config.dict(exclude={"module_processor_type"})
            )
        elif isinstance(config, ThreadPoolProcessorConfig):
            from kiara.processing.parallel import ThreadPoolProcessor

            proc = ThreadPoolProcessor(**config.dict(exclude={"module_processor_type"}))

        return proc

    def __init__(self, zmq_context: typing.Optional[Context] = None):

        if zmq_context is None:
            zmq_context = Context.instance()
        self._zmq_context: Context = zmq_context
        self._socket: Socket = self._zmq_context.socket(zmq.PUB)
        self._socket.connect("inproc://kiara_in")
        self._active_jobs: typing.Dict[str, Job] = {}
        self._finished_jobs: typing.Dict[str, Job] = {}

    def get_job_details(self, job_id: str) -> typing.Optional[Job]:

        if job_id in self._active_jobs.keys():
            return self._active_jobs[job_id]
        elif job_id in self._finished_jobs.keys():
            return self._finished_jobs[job_id]
        else:
            return None

    def start(
        self,
        pipeline_id: str,
        pipeline_name: str,
        step_id: str,
        module: "KiaraModule",
        inputs: ValueSet,
        outputs: ValueSet,
    ) -> str:

        job_id = str(uuid.uuid4())
        job = Job(
            id=job_id,
            pipeline_id=pipeline_id,
            pipeline_name=pipeline_name,
            step_id=step_id,
            module_type=module.type_name,
            module_config=module.config.dict(),
            inputs=PipelineValues.from_value_set(inputs),
            outputs=PipelineValues.from_value_set(outputs),
        )
        job.job_log.add_log("job created")
        self._active_jobs[job_id] = job
        self._socket.send_string(Job.create_event_msg(job))

        try:
            self.process(
                job_id=job_id,
                module=module,
                inputs=inputs,
                outputs=outputs,
                job_log=job.job_log,
            )
            return job_id
        except Exception as e:
            job.error = str(e)
            if is_debug():
                try:
                    import traceback

                    traceback.print_exc()
                except Exception:
                    pass
            if isinstance(e, KiaraProcessingException):
                e._module = module
                e._inputs = inputs
                raise e
            else:
                raise KiaraProcessingException(e, module=module, inputs=inputs)

    def job_status_updated(
        self, job_id: str, status: typing.Union[JobStatus, str, Exception]
    ):

        job = self._active_jobs.get(job_id, None)
        if job is None:
            raise Exception(
                f"Can't retrieve active job with id '{job_id}', no such job registered."
            )

        if status == JobStatus.SUCCESS:
            job.job_log.add_log("job finished successfully")
            job.status = JobStatus.SUCCESS
            job = self._active_jobs.pop(job_id)
            job.finished = datetime.now()
            self._finished_jobs[job_id] = job
            self._socket.send_string(Job.create_event_msg(job))
        elif status == JobStatus.FAILED or isinstance(status, (str, Exception)):
            job.job_log.add_log("job failed")
            job.status = JobStatus.FAILED
            job.finished = datetime.now()
            if isinstance(status, str):
                job.error = status
            elif isinstance(status, Exception):
                job.error = str(status)
            job = self._active_jobs.pop(job_id)
            self._finished_jobs[job_id] = job
            self._socket.send_string(Job.create_event_msg(job))
        elif status == JobStatus.STARTED:
            job.job_log.add_log("job started")
            job.status = JobStatus.STARTED
            self._socket.send_string(Job.create_event_msg(job))
        else:
            raise ValueError(f"Invalid value for status: {status}")

    @abc.abstractmethod
    def process(
        self,
        job_id: str,
        module: "KiaraModule",
        inputs: ValueSet,
        outputs: ValueSet,
        job_log: JobLog,
    ) -> str:
        pass

    @abc.abstractmethod
    def wait_for(self, *job_ids: str):

        pass
