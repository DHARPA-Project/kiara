# -*- coding: utf-8 -*-
import abc
import typing
import uuid
import zmq
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field, validator
from zmq import Context, Socket

from kiara.data import PipelineValues, ValueSet
from kiara.exceptions import KiaraProcessingException
from kiara.utils import is_debug

if typing.TYPE_CHECKING:
    from kiara import KiaraModule


class JobStatus(Enum):

    CREATED = "__job_created__"
    STARTED = "__job_started__"
    SUCCESS = "__job_success__"
    FAILURE = "__job_failed__"


class Job(BaseModel):
    @classmethod
    def create_event_msg(cls, job: "Job"):

        if isinstance(job.status, int):
            topic = "job_status_updated"
        else:
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
    submitted: datetime = Field(
        description="When the job was submitted.", default_factory=datetime.now
    )
    started: typing.Optional[datetime] = Field(
        description="When the job was started.", default=None
    )
    finished: typing.Optional[datetime] = Field(
        description="When the job was finished.", default=None
    )
    status: typing.Union[JobStatus, int] = Field(
        description="The current status of the job, either a job status string, or a 'percent finished' integer.",
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
    def __init__(self, zmq_context: typing.Optional[Context] = None):

        if zmq_context is None:
            zmq_context = Context.instance()
        self._zmq_context: Context = zmq_context
        self._socket: Socket = self._zmq_context.socket(zmq.PUB)
        self._socket.connect("inproc://kiara_in")
        self._active_jobs: typing.Dict[str, Job] = {}
        self._finished_jobs: typing.Dict[str, Job] = {}

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
        self._active_jobs[job_id] = job
        self._socket.send_string(Job.create_event_msg(job))

        try:
            self.process(job_id=job_id, module=module, inputs=inputs, outputs=outputs)
            return job_id
        except Exception as e:
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
        self, job_id: str, status: typing.Union[JobStatus, int, str, Exception]
    ):

        job = self._active_jobs.get(job_id, None)
        if job is None:
            raise Exception(
                f"Can't retrieve active job with id '{job_id}', no such job registered."
            )

        if status in [100, JobStatus.SUCCESS]:
            job.status = JobStatus.SUCCESS
            job = self._active_jobs.pop(job_id)
            job.finished = datetime.now()
            self._finished_jobs[job_id] = job
            self._socket.send_string(Job.create_event_msg(job))
        elif status == JobStatus.FAILURE or isinstance(status, (str, Exception)):
            job.status = JobStatus.FAILURE
            job.finished = datetime.now()
            if isinstance(status, str):
                job.error = status
            elif isinstance(status, Exception):
                job.error = str(status)
            job = self._active_jobs.pop(job_id)
            self._finished_jobs[job_id] = job
            self._socket.send_string(Job.create_event_msg(job))
        elif status in [0, JobStatus.STARTED]:
            job.status = JobStatus.STARTED
            self._socket.send_string(Job.create_event_msg(job))
        elif isinstance(status, int) and status > 1 and status < 100:
            job.status = status
            self._socket.send_string(Job.create_event_msg(job))
        else:
            raise ValueError(f"Invalid value for status: {status}")

    @abc.abstractmethod
    def process(
        self, job_id: str, module: "KiaraModule", inputs: ValueSet, outputs: ValueSet
    ) -> str:
        pass

    @abc.abstractmethod
    def wait_for(self, *job_ids: str):

        pass
