# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import structlog
import uuid
from bidict import bidict
from typing import TYPE_CHECKING, Any, Dict, Iterable, Mapping, Optional, Type

from kiara.models.events import KiaraEvent
from kiara.models.events.job_registry import (
    JobArchiveAddedEvent,
    JobRecordPreStoreEvent,
    JobRecordStoredEvent,
)
from kiara.models.module.jobs import ActiveJob, JobConfig, JobRecord, JobStatus
from kiara.models.module.manifest import InputsManifest, Manifest
from kiara.models.values.value import ValueMap
from kiara.processing import ModuleProcessor
from kiara.processing.synchronous import SynchronousProcessor
from kiara.registries import BaseArchive

if TYPE_CHECKING:
    from kiara.context import Kiara


logger = structlog.getLogger()

MANIFEST_SUB_PATH = "manifests"


class JobArchive(BaseArchive):
    @abc.abstractmethod
    def find_matching_job_record(
        self, inputs_manifest: InputsManifest
    ) -> Optional[JobRecord]:
        pass


class JobStore(JobArchive):
    @abc.abstractmethod
    def store_job_record(self, job_record: JobRecord):
        pass


class JobRegistry(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._active_jobs: bidict[int, uuid.UUID] = bidict()
        self._failed_jobs: Dict[int, uuid.UUID] = {}
        self._finished_jobs: Dict[int, uuid.UUID] = {}
        self._archived_records: Dict[uuid.UUID, JobRecord] = {}

        self._processor: ModuleProcessor = SynchronousProcessor(kiara=self._kiara)
        self._processor.register_job_status_listener(self)
        self._job_archives: Dict[str, JobArchive] = {}
        self._default_job_store: Optional[str] = None

        self._event_callback = self._kiara.event_registry.add_producer(self)

        # default_archive = FileSystemJobStore.create_from_kiara_context(self._kiara)
        # self.register_job_archive(default_archive, store_alias=DEFAULT_STORE_MARKER)

        # default_file_store = self._kiara.data_registry.get_archive(DEFAULT_STORE_MARKER)
        # self.register_job_archive(default_file_store, store_alias="default_data_store")  # type: ignore

    def suppoerted_event_types(self) -> Iterable[Type[KiaraEvent]]:

        return [JobArchiveAddedEvent, JobRecordPreStoreEvent, JobRecordStoredEvent]

    def register_job_archive(self, archive: JobArchive, alias: Optional[str] = None):

        if alias is None:
            alias = str(archive.archive_id)

        if alias in self._job_archives.keys():
            raise Exception(
                f"Can't register job store, store id already registered: {alias}."
            )

        self._job_archives[alias] = archive

        is_store = False
        is_default_store = False
        if isinstance(archive, JobStore):
            is_store = True
            if self._default_job_store is None:
                self._default_job_store = alias

        event = JobArchiveAddedEvent.construct(
            kiara_id=self._kiara.id,
            job_archive_id=archive.archive_id,
            job_archive_alias=alias,
            is_store=is_store,
            is_default_store=is_default_store,
        )
        self._event_callback(event)

    @property
    def default_job_store(self) -> str:

        if self._default_job_store is None:
            raise Exception("No default job store set (yet).")
        return self._default_job_store  # type: ignore

    def get_archive(self, store_id: Optional[str] = None) -> JobArchive:

        if store_id is None:
            store_id = self.default_job_store
            if store_id is None:
                raise Exception("Can't retrieve deafult job archive, none set (yet).")

        return self._job_archives[store_id]

    def job_status_changed(
        self, job_id: uuid.UUID, old_status: Optional[JobStatus], new_status: JobStatus
    ):

        # print(f"JOB STATUS CHANGED: {job_id} - {old_status} - {new_status.value}")
        if job_id in self._active_jobs.values() and new_status is JobStatus.FAILED:
            job_hash = self._active_jobs.inverse.pop(job_id)
            self._failed_jobs[job_hash] = job_id
        elif job_id in self._active_jobs.values() and new_status is JobStatus.SUCCESS:
            job_hash = self._active_jobs.inverse.pop(job_id)

            job_record = self._processor.get_job_record(job_id)

            self._finished_jobs[job_hash] = job_id
            self._archived_records[job_id] = job_record

    def store_job_record(self, job_id: uuid.UUID):

        if job_id not in self._archived_records.keys():
            raise Exception(
                f"Can't store job with id '{job_id}': no job record with that id exists."
            )

        job_record = self._archived_records[job_id]

        if job_record._is_stored:
            logger.debug(
                "ignore.store.job_record", reason="already stored", job_id=str(job_id)
            )
            return

        logger.debug(
            "store.job_record",
            job_hash=job_record.job_hash,
            module_type=job_record.module_type,
        )
        store: JobStore = self.get_archive()  # type: ignore
        if not isinstance(store, JobStore):
            raise Exception("Can't store job record to archive: not writable.")

        pre_store_event = JobRecordPreStoreEvent.construct(
            kiara_id=self._kiara.id, job_record=job_record
        )
        self._event_callback(pre_store_event)

        store.store_job_record(job_record)

        stored_event = JobRecordStoredEvent.construct(
            kiara_id=self._kiara.id, job_record=job_record
        )
        self._event_callback(stored_event)

    def get_job_record_in_session(self, job_id: uuid.UUID) -> JobRecord:

        return self._processor.get_job_record(job_id)

    def find_matching_job_record(
        self, inputs_manifest: InputsManifest
    ) -> Optional[uuid.UUID]:
        """Check if a job with same inputs manifest already ran some time before.

        Arguments:
            inputs_manifest: the manifest incl. inputs

        Returns:
            'None' if no such job exists, a (uuid) job-id if the job is currently running or has run in the past
        """

        if inputs_manifest.job_hash in self._active_jobs.keys():
            logger.debug("job.use_running")
            return self._active_jobs[inputs_manifest.job_hash]

        if inputs_manifest.job_hash in self._finished_jobs.keys():
            job_id = self._finished_jobs[inputs_manifest.job_hash]
            return job_id

        matches = []

        for store_id, archive in self._job_archives.items():
            match = archive.find_matching_job_record(inputs_manifest=inputs_manifest)
            if match:
                matches.append(match)

        if len(matches) == 0:
            return None
        elif len(matches) > 1:
            raise Exception(
                f"Multiple stores have a record for inputs manifest '{inputs_manifest}', this is not supported (yet)."
            )

        job_record = matches[0]

        self._finished_jobs[inputs_manifest.job_hash] = job_record.job_id
        self._archived_records[job_record.job_id] = job_record
        logger.debug("job.use_cached")
        return job_record.job_id

    def prepare_job_config(
        self, manifest: Manifest, inputs: Mapping[str, Any]
    ) -> JobConfig:

        module = self._kiara.create_module(manifest=manifest)
        job_config = JobConfig.create_from_module(
            data_registry=self._kiara.data_registry, module=module, inputs=inputs
        )

        return job_config

    def execute(
        self, manifest: Manifest, inputs: Mapping[str, Any], wait: bool = False
    ) -> uuid.UUID:

        job_config = self.prepare_job_config(manifest=manifest, inputs=inputs)
        return self.execute_job(job_config, wait=wait)

    def execute_job(self, job_config: JobConfig, wait: bool = False) -> uuid.UUID:

        log = logger.bind(
            module_type=job_config.module_type,
            module_config=job_config.module_config,
            inputs={k: str(v) for k, v in job_config.inputs.items()},
            job_hash=job_config.model_data_hash,
        )

        stored_job = self.find_matching_job_record(inputs_manifest=job_config)
        if stored_job is not None:
            return stored_job

        log.debug("job.execute")

        job_id = self._processor.create_job(job_config=job_config)
        self._active_jobs[job_config.job_hash] = job_id

        try:
            self._processor.queue_job(job_id=job_id)
        except Exception as e:
            log.error("error.queue_job", job_id=job_id)
            raise e

        if wait:
            self._processor.wait_for(job_id)

        return job_id

    def get_active_job(self, job_id: uuid.UUID) -> ActiveJob:

        if job_id in self._active_jobs.keys() or job_id in self._failed_jobs.keys():
            return self._processor.get_job(job_id)
        else:
            if job_id in self._archived_records.keys():
                raise Exception(
                    f"Can't retrieve active job with id '{job_id}': job is archived."
                )
            elif job_id in self._processor._failed_jobs.keys():
                job = self._processor.get_job(job_id)
                msg = job.error
                if not msg and job._exception:
                    msg = str(job._exception)
                    if not msg:
                        msg = repr(job._exception)
                raise Exception(f"Job failed: {msg}")
            else:
                raise Exception(f"Can't retrieve job with id '{job_id}': no such job.")

    def get_job_status(self, job_id: uuid.UUID) -> JobStatus:

        if job_id in self._archived_records.keys():
            return JobStatus.SUCCESS
        elif job_id in self._failed_jobs.values():
            return JobStatus.FAILED

        return self._processor.get_job_status(job_id=job_id)

    def wait_for(self, *job_id: uuid.UUID):
        not_finished = (j for j in job_id if j not in self._archived_records.keys())
        if not_finished:
            self._processor.wait_for(*not_finished)

    def retrieve_result(self, job_id: uuid.UUID) -> ValueMap:

        if job_id not in self._archived_records.keys():
            self._processor.wait_for(job_id)

        if job_id in self._archived_records.keys():
            job_record = self._archived_records[job_id]
            results = self._kiara.data_registry.load_values(job_record.outputs)
            return results
        elif job_id in self._failed_jobs.values():
            j = self._processor.get_job(job_id=job_id)
            raise Exception(f"Job failed: {j.error}")
        else:
            raise Exception(f"Could not find job with id: {job_id}")

    def execute_and_retrieve(
        self, manifest: Manifest, inputs: Mapping[str, Any]
    ) -> ValueMap:

        job_id = self.execute(manifest=manifest, inputs=inputs, wait=True)
        results = self.retrieve_result(job_id=job_id)
        return results
