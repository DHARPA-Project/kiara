# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import structlog
import uuid
from bidict import bidict
from typing import TYPE_CHECKING, Any, Dict, Iterable, Mapping, Type, Union

from kiara.context.config import JobCacheStrategy
from kiara.exceptions import FailedJobException
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
from kiara.utils import get_dev_config, is_develop

if TYPE_CHECKING:
    from kiara.context import Kiara


logger = structlog.getLogger()

MANIFEST_SUB_PATH = "manifests"


class JobArchive(BaseArchive):
    # @abc.abstractmethod
    # def find_matching_job_record(
    #     self, inputs_manifest: InputsManifest
    # ) -> Optional[JobRecord]:
    #     pass

    @abc.abstractmethod
    def retrieve_all_job_hashes(
        self,
        manifest_hash: Union[str, None] = None,
        inputs_hash: Union[str, None] = None,
    ) -> Iterable[str]:
        """Retrieve a list of all job record hashes (cids) that match the given filter arguments.

        A job record hash includes information about the module type used in the job, the module configuration, as well as input field names and value ids for the values used in those inputs.

        If the job archive retrieves its jobs in a dynamic way, this will return 'None'.
        """

    @abc.abstractmethod
    def _retrieve_record_for_job_hash(self, job_hash: str) -> Union[JobRecord, None]:
        pass

    def retrieve_record_for_job_hash(self, job_hash: str) -> Union[JobRecord, None]:

        job_record = self._retrieve_record_for_job_hash(job_hash=job_hash)
        return job_record


class JobStore(JobArchive):
    @abc.abstractmethod
    def store_job_record(self, job_record: JobRecord):
        pass


class JobMatcher(abc.ABC):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara

    @abc.abstractmethod
    def find_existing_job(
        self, inputs_manifest: InputsManifest
    ) -> Union[JobRecord, None]:
        pass


class NoneJobMatcher(JobMatcher):
    def find_existing_job(
        self, inputs_manifest: InputsManifest
    ) -> Union[JobRecord, None]:
        return None


class ValueIdJobMatcher(JobMatcher):
    def find_existing_job(
        self, inputs_manifest: InputsManifest
    ) -> Union[JobRecord, None]:

        matches = []

        for store_id, archive in self._kiara.job_registry.job_archives.items():

            match = archive.retrieve_record_for_job_hash(
                job_hash=inputs_manifest.job_hash
            )
            if match:
                matches.append(match)

        if len(matches) == 0:
            return None
        elif len(matches) > 1:
            raise Exception(
                f"Multiple stores have a record for inputs manifest '{inputs_manifest}', this is not supported (yet)."
            )

        job_record = matches[0]
        job_record._is_stored = True

        return job_record


class DataHashJobMatcher(JobMatcher):
    def find_existing_job(
        self, inputs_manifest: InputsManifest
    ) -> Union[JobRecord, None]:

        matches = []

        for store_id, archive in self._kiara.job_registry.job_archives.items():

            match = archive.retrieve_record_for_job_hash(
                job_hash=inputs_manifest.job_hash
            )
            if match:
                matches.append(match)

        if len(matches) > 1:
            raise Exception(
                f"Multiple stores have a record for inputs manifest '{inputs_manifest}', this is not supported (yet)."
            )

        elif len(matches) == 1:

            job_record = matches[0]
            job_record._is_stored = True

            return job_record

        inputs_data_cid = inputs_manifest.calculate_inputs_data_cid(
            data_registry=self._kiara.data_registry
        )
        if not inputs_data_cid:
            return None

        inputs_data_hash = str(inputs_data_cid)

        matching_records = []
        for store_id, archive in self._kiara.job_registry.job_archives.items():
            _matches = archive.retrieve_all_job_hashes(
                manifest_hash=inputs_manifest.manifest_hash
            )
            for _match in _matches:
                _job_record = archive.retrieve_record_for_job_hash(_match)
                assert _job_record is not None
                if _job_record.inputs_data_hash == inputs_data_hash:
                    matching_records.append(_job_record)

        if not matching_records:
            return None
        elif len(matches) > 1:
            raise Exception(
                f"Multiple stores have a record for inputs manifest '{inputs_manifest}', this is not supported (yet)."
            )
        else:
            return matching_records[0]


class JobRegistry(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara

        self._job_matcher_cache: Dict[JobCacheStrategy, JobMatcher] = {}

        self._active_jobs: bidict[str, uuid.UUID] = bidict()
        self._failed_jobs: Dict[str, uuid.UUID] = {}
        self._finished_jobs: Dict[str, uuid.UUID] = {}
        self._archived_records: Dict[uuid.UUID, JobRecord] = {}

        self._processor: ModuleProcessor = SynchronousProcessor(kiara=self._kiara)
        self._processor.register_job_status_listener(self)
        self._job_archives: Dict[str, JobArchive] = {}
        self._default_job_store: Union[str, None] = None

        self._event_callback = self._kiara.event_registry.add_producer(self)

        # default_archive = FileSystemJobStore.create_from_kiara_context(self._kiara)
        # self.register_job_archive(default_archive, store_alias=DEFAULT_STORE_MARKER)

        # default_file_store = self._kiara.data_registry.get_archive(DEFAULT_STORE_MARKER)
        # self.register_job_archive(default_file_store, store_alias="default_data_store")  # type: ignore

    @property
    def job_matcher(self) -> JobMatcher:

        strategy = self._kiara.runtime_config.job_cache
        if is_develop():
            dev_config = get_dev_config()
            if not dev_config.job_cache:
                logger.debug(
                    "disable.job_cache",
                    reason="dev mode enabled and 'disable_job_cache' is set.",
                )
                strategy = JobCacheStrategy.no_cache

        job_matcher = self._job_matcher_cache.get(strategy, None)
        if job_matcher is None:
            if strategy == JobCacheStrategy.no_cache:
                job_matcher = NoneJobMatcher(kiara=self._kiara)
            elif strategy == JobCacheStrategy.value_id:
                job_matcher = ValueIdJobMatcher(kiara=self._kiara)
            elif strategy == JobCacheStrategy.data_hash:
                job_matcher = DataHashJobMatcher(kiara=self._kiara)
            else:
                raise Exception(f"Job cache strategy not implemented: {strategy}")
            self._job_matcher_cache[strategy] = job_matcher

        return job_matcher

    def suppoerted_event_types(self) -> Iterable[Type[KiaraEvent]]:

        return [JobArchiveAddedEvent, JobRecordPreStoreEvent, JobRecordStoredEvent]

    def register_job_archive(self, archive: JobArchive, alias: Union[str, None] = None):

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

    def get_archive(self, store_id: Union[str, None] = None) -> JobArchive:

        if store_id is None:
            store_id = self.default_job_store
            if store_id is None:
                raise Exception("Can't retrieve deafult job archive, none set (yet).")

        return self._job_archives[store_id]

    @property
    def job_archives(self) -> Mapping[str, JobArchive]:
        return self._job_archives

    def job_status_changed(
        self,
        job_id: uuid.UUID,
        old_status: Union[JobStatus, None],
        new_status: JobStatus,
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

        store: JobStore = self.get_archive()  # type: ignore
        if not isinstance(store, JobStore):
            raise Exception("Can't store job record to archive: not writable.")

        # if job_record.job_id in self._finished_jobs.values():
        #     logger.debug(
        #         "ignore.store.job_record",
        #         reason="already stored in store",
        #         job_id=str(job_id),
        #     )
        #     return

        logger.debug(
            "store.job_record",
            job_hash=job_record.job_hash,
            module_type=job_record.module_type,
        )

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

    def get_job_record(self, job_id: uuid.UUID) -> Union[JobRecord, None]:

        if job_id in self._archived_records.keys():
            return self._archived_records[job_id]

        try:
            job_record = self._processor.get_job_record(job_id=job_id)
            return job_record
        except Exception:
            pass

        job = self._processor.get_job(job_id=job_id)
        if job is not None:
            if job.status == JobStatus.FAILED:
                return None

        raise NotImplementedError()

    def retrieve_all_job_records(self) -> Mapping[str, JobRecord]:

        all_records: Dict[str, JobRecord] = {}
        for archive in self.job_archives.values():
            all_record_ids = archive.retrieve_all_job_hashes()
            if all_record_ids is None:
                return {}
            for r in all_record_ids:
                assert r not in all_records.keys()
                job_record = archive.retrieve_record_for_job_hash(r)
                assert job_record is not None
                all_records[r] = job_record

        return all_records

    def find_matching_job_record(
        self, inputs_manifest: InputsManifest
    ) -> Union[uuid.UUID, None]:
        """Check if a job with same inputs manifest already ran some time before.

        Arguments:
            inputs_manifest: the manifest incl. inputs

        Returns:
            'None' if no such job exists, a (uuid) job-id if the job is currently running or has run in the past
        """

        log = logger.bind(module_type=inputs_manifest.module_type)
        if inputs_manifest.job_hash in self._active_jobs.keys():
            log.debug("job.use_running")
            return self._active_jobs[inputs_manifest.job_hash]

        if inputs_manifest.job_hash in self._finished_jobs.keys():
            job_id = self._finished_jobs[inputs_manifest.job_hash]
            return job_id

        module = self._kiara.module_registry.create_module(manifest=inputs_manifest)
        if not module.characteristics.is_idempotent:
            log.debug(
                "skip.job_matching",
                reason="module is not idempotent",
                module_type=inputs_manifest.module_type,
            )
            return None

        job_record = self.job_matcher.find_existing_job(inputs_manifest=inputs_manifest)
        if job_record is None:
            return None

        self._finished_jobs[inputs_manifest.job_hash] = job_record.job_id
        self._archived_records[job_record.job_id] = job_record
        log.debug(
            "job.found_cached_record",
            job_id=str(job_record.job_id),
            job_hash=inputs_manifest.job_hash,
            module_type=inputs_manifest.module_type,
        )
        return job_record.job_id

    def prepare_job_config(
        self, manifest: Manifest, inputs: Mapping[str, Any]
    ) -> JobConfig:

        module = self._kiara.module_registry.create_module(manifest=manifest)

        job_config = JobConfig.create_from_module(
            data_registry=self._kiara.data_registry, module=module, inputs=inputs
        )

        return job_config

    def execute(
        self,
        manifest: Manifest,
        inputs: Mapping[str, Any],
        wait: bool = False,
        job_metadata: Union[None, Any] = None,
    ) -> uuid.UUID:

        job_config = self.prepare_job_config(manifest=manifest, inputs=inputs)
        return self.execute_job(job_config, wait=wait, job_metadata=job_metadata)

    def execute_job(
        self,
        job_config: JobConfig,
        wait: bool = False,
        job_metadata: Union[None, Any] = None,
    ) -> uuid.UUID:

        log = logger.bind(
            module_type=job_config.module_type,
            module_config=job_config.module_config,
            inputs={k: str(v) for k, v in job_config.inputs.items()},
            job_hash=job_config.job_hash,
        )

        stored_job = self.find_matching_job_record(inputs_manifest=job_config)
        if stored_job is not None:
            log.debug(
                "job.use_cached",
                job_id=str(stored_job),
                module_type=job_config.module_type,
            )
            return stored_job

        if job_metadata is None:
            job_metadata = {}

        is_pipeline_step = job_metadata.get("is_pipeline_step", False)
        dbg_data = {
            "module_type": job_config.module_type,
            "is_pipeline_step": is_pipeline_step,
        }
        if is_pipeline_step:
            dbg_data["step_id"] = job_metadata["step_id"]
        log.debug("job.execute", **dbg_data)

        job_id = self._processor.create_job(
            job_config=job_config, job_metadata=job_metadata
        )
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
                raise FailedJobException(job=job, msg=msg, parent=job._exception)
            else:
                raise Exception(f"Can't retrieve job with id '{job_id}': no such job.")

    def get_job(self, job_id: uuid.UUID) -> ActiveJob:
        return self._processor.get_job(job_id=job_id)

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
            raise FailedJobException(job=j, parent=j._exception)
        else:
            raise Exception(f"Could not find job with id: {job_id}")

    def execute_and_retrieve(
        self, manifest: Manifest, inputs: Mapping[str, Any]
    ) -> ValueMap:

        job_id = self.execute(manifest=manifest, inputs=inputs, wait=True)
        results = self.retrieve_result(job_id=job_id)
        return results
