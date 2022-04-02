# -*- coding: utf-8 -*-
import abc
import orjson
import structlog
import uuid
from bidict import bidict
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, TYPE_CHECKING

from kiara.models.module.jobs import ActiveJob, JobConfig, JobRecord, JobStatus
from kiara.models.module.manifest import InputsManifest, Manifest
from kiara.models.values.value import ValueSet
from kiara.processing import ModuleProcessor
from kiara.processing.synchronous import SynchronousProcessor

if TYPE_CHECKING:
    from kiara.kiara import Kiara


logger = structlog.getLogger()

MANIFEST_SUB_PATH = "manifests"


class JobArchive(abc.ABC):
    @abc.abstractmethod
    def get_job_archive_id(self) -> uuid.UUID:
        pass

    @abc.abstractmethod
    def find_matching_job_record(
        self, inputs_manifest: InputsManifest
    ) -> Optional[JobRecord]:
        pass


class JobStore(JobArchive):
    @abc.abstractmethod
    def store_job_record(self, job_record: JobRecord):
        pass


class FileSystemJobArchive(JobArchive):
    @classmethod
    def create_from_kiara_context(cls, kiara: "Kiara"):

        base_path = Path(kiara.context_config.data_directory) / "job_store"
        base_path.mkdir(parents=True, exist_ok=True)
        return cls(base_path=base_path, store_id=kiara.id)

    def __init__(self, base_path: Path, store_id: Optional[uuid.UUID] = None):

        if not base_path.is_dir():
            raise Exception(
                f"Can't create file system archive instance, base path does not exist or is not a folder: {base_path.as_posix()}."
            )

        from kiara.kiara import KIARA_IDS

        self._store_id: uuid.UUID = KIARA_IDS.generate(
            id=store_id, obj=self, type="job archive", cls=self.__class__
        )
        self._base_path: Path = base_path

    def get_job_archive_id(self) -> uuid.UUID:
        return self._store_id

    def find_matching_job_record(
        self, inputs_manifest: InputsManifest
    ) -> Optional[JobRecord]:

        return self._retrieve_job_record(
            manifest_hash=inputs_manifest.manifest_hash,
            jobs_hash=inputs_manifest.jobs_hash,
        )

    def _retrieve_job_record(
        self, manifest_hash: int, jobs_hash: int
    ) -> Optional[JobRecord]:

        base_path = self._base_path / MANIFEST_SUB_PATH
        manifest_folder = base_path / str(manifest_hash)

        if not manifest_folder.exists():
            return None

        manifest_file = manifest_folder / "manifest.json"

        if not manifest_file.exists():
            raise Exception(
                f"No 'manifests.json' file for manifest with hash: {manifest_hash}"
            )

        details_folder = manifest_folder / str(jobs_hash)
        if not details_folder.exists():
            return None

        details_file_name = details_folder / "details.json"
        if not details_file_name.exists():
            raise Exception(
                f"No 'inputs.json' file for manifest/inputs hash-combo: {manifest_hash} / {jobs_hash}"
            )

        details_content = details_file_name.read_text()
        details: Dict[str, Any] = orjson.loads(details_content)

        job_record = JobRecord(**details)
        return job_record


class FileSystemJobStore(FileSystemJobArchive, JobStore):
    def store_job_record(self, job_record: JobRecord):

        manifest_hash = job_record.manifest_hash
        jobs_hash = job_record.jobs_hash

        base_path = self._base_path / MANIFEST_SUB_PATH
        manifest_folder = base_path / str(manifest_hash)

        manifest_folder.mkdir(parents=True, exist_ok=True)

        manifest_info_file = manifest_folder / "manifest.json"
        if not manifest_info_file.exists():
            manifest_info_file.write_text(job_record.manifest_data_as_json())

        job_folder = manifest_folder / str(jobs_hash)
        job_folder.mkdir(parents=True, exist_ok=True)

        job_details_file_name = job_folder / "details.json"
        if job_details_file_name.exists():
            raise Exception(
                f"Job record already exists: {job_details_file_name.as_posix()}"
            )

        job_details_file_name.write_text(job_record.json())

        for output_name, output_v_id in job_record.outputs.items():

            outputs_file_name = (
                job_folder / f"output__{output_name}__value_id__{output_v_id}.json"
            )

            if outputs_file_name.exists():
                # if value.pedigree_output_name == "__void__":
                #     return
                # else:
                raise Exception(f"Can't write value '{output_v_id}': already exists.")
            else:
                outputs_file_name.touch()


class JobRegistry(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._active_jobs: bidict[int, uuid.UUID] = bidict()
        self._save_marks: Dict[uuid.UUID, bool] = {}
        self._failed_jobs: Dict[int, uuid.UUID] = {}
        self._finished_jobs: Dict[int, uuid.UUID] = {}
        self._archived_records: Dict[uuid.UUID, JobRecord] = {}

        self._processor: ModuleProcessor = SynchronousProcessor(kiara=self._kiara)
        self._processor.register_job_status_listener(self)
        self._job_archives: Dict[uuid.UUID, JobArchive] = {}
        self._default_job_store: Optional[JobStore] = None

        self.register_job_archive(
            FileSystemJobStore.create_from_kiara_context(self._kiara)
        )

    def job_status_changed(
        self, job_id: uuid.UUID, old_status: Optional[JobStatus], new_status: JobStatus
    ):

        if job_id in self._active_jobs.values() and new_status is JobStatus.FAILED:
            job_hash = self._active_jobs.inverse.pop(job_id)
            self._failed_jobs[job_hash] = job_id
            self._save_marks.pop(job_id)
        elif job_id in self._active_jobs.values() and new_status is JobStatus.SUCCESS:
            job_hash = self._active_jobs.inverse.pop(job_id)

            job_record = self._processor.get_job_record(job_id)
            save = self._save_marks.pop(job_id)
            if save:
                logger.debug(
                    "store.job_record",
                    jobs_hash=job_hash,
                    module_type=job_record.module_type,
                )
                self._default_job_store.store_job_record(job_record)

            self._finished_jobs[job_hash] = job_id
            self._archived_records[job_id] = job_record

    def register_job_archive(self, job_store: JobStore):

        js_id = job_store.get_job_archive_id()
        if js_id in self._job_archives.keys():
            raise Exception(
                f"Can't register job store, store id already registered: {js_id}."
            )

        self._job_archives[js_id] = job_store

        if self._default_job_store is None and isinstance(job_store, JobStore):
            self._default_job_store = job_store

    @property
    def default_job_store(self) -> JobStore:

        if self._default_job_store is None:
            raise Exception("No default job store set (yet).")
        return self._default_job_store

    def find_matching_job_record(
        self, inputs_manifest: InputsManifest
    ) -> Optional[uuid.UUID]:
        """Check if a job with same inputs manifest already ran some time before.

        Arguments:
            inputs_manifest: the manifest incl. inputs

        Returns:
            'None' if no such job exists, a (uuid) job-id if the job is currently running or has run in the past
        """

        if inputs_manifest.jobs_hash in self._active_jobs.keys():
            logger.debug("job.use_running")
            return self._active_jobs[inputs_manifest.jobs_hash]

        if inputs_manifest.jobs_hash in self._finished_jobs.keys():
            job_id = self._finished_jobs[inputs_manifest.jobs_hash]
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

        from kiara.kiara import KIARA_IDS

        fake_job_id = KIARA_IDS.generate(type="archived_job_id", obj=job_record)

        self._finished_jobs[inputs_manifest.jobs_hash] = fake_job_id
        self._archived_records[fake_job_id] = job_record
        logger.debug("job.use_cached")
        return fake_job_id

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

    def execute_job(
        self, job_config: JobConfig, wait: bool = False, save_job: bool = False
    ) -> uuid.UUID:

        log = logger.bind(
            module_type=job_config.module_type,
            inputs={k: str(v) for k, v in job_config.inputs.items()},
            job_hash=job_config.model_data_hash,
        )

        stored_job = self.find_matching_job_record(inputs_manifest=job_config)
        if stored_job is not None:
            return stored_job

        log.debug("job.execute", inputs=job_config.inputs)

        job_id = self._processor.create_job(job_config=job_config)
        self._save_marks[job_id] = save_job
        self._active_jobs[job_config.jobs_hash] = job_id

        self._processor.queue_job(job_id=job_id)

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
            else:
                raise Exception(f"Can't retrieve job with id '{job_id}': no such job.")

    def get_job_status(self, job_id: uuid.UUID) -> JobStatus:

        if job_id in self._archived_records.keys():
            return JobStatus.SUCCESS
        elif job_id in self._failed_jobs.values():
            return JobStatus.FAILED

        return self._processor.get_job_status(job_id=job_id)

    def retrieve_result(self, job_id: uuid.UUID) -> ValueSet:

        if job_id not in self._archived_records.keys():
            self._processor.wait_for(job_id)

        job_record = self._archived_records[job_id]

        results = self._kiara.data_registry.load_values(job_record.outputs)
        return results

    def execute_and_retrieve(
        self, manifest: Manifest, inputs: Mapping[str, Any]
    ) -> ValueSet:

        job_id = self.execute(manifest=manifest, inputs=inputs, wait=True)
        results = self.retrieve_result(job_id=job_id)
        return results
