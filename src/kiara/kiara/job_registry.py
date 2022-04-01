# -*- coding: utf-8 -*-
import abc
from pathlib import Path

import orjson
import structlog
import uuid
from typing import TYPE_CHECKING, Any, Mapping, Optional

from kiara.models.module.jobs import JobConfig, ActiveJob, JobStatus, JobRecord
from kiara.models.module.manifest import Manifest, InputsManifest
from kiara.models.values.value import ValueSet
from kiara.processing import ModuleProcessor
from kiara.processing.synchronous import SynchronousProcessor
from kiara.utils import orjson_dumps

if TYPE_CHECKING:
    from kiara import Kiara

logger = structlog.getLogger()

MANIFEST_SUB_PATH = "manifests"

class JobArchive(abc.ABC):

    @abc.abstractmethod
    def find_matching_job_record(self, inputs_manifest: InputsManifest) -> Optional[JobRecord]:
        pass

class JobStore(JobArchive):

    @abc.abstractmethod
    def store_job_record(self, job_record: JobRecord):
        pass

class FileSystemJobArchive(JobArchive):

    def __init__(self, kiara: "Kiara"):

        self._base_path: Optional[Path] = None

    @property
    def job_store_path(self) -> Path:

        if self._base_path is not None:
            return self._base_path

        self._base_path = Path(self._kiara.context_config.data_directory) / "job_store"
        self._base_path.mkdir(parents=True, exist_ok=True)
        return self._base_path

    def find_matching_job_record(self, inputs_manifest: InputsManifest) -> Optional[JobRecord]:

        return self._retrieve_job_record(
            manifest_hash=inputs_manifest.manifest_hash, inputs_hash=inputs_manifest.inputs_hash
        )

    def _retrieve_job_record(
        self, manifest_hash: int, inputs_hash: int
    ) -> Optional[JobRecord]:

        base_path = self.job_store_path / MANIFEST_SUB_PATH
        manifest_folder = base_path / str(manifest_hash)

        if not manifest_folder.exists():
            return None

        manifest_file = manifest_folder / "manifest.json"

        if not manifest_file.exists():
            raise Exception(
                f"No 'manifests.json' file for manifest with hash: {manifest_hash}"
            )

        manifest_data = orjson.loads(manifest_file.read_text())

        inputs_folder = manifest_folder / str(inputs_hash)

        if not inputs_folder.exists():
            return None

        inputs_file_name = inputs_folder / "inputs.json"
        if not inputs_file_name.exists():
            raise Exception(
                f"No 'inputs.json' file for manifest/inputs hash-combo: {manifest_hash} / {inputs_hash}"
            )

        inputs_data = {
            k: uuid.UUID(v)
            for k, v in orjson.loads(inputs_file_name.read_text()).items()
        }

        outputs = {}
        for output_file in inputs_folder.glob("output__*.json"):
            full_output_name = output_file.name[8:]
            start_value_id = full_output_name.find("__value_id__")
            output_name = full_output_name[0:start_value_id]
            value_id_str = full_output_name[start_value_id + 12 : -5]

            value_id = uuid.UUID(value_id_str)
            outputs[output_name] = value_id

        job_record = JobRecord(
            module_type=manifest_data["module_type"],
            module_config=manifest_data["module_config"],
            inputs=inputs_data,
            outputs=outputs,
        )
        return job_record

class FileSystemJobStore(FileSystemJobArchive, JobStore):

    def store_job_record(self, job_record: JobRecord):

        manifest_hash = job_record.manifest_hash
        inputs_hash = job_record.inputs_hash

        base_path = self.job_store_path / MANIFEST_SUB_PATH
        manifest_folder = base_path / str(manifest_hash)

        manifest_folder.mkdir(parents=True, exist_ok=True)

        manifest_info_file = manifest_folder / "manifest.json"
        if not manifest_info_file.exists():
            manifest_info_file.write_text(job_record.manifest_data_as_json())

        inputs_folder = manifest_folder / str(inputs_hash)

        inputs_folder.mkdir(parents=True, exist_ok=True)

        inputs_details_file_name = inputs_folder / "inputs.json"
        if not inputs_details_file_name.exists():
            inputs_details_file_name.write_text(orjson_dumps(job_record.inputs))

        for output_name, output_v_id in job_record.outputs.items():

            outputs_file_name = (
                inputs_folder
                / f"output__{output_name}__value_id__{output_v_id}.json"
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
        self._processor: ModuleProcessor = SynchronousProcessor(kiara=self._kiara)


    def find_matching_job_record(self, inputs_manifest: InputsManifest) -> Optional[JobRecord]:

        if inputs_manifest.model_data_hash in self._registred_jobs.keys():
            return self._registred_jobs[inputs_manifest.model_data_hash]

        matches = []
        match = None
        for store_id, store in self.data_stores.items():
            match = store.retrieve_job_record(inputs_manifest=inputs_manifest)
            if match:
                matches.append(match)

        if len(matches) == 0:
            return None
        elif len(matches) > 1:
            raise Exception(
                f"Multiple stores have a record for inputs manifest '{inputs_manifest}', this is not supported (yet)."
            )

        # self._job_store_map[job.model_data_hash] = matches[0]
        self._registred_jobs[inputs_manifest.model_data_hash] = matches[0]

        return match

    def prepare_job_config(
        self, manifest: Manifest, inputs: Mapping[str, Any]
    ) -> JobConfig:

        module = self._kiara.create_module(manifest=manifest)
        job_config = JobConfig.create_from_module(data_registry=self._kiara.data_registry, module=module, inputs=inputs)

        return job_config

    def execute(self, manifest: Manifest, inputs: Mapping[str, Any], wait: bool=False) -> uuid.UUID:

        job_config = self.prepare_job_config(manifest=manifest, inputs=inputs)
        return self.execute_job(job_config, wait=wait)

    def execute_job(self, job_config: JobConfig, wait: bool=False) -> uuid.UUID:

        log = logger.bind(
            module_type=job_config.module_type,
            inputs={k: str(v) for k, v in job_config.inputs.items()},
            job_hash=job_config.model_data_hash,
        )

        # raise NotImplementedError()
        # stored_job = self._kiara.data_registry.find_matching_job_record(inputs_manifest=job_config)
        # if stored_job is not None:
        #     log.debug("job.use.cached")
        #     raise NotImplementedError()
        #     return self._kiara.data_registry.load_values(values=stored_job.outputs)

        log.debug("job.execute", inputs=job_config.inputs)

        job_id = self._processor.queue_job(job_config=job_config)

        if wait:
            self._processor.wait_for(job_id)

        return job_id

    def get_job_details(self, job_id: uuid.UUID) -> ActiveJob:
        return self._processor.get_job(job_id)

    def get_job_status(self, job_id: uuid.UUID) -> JobStatus:

        return self._processor.get_job_status(job_id=job_id)

    def retrieve_job(self, job_id: uuid.UUID, wait_for_finish: bool=False) -> ActiveJob:

        if wait_for_finish:
            self._processor.wait_for(job_id)

        job = self._processor.get_job(job_id=job_id)
        return job

    def retrieve_result(self, job_id: uuid.UUID) -> ValueSet:

        self._processor.wait_for(job_id)
        job = self._processor.get_job_record(job_id=job_id)

        results = self._kiara.data_registry.load_values(job.outputs)
        return results

    def execute_and_retrieve(self, manifest: Manifest, inputs: Mapping[str, Any]) -> ValueSet:

        job_id = self.execute(manifest=manifest, inputs=inputs, wait=True)
        results = self.retrieve_result(job_id=job_id)
        return results

