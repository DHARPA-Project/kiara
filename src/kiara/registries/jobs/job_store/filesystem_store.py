# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import orjson
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from kiara.models.module.jobs import JobRecord
from kiara.models.module.manifest import InputsManifest
from kiara.registries import FileSystemArchiveConfig
from kiara.registries.jobs import MANIFEST_SUB_PATH, JobArchive, JobStore


class FileSystemJobArchive(JobArchive):

    _archive_type_name = "filesystem_job_archive"
    _config_cls = FileSystemArchiveConfig

    @classmethod
    def is_writeable(cls) -> bool:
        return False

    @classmethod
    def supported_item_types(cls) -> Iterable[str]:
        return ["job_record"]

    def __init__(self, archive_id: uuid.UUID, config: FileSystemArchiveConfig):

        super().__init__(archive_id=archive_id, config=config)
        self._base_path: Optional[Path] = None

    @property
    def job_store_path(self) -> Path:

        if self._base_path is not None:
            return self._base_path

        self._base_path = Path(self.config.archive_path).absolute()
        self._base_path.mkdir(parents=True, exist_ok=True)
        return self._base_path

    def find_matching_job_record(
        self, inputs_manifest: InputsManifest
    ) -> Optional[JobRecord]:

        manifest_hash = inputs_manifest.manifest_hash
        jobs_hash = inputs_manifest.job_hash

        base_path = self.job_store_path / MANIFEST_SUB_PATH
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
        job_record._is_stored = True
        return job_record


class FileSystemJobStore(FileSystemJobArchive, JobStore):

    _archive_type_name = "filesystem_job_store"

    @classmethod
    def is_writeable(cls) -> bool:
        return False

    def store_job_record(self, job_record: JobRecord):

        manifest_hash = job_record.manifest_hash
        jobs_hash = job_record.job_hash

        base_path = self.job_store_path / MANIFEST_SUB_PATH
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
