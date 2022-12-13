# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import datetime
import orjson
import shutil
import structlog
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, Union

from kiara.models.module.jobs import JobRecord
from kiara.registries import ArchiveDetails, FileSystemArchiveConfig
from kiara.registries.jobs import MANIFEST_SUB_PATH, JobArchive, JobStore
from kiara.utils.windows import fix_windows_longpath

log = structlog.getLogger()


class FileSystemJobArchive(JobArchive):

    _archive_type_name = "filesystem_job_archive"
    _config_cls = FileSystemArchiveConfig  # type: ignore

    @classmethod
    def is_writeable(cls) -> bool:
        return False

    @classmethod
    def supported_item_types(cls) -> Iterable[str]:
        return ["job_record"]

    def __init__(self, archive_id: uuid.UUID, config: FileSystemArchiveConfig):

        super().__init__(archive_id=archive_id, config=config)
        self._base_path: Union[Path, None] = None

    def get_archive_details(self) -> ArchiveDetails:

        size = sum(
            f.stat().st_size for f in self.job_store_path.glob("**/*") if f.is_file()
        )
        return ArchiveDetails(size=size)

    @property
    def job_store_path(self) -> Path:

        if self._base_path is not None:
            return self._base_path

        self._base_path = Path(self.config.archive_path).absolute()  # type: ignore
        self._base_path = fix_windows_longpath(self._base_path)
        self._base_path.mkdir(parents=True, exist_ok=True)
        return self._base_path

    def _delete_archive(self) -> None:

        shutil.rmtree(self.job_store_path)

    def retrieve_all_job_hashes(
        self,
        manifest_hash: Union[str, None] = None,
        inputs_hash: Union[str, None] = None,
    ) -> Iterable[str]:

        base_path = self.job_store_path / MANIFEST_SUB_PATH
        if not manifest_hash:
            if not inputs_hash:
                records = base_path.glob("*/*/*.job_record")
            else:
                records = base_path.glob(f"*/{inputs_hash}/*.job_record")
        else:
            if not inputs_hash:
                records = base_path.glob(f"{manifest_hash}/*/*.job_record")
            else:
                records = base_path.glob(f"{manifest_hash}/{inputs_hash}/*.job_record")

        result = []
        for record in records:
            result.append(record.name[0:-11])
        return result

    def _retrieve_record_for_job_hash(self, job_hash: str) -> Union[JobRecord, None]:

        base_path = self.job_store_path / MANIFEST_SUB_PATH
        records = list(base_path.glob(f"*/*/{job_hash}.job_record"))

        if not records:
            return None

        assert len(records) == 1
        details_file = records[0]

        details_content = details_file.read_text()
        details: Dict[str, Any] = orjson.loads(details_content)

        job_record = JobRecord(**details)
        job_record._is_stored = True
        return job_record

    # def find_matching_job_record(
    #     self, inputs_manifest: InputsManifest
    # ) -> Optional[JobRecord]:
    #
    #     manifest_hash = inputs_manifest.manifest_cid
    #     jobs_hash = inputs_manifest.job_hash
    #
    #     base_path = self.job_store_path / MANIFEST_SUB_PATH
    #     manifest_folder = base_path / str(manifest_hash)
    #
    #     if not manifest_folder.exists():
    #         return None
    #
    #     manifest_file = manifest_folder / "manifest.json"
    #
    #     if not manifest_file.exists():
    #         raise Exception(
    #             f"No 'manifests.json' file for manifest with hash: {manifest_hash}"
    #         )
    #
    #     details_folder = manifest_folder / str(jobs_hash)
    #     if not details_folder.exists():
    #         return None
    #
    #     details_file_name = details_folder / "details.json"
    #     if not details_file_name.exists():
    #         raise Exception(
    #             f"No 'inputs.json' file for manifest/inputs hash-combo: {manifest_hash} / {jobs_hash}"
    #         )
    #
    #     details_content = details_file_name.read_text()
    #     details: Dict[str, Any] = orjson.loads(details_content)
    #
    #     job_record = JobRecord(**details)
    #     job_record._is_stored = True
    #     return job_record


class FileSystemJobStore(FileSystemJobArchive, JobStore):

    _archive_type_name = "filesystem_job_store"

    @classmethod
    def is_writeable(cls) -> bool:
        return False

    def store_job_record(self, job_record: JobRecord):

        manifest_cid = job_record.manifest_cid
        inputs_hash = job_record.inputs_hash

        base_path = self.job_store_path / MANIFEST_SUB_PATH
        manifest_folder = base_path / str(manifest_cid)

        manifest_folder.mkdir(parents=True, exist_ok=True)

        manifest_info_file = manifest_folder / "manifest.json"
        if not manifest_info_file.exists():
            manifest_info_file.write_text(job_record.manifest_data_as_json())

        job_folder = manifest_folder / inputs_hash
        job_folder = fix_windows_longpath(job_folder)
        job_folder.mkdir(parents=True, exist_ok=True)

        job_details_file = job_folder / f"{job_record.job_hash}.job_record"
        job_details_file = fix_windows_longpath(job_details_file)

        exists = False
        if job_details_file.exists():
            exists = True
            # TODO: check details match? or overwrite
            file_m_time = datetime.datetime.fromtimestamp(
                job_details_file.stat().st_mtime
            ).timestamp()
            archive = job_folder / ".archive"
            archive.mkdir(parents=True, exist_ok=True)
            backup = archive / f"{job_details_file.name}.{file_m_time}"
            log.debug(
                "overwrite.store_job_record",
                reason="job record already exists",
                job_hash=job_record.job_hash,
                new_path=backup.as_posix(),
            )
            shutil.move(job_details_file.as_posix(), backup)

        job_details_file.write_text(job_record.json())

        for output_name, output_v_id in job_record.outputs.items():

            outputs_file_name = (
                job_folder / f"output__{output_name}__value_id__{output_v_id}.json"
            )

            if outputs_file_name.exists() and not exists:
                # if value.pedigree_output_name == "__void__":
                #     return
                # else:
                raise Exception(f"Can't write value '{output_v_id}': already exists.")
            else:
                outputs_file_name.touch()
