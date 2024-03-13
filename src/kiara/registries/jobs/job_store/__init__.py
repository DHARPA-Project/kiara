# -*- coding: utf-8 -*-
import abc
import uuid
from datetime import datetime
from typing import Generator, Iterable, Mapping, Union

from kiara.models.module.jobs import JobMatcher, JobRecord
from kiara.registries import BaseArchive


class JobArchive(BaseArchive):
    # @abc.abstractmethod
    # def find_matching_job_record(
    #     self, inputs_manifest: InputsManifest
    # ) -> Optional[JobRecord]:
    #     pass

    @classmethod
    def supported_item_types(cls) -> Iterable[str]:
        return ["job_record"]

    @abc.abstractmethod
    def retrieve_all_job_hashes(
        self,
        manifest_hash: Union[str, None] = None,
        inputs_id_hash: Union[str, None] = None,
        inputs_data_hash: Union[str, None] = None,
    ) -> Iterable[str]:
        """
        Retrieve a list of all job record hashes (cids) that match the given filter arguments.

        A job record hash includes information about the module type used in the job, the module configuration, as well as input field names and value ids for the values used in those inputs.

        If the job archive retrieves its jobs in a dynamic way, this will return 'None'.
        """

    @abc.abstractmethod
    def _retrieve_all_job_ids(self) -> Mapping[uuid.UUID, datetime]:
        """
        Retrieve a list of all job record ids in the archive, along with when they where submitted.
        """

    def retrieve_all_job_ids(self) -> Mapping[uuid.UUID, datetime]:
        """Retrieve a list of all job ids in the archive, along with when they where submitted."""
        return self._retrieve_all_job_ids()

    @abc.abstractmethod
    def _retrieve_record_for_job_id(self, job_id: uuid.UUID) -> Union[JobRecord, None]:
        pass

    def retrieve_record_for_job_id(self, job_id: uuid.UUID) -> Union[JobRecord, None]:
        job_record = self._retrieve_record_for_job_id(job_id=job_id)
        return job_record

    @abc.abstractmethod
    def _retrieve_record_for_job_hash(self, job_hash: str) -> Union[JobRecord, None]:
        pass

    def retrieve_record_for_job_hash(self, job_hash: str) -> Union[JobRecord, None]:

        job_record = self._retrieve_record_for_job_hash(job_hash=job_hash)
        return job_record

    def retrieve_matching_job_records(
        self, matcher: JobMatcher
    ) -> Generator[JobRecord, None, None]:
        return self._retrieve_matching_job_records(matcher=matcher)

    @abc.abstractmethod
    def _retrieve_matching_job_records(
        self, matcher: JobMatcher
    ) -> Generator[JobRecord, None, None]:
        pass


class JobStore(JobArchive):
    @classmethod
    def _is_writeable(cls) -> bool:
        return True

    @abc.abstractmethod
    def store_job_record(self, job_record: JobRecord):
        pass
