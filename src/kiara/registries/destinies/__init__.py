# -*- coding: utf-8 -*-
import abc
import uuid
from typing import Mapping, Optional, Set, Iterable

from kiara.models.module.destiniy import Destiny
from kiara.models.values.value import Value
from kiara.models.values.value_schema import ValueSchema


class DestinyArchive(abc.ABC):
    @abc.abstractmethod
    def get_destiny_archive_id(self) -> uuid.UUID:
        pass

    @abc.abstractmethod
    def get_destinies_for(
        self, value_id: uuid.UUID
    ) -> Optional[Mapping[str, ValueSchema]]:
        """Retrieve all the destinies for the specified value within this archive.

        In case this archive discovers its value destinies dynamically, this can return 'None'.
        """

    @abc.abstractmethod
    def get_destiny(self, value_id: uuid.UUID, destiny: str) -> Destiny:
        pass


class DestinyStore(DestinyArchive):

    @abc.abstractmethod
    def persist_destiny(
        self, value_ids: Iterable[Value], destiny_alias: str, destiny: Destiny
    ):
        pass

