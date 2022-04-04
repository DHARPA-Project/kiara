# -*- coding: utf-8 -*-
import abc
import uuid
from typing import Mapping, Optional, Set

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
    def persist_destinies(
        self, value: Value, category: str, key: str, destinies: Set[Destiny]
    ):
        self._persist_destinies(
            value=value, category=category, key=key, destinies=destinies
        )

    @abc.abstractmethod
    def _persist_destinies(
        self, value: Value, category: str, key: str, destinies: Set[Destiny]
    ):
        pass
