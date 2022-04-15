# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import uuid
from typing import Optional, Set

from kiara.models.module.destiniy import Destiny


class DestinyArchive(abc.ABC):
    @abc.abstractmethod
    def get_destiny_archive_id(self) -> uuid.UUID:
        pass

    @abc.abstractmethod
    def get_all_value_ids(self) -> Set[uuid.UUID]:
        """Retrun a list of all value ids that have destinies stored in this archive."""

    @abc.abstractmethod
    def get_destiny_aliases_for_value(self, value_id: uuid.UUID) -> Optional[Set[str]]:
        """Retrieve all the destinies for the specified value within this archive.

        In case this archive discovers its value destinies dynamically, this can return 'None'.
        """

    @abc.abstractmethod
    def get_destiny(self, value_id: uuid.UUID, destiny: str) -> Destiny:
        pass


class DestinyStore(DestinyArchive):
    @abc.abstractmethod
    def persist_destiny(self, destiny: Destiny):
        pass
