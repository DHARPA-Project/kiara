# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import uuid
from typing import Iterable, Set, Union

from kiara.models.module.destiny import Destiny
from kiara.registries import ARCHIVE_CONFIG_CLS, BaseArchive


class DestinyArchive(BaseArchive):
    @classmethod
    def supported_item_types(cls) -> Iterable[str]:
        return ["destiny"]

    def __init__(self, archive_id: uuid.UUID, config: ARCHIVE_CONFIG_CLS):

        super().__init__(archive_id=archive_id, config=config)

    @abc.abstractmethod
    def get_all_value_ids(self) -> Set[uuid.UUID]:
        """Retrun a list of all value ids that have destinies stored in this archive."""

    @abc.abstractmethod
    def get_destiny_aliases_for_value(
        self, value_id: uuid.UUID
    ) -> Union[Set[str], None]:
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
