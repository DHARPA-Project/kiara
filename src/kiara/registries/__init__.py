# -*- coding: utf-8 -*-
import abc
import uuid
from typing import TYPE_CHECKING, Iterable, Optional

if TYPE_CHECKING:
    from kiara.kiara import Kiara


class KiaraArchive(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def supported_item_types(cls) -> Iterable[str]:
        pass

    @classmethod
    @abc.abstractmethod
    def is_writeable(cls) -> bool:
        pass

    @abc.abstractmethod
    def register_archive(self, kiara: "Kiara") -> uuid.UUID:
        pass


class BaseArchive(KiaraArchive):
    def __init__(self, archive_id: uuid.UUID):

        self._archive_id: uuid.UUID = archive_id
        self._kiara: Optional["Kiara"] = None

    @property
    def kiara_context(self) -> "Kiara":
        if self._kiara is None:
            raise Exception("Archive not registered into a kiara context yet.")
        return self._kiara

    def register_archive(self, kiara: "Kiara") -> uuid.UUID:
        self._kiara = kiara
        return self._archive_id
