import abc
from typing import Iterable, Generic, TypeVar, Type

from kiara.models.events import KiaraEvent
from kiara.models.events.data_registry import RegistryEvent


class KiaraEventHook(abc.ABC):

    @abc.abstractmethod
    def supported_event_types(self) -> Iterable[Type[KiaraEvent]]:
        pass


class DataEventHook(KiaraEventHook):

    @abc.abstractmethod
    def process_hook(self, event: RegistryEvent):
        pass
