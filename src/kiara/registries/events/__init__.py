import abc
from typing import Iterable, Type, Protocol, Any

from kiara.models.events import KiaraEvent


class EventListener(Protocol):

    def handle_events(self, *events: KiaraEvent) -> Any:
        pass


class AsyncEventListener(Protocol):

    def wait_for_processing(self, processing_id: Any):
        pass


class EventProducer(Protocol):

    def suppoerted_event_types(self) -> Iterable[Type[KiaraEvent]]:
        pass
