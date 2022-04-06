# -*- coding: utf-8 -*-
import uuid
from enum import Enum
from functools import partial
from typing import TYPE_CHECKING, Dict, Protocol, List, Iterable, Type, Callable

from kiara.models.events import KiaraEvent
from kiara.registries.ids import ID_REGISTRY

if TYPE_CHECKING:
    from kiara.kiara import Kiara


class EventListener(Protocol):

    def handle_events(self, *events: KiaraEvent):
        pass

class EventProducer(Protocol):

    def suppoerted_event_types(self) -> Iterable[Type[KiaraEvent]]:
        pass

class EventRegistry(object):

    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._producers: Dict[uuid.UUID, EventProducer] = {}
        self._listeners: Dict[Type[KiaraEvent], List[EventListener]] = {}

    def add_producer(self, producer: EventProducer) -> Callable:

        producer_id = ID_REGISTRY.generate(obj=producer, comment="adding event producer")
        func = partial(self.handle_events, producer_id)
        return func

    def add_listener(self, listener, *event_types: str):

        for event_type in event_types:
            self._listeners.setdefault(event_type, []).append(listener)

    def handle_events(self, producer_id: uuid.UUID, *events: KiaraEvent):
        pass
