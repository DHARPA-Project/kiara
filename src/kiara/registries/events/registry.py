# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import fnmatch
import uuid
from functools import partial
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List

from kiara.models.events import KiaraEvent
from kiara.registries.events import AsyncEventListener, EventListener, EventProducer
from kiara.registries.ids import ID_REGISTRY

if TYPE_CHECKING:
    from kiara.context import Kiara


class AllEvents(KiaraEvent):
    pass


class EventRegistry(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._producers: Dict[uuid.UUID, EventProducer] = {}
        self._listeners: Dict[uuid.UUID, EventListener] = {}
        self._subscriptions: Dict[uuid.UUID, List[str]] = {}

    def add_producer(self, producer: EventProducer) -> Callable:

        producer_id = ID_REGISTRY.generate(
            obj=producer, comment="adding event producer"
        )
        func = partial(self.handle_events, producer_id)
        return func

    def add_listener(self, listener, *subscriptions: str):

        if not subscriptions:
            _subscriptions = ["*"]
        else:
            _subscriptions = list(subscriptions)

        listener_id = ID_REGISTRY.generate(
            obj=listener, comment="adding event listener"
        )
        self._listeners[listener_id] = listener
        self._subscriptions[listener_id] = _subscriptions

    def _matches_subscription(
        self, events: Iterable[KiaraEvent], subscriptions: Iterable[str]
    ) -> Iterable[KiaraEvent]:

        result = []
        for subscription in subscriptions:
            for event in events:
                match = fnmatch.filter([event.get_event_type()], subscription)
                if match:
                    result.append(event)

        return result

    def handle_events(self, producer_id: uuid.UUID, *events: KiaraEvent):

        event_targets: Dict[uuid.UUID, List[KiaraEvent]] = {}

        for l_id, listener in self._listeners.items():
            matches = self._matches_subscription(
                events=events, subscriptions=self._subscriptions[l_id]
            )
            if matches:
                event_targets.setdefault(l_id, []).extend(matches)

        responses = {}
        for l_id, l_events in event_targets.items():
            listener = self._listeners[l_id]
            response = listener.handle_events(*l_events)
            responses[l_id] = response

        for l_id, response in responses.items():
            if response is None:
                continue

            a_listener: AsyncEventListener = self._listeners[l_id]  # type: ignore
            if not hasattr(a_listener, "wait_for_processing"):
                raise Exception(
                    "Can't wait for processing of event for listener: listener does not provide 'wait_for_processing' method."
                )
            a_listener.wait_for_processing(response)
