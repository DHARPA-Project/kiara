# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from typing import Any, Protocol

from kiara.models.events import KiaraEvent


class EventListener(Protocol):
    def handle_events(self, *events: KiaraEvent) -> Any:
        pass


class AsyncEventListener(Protocol):
    def wait_for_processing(self, processing_id: Any):
        pass


class EventProducer(Protocol):

    pass

    # def suppoerted_event_types(self) -> Iterable[Type[KiaraEvent]]:
    #     pass
