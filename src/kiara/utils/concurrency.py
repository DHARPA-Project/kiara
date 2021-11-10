# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import threading


class ThreadSaveCounter(object):
    """A thread-safe counter, can be used in kiara modules to update completion percentage."""

    def __init__(self):

        self._current = 0
        self._lock = threading.Lock()

    @property
    def current(self):
        return self._current

    def current_percent(self, total: int) -> int:

        return int((self.current / total) * 100)

    def increment(self):

        with self._lock:
            self._current += 1
            return self._current

    def decrement(self):

        with self._lock:
            self._current -= 1
            return self._current
