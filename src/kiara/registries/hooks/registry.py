# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kiara.kiara import Kiara


class HookRegistry(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
