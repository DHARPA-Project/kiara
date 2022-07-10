# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Implementation of interfaces for *Kiara*."""

import os
import rich
import structlog
import sys
from rich.console import Console
from typing import Union

log = structlog.getLogger()

# Global console used by alternative print
_console: Union[Console, None] = None


def get_console() -> Console:
    """Get a global Console instance.

    Returns:
        Console: A console instance.
    """
    global _console
    if _console is None or True:
        console_width = os.environ.get("CONSOLE_WIDTH", None)
        width = None

        if console_width:
            try:
                width = int(console_width)
            except Exception:
                pass

        _console = Console(width=width)

    return _console


def set_console_width(width: Union[int, None] = None, prefer_env: bool = True):

    global _console
    if prefer_env or not width:
        _width: Union[None, int] = None
        try:
            _width = int(os.environ.get("CONSOLE_WIDTH", None))  # type: ignore
        except Exception:
            pass
        if _width:
            width = _width

    if width:
        try:
            width = int(width)
        except Exception as e:
            log.debug("invalid.console_width", error=str(e))

    _console = Console(width=width)

    if not width:
        if "google.colab" in sys.modules or "jupyter_client" in sys.modules:
            width = 140

    if width:
        con = rich.get_console()
        con.width = width
