# -*- coding: utf-8 -*-
"""Implementation of interfaces for *Kiara*."""
# -*- coding: utf-8 -*-
import os
from rich.console import Console
from typing import Optional

# Global console used by alternative print
_console: Optional[Console] = None


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
