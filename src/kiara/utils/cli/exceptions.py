# -*- coding: utf-8 -*-
from functools import partial, wraps

from kiara.utils import is_debug, is_develop
from kiara.utils.cli import terminal_print


def handle_exception(
    func=None,
):

    if not func:
        return partial(handle_exception)

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:

            if is_debug() or is_develop():
                import traceback

                traceback.print_exc()
            terminal_print("")
            terminal_print(e)

    return wrapper
