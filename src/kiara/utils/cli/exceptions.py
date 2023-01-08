# -*- coding: utf-8 -*-
from functools import partial, wraps

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

            terminal_print("")
            terminal_print(e)

    return wrapper
