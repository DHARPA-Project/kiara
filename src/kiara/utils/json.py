# -*- coding: utf-8 -*-
import orjson

from kiara.utils import is_debug


def orjson_dumps(v, *, default=None, **args):
    # orjson.dumps returns bytes, to match standard json.dumps we need to decode

    try:
        return orjson.dumps(v, default=default, **args).decode()
    except Exception as e:
        if is_debug():
            print(f"Error dumping json data: {e}")
            from kiara import dbg

            dbg(v)

        raise e
