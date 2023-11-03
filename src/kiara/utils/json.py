# -*- coding: utf-8 -*-
import orjson

from kiara.utils import is_debug

DEFAULT_ORJSON_OPTIONS = (
    orjson.OPT_SERIALIZE_NUMPY
    | orjson.OPT_SERIALIZE_DATACLASS
    | orjson.OPT_NON_STR_KEYS
)
DEFAULT_ORJSON_DUMP_ARGS = {"option": DEFAULT_ORJSON_OPTIONS}


def orjson_dumps(v, *, default=None, **args) -> str:
    # orjson.dumps returns bytes, to match standard json.dumps we need to decode

    if not args:
        args = DEFAULT_ORJSON_DUMP_ARGS

    try:
        return orjson.dumps(v, default=default, **args).decode()
    except Exception as e:
        if is_debug():
            from kiara.utils.cli import terminal_print

            terminal_print(f"Error dumping json data: {e}")
            from kiara import dbg

            dbg(v)

        raise e
