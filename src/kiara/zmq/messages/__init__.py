# -*- coding: utf-8 -*-
from collections import namedtuple
from typing import Any, List

import orjson

from kiara.utils.json import DEFAULT_ORJSON_OPTIONS

ReqMsg = namedtuple("ReqMsg", ["version", "endpoint", "args"])


class KiaraApiMsgBuilder(object):
    def __init__(self):
        self._version_nr_mayor = 0
        self._version_nr_minor = 0
        self._version = int.to_bytes(
            self._version_nr_mayor, length=1, byteorder="big"
        ) + int.to_bytes(self._version_nr_minor, length=1, byteorder="big")

    def encode_msg(self, endpoint_name: str, args: Any) -> List[bytes]:

        try:
            if args:
                if hasattr(args, "model_dump_json"):
                    _args = args.model_dump_json()
                elif hasattr(args, "json"):
                    _args = args.json()
                else:
                    _args = orjson.dumps(args, option=DEFAULT_ORJSON_OPTIONS)
                return [self._version, endpoint_name.encode(), _args]
            else:
                return [self._version, endpoint_name.encode()]
        except Exception as e:
            return [
                self._version,
                endpoint_name.encode(),
                orjson.dumps({"error": str(e)}),
            ]

    def decode_msg(self, msg: List[bytes]) -> ReqMsg:

        version, endpoint = msg[0], msg[1]
        if len(msg) == 3:
            args = orjson.loads(msg[2])
        else:
            args = {}

        return ReqMsg(version, endpoint.decode(), args)
