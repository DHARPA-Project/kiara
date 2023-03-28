# -*- coding: utf-8 -*-
import sys
from typing import Any, Union

from kiara.interfaces import get_console


class KiaraZmqClient(object):
    def __init__(self, host: Union[None, str] = None, port: Union[None, int] = None):

        import zmq

        from kiara.zmq.messages import KiaraApiMsgBuilder

        if host is None:
            host = "localhost"
        elif host in ["0.0.0.0", "*"]:  # noqa
            host = "localhost"

        if port is None:
            port = 8080

        self._host: str = host
        self._port: int = port
        self._context = zmq.Context()
        self._msg_builder = KiaraApiMsgBuilder()
        self._socket = self._context.socket(zmq.REQ)
        self._socket.connect(f"tcp://{host}:%s" % self._port)

    def close(self):
        self._context.destroy()

    def request_cli(self, args: Any) -> Any:

        width = get_console().width
        args = {"console_width": width, "sub-command": args, "executable": sys.argv[0]}

        msg = self._msg_builder.encode_msg(endpoint_name="cli", args=args)

        self._socket.send_multipart(msg)
        response = self._socket.recv_multipart()
        response_msg = self._msg_builder.decode_msg(response)

        print(response_msg.args["stdout"])  # noqa
        stderr = response_msg.args["stderr"]
        if stderr:
            print(stderr, file=sys.stderr)  # noqa

    def request(self, endpoint_name: str, args: Any = None) -> Any:

        if endpoint_name == "cli":
            self.request_cli(args=args)
            return

        msg = self._msg_builder.encode_msg(endpoint_name=endpoint_name, args=args)

        self._socket.send_multipart(msg)
        response = self._socket.recv_multipart()
        response_msg = self._msg_builder.decode_msg(response)

        return response_msg.args
