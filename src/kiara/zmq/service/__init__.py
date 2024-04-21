# -*- coding: utf-8 -*-
import atexit
import os
from threading import Thread
from typing import Any, Mapping, Union

import orjson
import zmq

from kiara.defaults import KIARA_MAIN_CONTEXT_LOCKS_PATH
from kiara.exceptions import KiaraException
from kiara.interfaces import BaseAPIWrap, get_console, get_proxy_console
from kiara.interfaces.cli.proxy_cli import proxy_cli
from kiara.interfaces.python_api.base_api import BaseAPI
from kiara.interfaces.python_api.proxy import ApiEndpoints
from kiara.zmq import (
    KiaraZmqServiceDetails,
    get_default_stderr_zmq_service_log_path,
    get_default_stdout_zmq_service_log_path,
)
from kiara.zmq.messages import KiaraApiMsgBuilder

DEFAULT_LISTEN_HOST = "*"
DEFAULT_PORT = 8000


class KiaraZmqAPI(object):
    def __init__(
        self,
        api_wrap: BaseAPIWrap,
        stdout: Union[str, None] = None,
        stderr: Union[str, None] = None,
        host: Union[str, None] = None,
        port: Union[int, None] = None,
        listen_timout_in_ms: Union[int, None] = None,
    ):

        if listen_timout_in_ms is None:
            listen_timout_in_ms = 0

        if host in [None, "*", "localhost"]:
            host_ip = "127.0.0.1"
        else:
            host_ip = host  # type: ignore

        if not port:
            import socketserver

            with socketserver.TCPServer((host_ip, 0), None) as s:  # type: ignore
                port = s.server_address[1]

        self._api_wrap: BaseAPIWrap = api_wrap
        self._api_wrap.exit_process = False

        self._listen_host: str = host_ip
        self._port: int = int(port)
        self._service_thread = None
        self._msg_builder = KiaraApiMsgBuilder()
        self._api_endpoints: ApiEndpoints = ApiEndpoints(api_cls=BaseAPI)

        self._initial_timeout = listen_timout_in_ms
        self._allow_timeout_change = False

        if stdout is None:
            stdout = get_default_stdout_zmq_service_log_path(
                context_name=api_wrap.kiara_context_name
            )

        if stderr is None:
            stderr = get_default_stderr_zmq_service_log_path(
                context_name=api_wrap.kiara_context_name
            )

        if isinstance(stdout, str):
            os.makedirs(os.path.dirname(stdout), exist_ok=True)
            self._stdout = open(stdout, "w")
        else:
            self._stdout = stdout

        if isinstance(stderr, str):
            os.makedirs(os.path.dirname(stderr), exist_ok=True)
            self._stderr = open(stderr, "w")
        else:
            self._stderr = stderr

        # reserving host and port, cross-process
        zmq_base = os.path.join(KIARA_MAIN_CONTEXT_LOCKS_PATH, "zmq")
        service_info_file = os.path.join(
            zmq_base, f"{self._api_wrap.kiara_context_name}.zmq"
        )

        if os.path.exists(service_info_file):
            raise KiaraException(
                f"Zmq service port for context '{self._api_wrap.kiara_context_name}' already reserved: {service_info_file}"
            )

        os.makedirs(os.path.dirname(service_info_file), exist_ok=True)

        details = KiaraZmqServiceDetails(
            context_name=self._api_wrap.kiara_context_name,
            process_id=os.getpid(),
            stdout=stdout,
            stderr=stderr,
            newly_started=None,
            host=host_ip,
            port=port,
        )

        with open(service_info_file, "wb") as f:
            f.write(orjson.dumps(details.model_dump()))

        def delete_info_file():
            os.unlink(service_info_file)

        atexit.register(delete_info_file)

    def service_loop(self):

        try:

            api = self._api_wrap.base_api

            timeout = self._initial_timeout

            context = zmq.Context()
            context_rep_socket = context.socket(zmq.REP)
            context_rep_socket.bind(f"tcp://{self._listen_host}:{self._port}")

            poller = zmq.Poller()
            poller.register(context_rep_socket, zmq.POLLIN)

            stop = False
            while not stop:

                if timeout:
                    socks = dict(poller.poll(timeout))
                else:
                    socks = dict(poller.poll())

                if not socks:
                    print(
                        "Socket timed out, shutting down service...", file=self._stdout
                    )
                    stop = True

                if (
                    context_rep_socket in socks
                    and socks[context_rep_socket] == zmq.POLLIN
                ):

                    #  Wait for next request from client
                    msg = context_rep_socket.recv_multipart()
                    print("Received request: ", msg, file=self._stdout)
                    decoded = self._msg_builder.decode_msg(msg)

                    if decoded.endpoint == "ping":
                        result = "pong"
                    elif decoded.endpoint in ["shutdown", "stop"]:
                        print("Shutting down...", file=self._stdout)
                        result = "ok"
                        stop = True
                    elif decoded.endpoint == "service_status":
                        context_config = (
                            self._api_wrap.base_api.context.context_config.model_dump()
                        )
                        runtime_config = (
                            self._api_wrap.base_api.context.runtime_config.model_dump()
                        )

                        result = {
                            "state": "running",
                            "timeout": timeout,
                            "context_config": context_config,
                            "runtime_config": runtime_config,
                        }
                    elif decoded.endpoint == "cli":
                        result = self.call_cli(api=api, **decoded.args)
                    elif decoded.endpoint == "control":
                        raise NotImplementedError()
                    else:
                        result = self.call_endpoint(
                            api=api, endpoint=decoded.endpoint, **decoded.args
                        )

                    resp_msg = self._msg_builder.encode_msg(decoded.endpoint, result)
                    context_rep_socket.send_multipart(resp_msg)

        except Exception as e:
            import traceback

            traceback.print_exc()
            print(f"ERROR IN ZMQ SERVICE: {e}", file=self._stderr)
            print("Stopping...", file=self._stderr)

    def call_cli(self, api: BaseAPI, **kwargs) -> Mapping[str, str]:

        console = get_console()
        old_width = console.width

        console_width = kwargs.get("console_width", old_width)
        color_system = kwargs.get("color_system", None)

        sub_command = kwargs.get("sub-command")

        console.width = console_width
        stdout = ""
        stderr = ""
        try:
            with get_proxy_console(
                width=console_width,
                color_system=color_system,
                restore_default_console=False,
            ) as proxy_console:
                with proxy_console.capture() as capture:

                    try:
                        proxy_cli.main(
                            args=sub_command,
                            prog_name="kiara",
                            obj=self._api_wrap,
                            standalone_mode=False,
                        )
                    except Exception as e:
                        stderr = str(e)

                if not stderr:
                    stdout = capture.get()
        except Exception as oe:
            stderr = str(oe)

        return {"stdout": stdout, "stderr": stderr}

    def call_endpoint(self, api: BaseAPI, endpoint: str, **kwargs) -> Any:

        try:
            endpoint_proxy = self._api_endpoints.get_api_endpoint(
                endpoint_name=endpoint
            )
        except Exception as e:
            msg = str(e)
            return {"error": msg}

        result = endpoint_proxy.execute(instance=api, **kwargs)
        return result

    def start(self):

        if self._service_thread is not None:
            raise Exception("Service already running")

        self._service_thread = Thread(target=self.service_loop)
        self._service_thread.start()

        return self._service_thread

    def stop(self):

        if self._service_thread is None:
            raise Exception("Service not running")

        if self._listen_host in ["0.0.0.0", "*"]:  # noqa
            c_host = "localhost"
        else:
            c_host = self._listen_host

        from kiara.zmq.client import KiaraZmqClient

        zmq_client = KiaraZmqClient(host=c_host, port=self._port)
        zmq_client.request(endpoint_name="stop", args={})

        self._service_thread.join()
        self._service_thread = None
