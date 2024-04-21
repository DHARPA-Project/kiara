# -*- coding: utf-8 -*-
import os
import sys
import typing
from typing import Dict, List, Union

import orjson
from pydantic import BaseModel, Field

from kiara.defaults import KIARA_MAIN_CONTEXT_DATA_PATH, KIARA_MAIN_CONTEXT_LOCKS_PATH
from kiara.interfaces import BaseAPIWrap

if typing.TYPE_CHECKING:
    pass


class KiaraZmqServiceDetails(BaseModel):

    context_name: str = Field(description="The name of the kiara context.")
    process_id: Union[None, int] = Field(
        None, description="The process id of the kiara service."
    )
    stdout: str = Field(description="The stdout handle.")
    stderr: str = Field(description="The stderr handle.")
    host: str = Field(description="The host the service is running on.")
    port: int = Field(description="The port the service is running on.")
    newly_started: Union[bool, None] = Field(
        description="If the service was newly started, or already running.",
        default=None,
    )


def get_default_stdout_zmq_service_log_path(context_name: str):
    return os.path.join(
        KIARA_MAIN_CONTEXT_DATA_PATH, context_name, "logs", "zmq_stdout.log"
    )


def get_default_stderr_zmq_service_log_path(context_name: str):
    return os.path.join(
        KIARA_MAIN_CONTEXT_DATA_PATH, context_name, "logs", "zmq_stderr.log"
    )


def zmq_context_registered(context_name: str) -> bool:

    zmq_base = os.path.join(
        KIARA_MAIN_CONTEXT_LOCKS_PATH, "zmq", f"{context_name}.json"
    )
    service_info_file = os.path.join(zmq_base, f"{context_name}.zmq")

    return os.path.exists(service_info_file)


def list_registered_contexts() -> List[str]:

    zmq_base = os.path.join(KIARA_MAIN_CONTEXT_LOCKS_PATH, "zmq")
    if not os.path.exists(zmq_base):
        return []

    return [x.replace(".zmq", "") for x in os.listdir(zmq_base) if x.endswith(".zmq")]


def get_context_details(context_name: str) -> Union[Dict, None]:

    zmq_base = os.path.join(KIARA_MAIN_CONTEXT_LOCKS_PATH, "zmq")
    service_info_file = os.path.join(zmq_base, f"{context_name}.zmq")

    if not os.path.exists(service_info_file):
        return None

    with open(service_info_file, "r") as f:
        result: Dict = orjson.loads(f.read())
        return result


def start_zmq_service(
    api_wrap: BaseAPIWrap,
    host: Union[str, None],
    port: Union[int, None] = None,
    stdout: Union[str, None] = None,
    stderr: Union[str, None] = None,
    timeout: Union[None, int] = None,
    monitor: bool = False,
) -> Union[None, KiaraZmqServiceDetails]:

    from kiara.exceptions import KiaraException

    if monitor:
        from kiara.zmq.service import KiaraZmqAPI

        zmq_api = KiaraZmqAPI(
            api_wrap=api_wrap,
            host=host,
            port=port,
            listen_timout_in_ms=timeout,
            stdout=stdout,
            stderr=stderr,
        )
        try:
            thread = zmq_api.start()
        except Exception as e:
            raise KiaraException(msg="Error starting zmq service.", parent=e)

        try:
            thread.join()
        except KeyboardInterrupt:
            zmq_api.stop()
            raise KiaraException(msg="User requested service stop...")
        return None

    else:

        import subprocess

        from kiara.zmq.client import KiaraZmqClient

        context_details = get_context_details(context_name=api_wrap.kiara_context_name)
        _newly_started = True

        if context_details is not None:
            _newly_started = False
            _process_id: int = context_details["process_id"]
            _stdout: str = context_details["stdout"]
            _stderr: str = context_details["stderr"]
            _host: str = context_details["host"]
            _port: int = context_details["port"]
            # TODO: check if stdout/stderr differ

        else:
            if host is None:
                _host = "127.0.0.1"
            else:
                _host = host
            if port:
                _port = port
            else:
                if host in [None, "*", "localhost"]:
                    host_ip = "127.0.0.1"
                else:
                    host_ip = _host
                import socketserver

                with socketserver.TCPServer((host_ip, 0), None) as s:  # type: ignore
                    _port = s.server_address[1]

            if stdout is None:
                _stdout = get_default_stdout_zmq_service_log_path(
                    context_name=api_wrap.kiara_context_name
                )
            else:
                _stdout = stdout

            if stderr is None:
                _stderr = get_default_stderr_zmq_service_log_path(
                    context_name=api_wrap.kiara_context_name
                )
            else:
                _stderr = stderr

            cli = [
                sys.argv[0],
                "-c",
                api_wrap.kiara_context_name,
                "context",
                "service",
                "start",
                "--monitor",
                "--host",
                _host,
                "--port",
                str(_port),
                "--stdout",
                _stdout,
                "--stderr",
                _stderr,
                "--timeout",
                str(timeout),
            ]
            p = subprocess.Popen(cli)
            _process_id = p.pid

        zmq_client = KiaraZmqClient(host=_host, port=_port)
        response = zmq_client.request("ping")
        assert response == "pong"

        return KiaraZmqServiceDetails(
            context_name=api_wrap.kiara_context_name,
            process_id=_process_id,
            stdout=_stdout,
            stderr=_stderr,
            newly_started=_newly_started,
            host=_host,
            port=_port,
        )


def ensure_zmq_service(
    api_wrap: BaseAPIWrap,
    host: str,
    port: int,
    stdout: str,
    stderr: str,
    timeout: int = 0,
    monitor: bool = False,
) -> Union[None, KiaraZmqServiceDetails]:

    return start_zmq_service(
        api_wrap=api_wrap,
        host=host,
        port=port,
        stdout=stdout,
        stderr=stderr,
        timeout=timeout,
        monitor=monitor,
    )
