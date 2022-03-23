# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import rich_click as click
import zmq

from kiara import Kiara
from kiara.protocol import KiaraMsg, ProcessingRequestMsg


@click.group(name="server")
@click.pass_context
def server(ctx):
    """Start a kiara server."""


@server.command(name="start")
@click.pass_context
def start(ctx):
    """List available data_types (work in progress)."""

    kiara_obj: Kiara = ctx.obj["kiara"]

    jobs = JobsMgmt(kiara=kiara_obj)

    host = "127.0.0.1"
    port = "5001"
    # Creates a socket instance
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://{host}:{port}")

    print("Server started")

    while True:
        #  Wait for next request from client
        msg_data = socket.recv_multipart()
        print(f"Received request")

        msg: ProcessingRequestMsg = KiaraMsg.from_msg(msg_data)

        print(msg.payload)
        print(type(msg.payload))

        response = jobs.add_job(msg.payload)

        #  Send reply back to client
        socket.send_string(response)


@click.group(name="client")
@click.pass_context
def client(ctx):
    """Send client commands to a kiara server."""


@client.command(name="send")
@click.argument("msg", nargs=1, required=True)
@click.pass_context
def send(ctx, msg: str):
    kiara_obj: Kiara = ctx.obj["kiara"]

    host = "127.0.0.1"
    port = "5001"
    # Creates a socket instance
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://{host}:{port}")

    msg = {
        "msg_type": "processing_request",
        "payload": {
            "module_type": "table.query.sql",
            "module_config": {"query": "select * from data"},
        },
    }
    pr = ProcessingRequestMsg(**msg)
    msg = pr.create_msg()
    socket.send_multipart(msg)

    result = socket.recv()
    print(result)
