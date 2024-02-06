# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import os
import sys
import typing
from typing import Literal, Union

import networkx as nx

from kiara.defaults import KIARA_DEFAULT_STAGES_EXTRACTION_TYPE
from kiara.utils import log_message
from kiara.utils.cli import terminal_print

if typing.TYPE_CHECKING:
    from IPython.core.display import Image

    from kiara.models.module.pipeline import PipelineConfig, PipelineStructure
    from kiara.models.module.pipeline.pipeline import Pipeline


def print_ascii_graph(
    graph: nx.Graph, restart_interpreter_if_asciinet_installed: bool = False
):

    try:
        from asciinet import graph_to_ascii  # type: ignore
    except:  # noqa
        import pip._internal.cli.main as pip

        cmd = ["-q", "--isolated", "install"]
        cmd.append("asciinet")

        log_message("install.python_package", packages="asciinet")
        exit_code = pip.main(cmd)
        try:
            from asciinet import graph_to_ascii  # type: ignore
        except:  # noqa
            exit_code = 1

        if restart_interpreter_if_asciinet_installed:
            os.execvp(sys.executable, (sys.executable,) + tuple(sys.argv))  # noqa

        if exit_code != 0:
            terminal_print(
                "\nCan't print graph on terminal, package 'asciinet' not available. Please install it into the current virtualenv using:\n\npip install 'git+https://github.com/cosminbasca/asciinet.git#egg=asciinet&subdirectory=pyasciinet'"
            )
            return

    try:
        from asciinet._libutil import check_java  # type: ignore

        check_java("Java ")
    except Exception:
        terminal_print()
        terminal_print(
            "\nJava is currently necessary to print ascii graph. This might change in the future, but to use this functionality please install a JRE."
        )
        return

    print(graph_to_ascii(graph))  # noqa


def create_image(graph: nx.Graph) -> bytes:

    try:
        import pygraphviz as pgv  # noqa  # type: ignore
    except:  # noqa
        raise Exception(
            "pygraphviz not available, please install it manually into the current virtualenv"
        )

    # graph = nx.relabel_nodes(graph, lambda x: hash(x))
    G = nx.nx_agraph.to_agraph(graph)

    G.node_attr["shape"] = "box"
    # G.unflatten().layout(prog="dot")
    G.layout(prog="dot")

    b: bytes = G.draw(format="png")
    return b


def save_image(graph: nx.Graph, path: str):

    with open(path, "wb") as f:
        try:
            graph_b = create_image(graph=graph)
        except Exception as e:
            graph_b = str(e).encode("utf-8")
        f.write(graph_b)


def graph_to_image(
    graph: nx.Graph, return_bytes: bool = False
) -> Union[bytes, "Image"]:

    b = create_image(graph=graph)

    if return_bytes:
        return b
    else:
        try:
            from IPython.core.display import Image

            return Image(b)
        except Exception:
            raise Exception(
                "pygraphviz not available, please install it manually into the current virtualenv."
            )


def pipeline_graph_to_image(
    pipeline: Union["Pipeline", "PipelineConfig", "PipelineStructure"],
    graph_type: Literal[
        "data-flow", "data-flow-simple", "execution", "stages"
    ] = "execution",
    stages_extraction_type: str = KIARA_DEFAULT_STAGES_EXTRACTION_TYPE,
    return_bytes: bool = False,
):

    if hasattr(pipeline, "structure"):
        pipeline = pipeline.structure  # type: ignore

    if graph_type == "data-flow":
        graph = pipeline.data_flow_graph  # type: ignore
    elif graph_type == "data-flow-simple":
        graph = pipeline.data_flow_graph_simple  # type: ignore
    elif graph_type == "execution":
        graph = pipeline.execution_graph  # type: ignore
    elif graph_type == "stages":
        graph = pipeline.get_stages_graph(stages_extraction_type=stages_extraction_type)  # type: ignore
    else:
        raise Exception(
            f"Invalid graph type '{graph_type}': must be one of 'data-flow', 'data-flow-simple', 'execution', 'stages'."
        )

    return graph_to_image(graph=graph, return_bytes=return_bytes)
