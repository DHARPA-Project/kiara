# -*- coding: utf-8 -*-
import networkx as nx
import typing
from typing import Literal, Union

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


if typing.TYPE_CHECKING:
    from kiara.models.module.pipeline import PipelineConfig, PipelineStructure
    from kiara.models.module.pipeline.pipeline import Pipeline


def create_image(graph: nx.Graph):

    try:
        import pygraphviz as pgv  # noqa
    except:  # noqa
        return "pygraphviz not available, please install it manually into the current virtualenv"

    # graph = nx.relabel_nodes(graph, lambda x: hash(x))
    G = nx.nx_agraph.to_agraph(graph)

    G.node_attr["shape"] = "box"
    # G.unflatten().layout(prog="dot")
    G.layout(prog="dot")

    b = G.draw(format="png")
    return b


def save_image(graph: nx.Graph, path: str):

    graph_b = create_image(graph=graph)
    with open(path, "wb") as f:
        f.write(graph_b)


def graph_to_image(graph: nx.Graph, return_bytes: bool = False):
    b = create_image(graph=graph)
    if return_bytes:
        return b
    else:
        from IPython.core.display import Image

        return Image(b)


def pipeline_graph_to_image(
    pipeline: Union["Pipeline", "PipelineConfig", "PipelineStructure"],
    graph_type: Literal["data-flow", "data-flow-simple", "execution"] = "execution",
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
    else:
        raise Exception(
            f"Invalid graph type '{graph_type}': must be one of 'data-flow', 'data-flow-simple', 'execution'"
        )

    return graph_to_image(graph=graph, return_bytes=return_bytes)


def graph_widget(
    pipeline: Union["PipelineStructure", "PipelineConfig", "PipelineStructure"],
    graph_type: Literal["data-flow", "data-flow-simple", "execution"] = "execution",
):

    if hasattr(pipeline, "structure"):
        pipeline = pipeline.structure  # type: ignore

    import ipydagred3 as ipydagred3

    g = ipydagred3.Graph()
    if graph_type == "execution":
        graph = pipeline.execution_graph  # type: ignore
    elif graph_type == "data-flow":
        graph = pipeline.data_flow_graph  # type: ignore
    elif graph_type == "data-flow-simple":
        graph = pipeline.data_flow_graph_simple  # type: ignore

    nodes_set = set()
    for node in graph.nodes:
        nodes_set.add(str(node))
        g.setNode(str(node))

    for edge in graph.edges:
        e = str(edge[0])
        if e not in nodes_set:
            print("MISSING")
            print(e)
        e2 = str(edge[1])
        if e2 not in nodes_set:
            print("MISSING 2")
            print(e2)
        g.setEdge(e, e2)

    widget = ipydagred3.DagreD3Widget(graph=g)
    return widget
