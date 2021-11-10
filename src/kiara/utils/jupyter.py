# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import networkx as nx
from IPython.core.display import Image


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
        return Image(b)
