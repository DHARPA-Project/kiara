# -*- coding: utf-8 -*-
import networkx as nx
from IPython.core.display import Image


def graph_to_image(graph: nx.Graph):
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
    return Image(b)
