# -*- coding: utf-8 -*-
import networkx as nx


def print_ascii_graph(graph: nx.Graph):

    try:
        from asciinet import graph_to_ascii
    except:  # noqa
        print(
            "\nCan't print graph on terminal, package 'asciinet' not available. Please install it into the current virtualenv using:\n\npip install 'git+https://github.com/cosminbasca/asciinet.git#egg=asciinet&subdirectory=pyasciinet'"
        )
        return

    try:
        from asciinet._libutil import check_java

        check_java("Java ")
    except Exception as e:
        print(e)
        print(
            "\nJava is currently necessary to print ascii graph. This might change in the future, but to use this functionality please install a JRE."
        )
        return

    print(graph_to_ascii(graph))