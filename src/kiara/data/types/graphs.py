# -*- coding: utf-8 -*-
import networkx
import networkx as nx
import typing
from networkx import DiGraph

from kiara.data.types import ValueType


class NetworkGraphType(ValueType):
    def validate(cls, value: typing.Any) -> typing.Any:

        if not isinstance(value, networkx.Graph):
            raise ValueError(f"Invalid type '{type(value)}' for graph: {value}")
        return value

    def extract_type_metadata(
        cls, value: typing.Any
    ) -> typing.Mapping[str, typing.Any]:

        graph: nx.Graph = value
        return {
            "directed": isinstance(value, DiGraph),
            "number_of_nodes": len(graph.nodes),
            "number_of_edges": len(graph.edges),
            "density": nx.density(graph),
        }
