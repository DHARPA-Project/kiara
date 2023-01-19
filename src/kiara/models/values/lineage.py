# -*- coding: utf-8 -*-
from networkx import DiGraph
from rich.console import Console, ConsoleOptions, RenderableType, RenderResult
from rich.jupyter import JupyterMixin
from rich.tree import Tree
from typing import TYPE_CHECKING, Any, Union

from kiara.models.values.value import ORPHAN, Value, ValuePedigree

if TYPE_CHECKING:
    from kiara.context import Kiara

COLOR_LIST = [
    "green",
    "blue",
    "bright_magenta",
    "dark_red",
    "gold3",
    "cyan",
    "orange1",
    "light_yellow3",
    "light_slate_grey",
    "deep_pink4",
]


def fill_renderable_lineage_tree(
    kiara: "Kiara",
    pedigree: ValuePedigree,
    node: Union[Tree, None] = None,
    include_ids: bool = False,
    level: int = 0,
):

    color = COLOR_LIST[level % len(COLOR_LIST)]
    title = f"[b {color}]{pedigree.module_type}[/b {color}]"
    if node is None:
        main = Tree(title)
    else:
        main = node.add(title)

    for input_name in sorted(pedigree.inputs.keys()):

        child_value_id = pedigree.inputs[input_name]

        child_value = kiara.data_registry.get_value(child_value_id)

        value_type = child_value.data_type_name
        if include_ids:
            v_id_str = f" = {child_value.value_id}"
        else:
            v_id_str = ""
        input_node = main.add(
            f"input: [i {color}]{input_name} ({value_type})[/i {color}]{v_id_str}"
        )
        if child_value.pedigree != ORPHAN:
            fill_renderable_lineage_tree(
                kiara=kiara,
                pedigree=child_value.pedigree,
                node=input_node,
                level=level + 1,
                include_ids=include_ids,
            )

    return main


def create_lineage_graph(
    kiara: "Kiara",
    value: Value,
    graph: Union[DiGraph, None] = None,
    parent: Union[None, str] = None,
    level: int = 1,
) -> DiGraph:

    if graph is None:
        graph = DiGraph()
        graph.add_node(
            f"value:{value.value_id}",
            data_type=value.data_type_name,
            label="root_value",
            node_type="value",
            data_type_config=value.data_type_config,
            level=1,
        )
        parent = f"value:{value.value_id}"

    module_id = f"module:{value.pedigree.job_hash}"
    module_label = f"module:{value.pedigree.module_type}"
    graph.add_node(
        module_id,
        module_type=value.pedigree.module_type,
        module_config=value.pedigree.module_config,
        label=module_label,
        node_type="operation",
        level=(level * 2) + 1,
    )
    graph.add_edge(
        parent,
        module_id,
        id=f"{parent}:{module_id}",
        field_name=value.pedigree_output_name,
        label=value.pedigree_output_name,
    )

    for input_name in sorted(value.pedigree.inputs.keys()):

        child_value_id = value.pedigree.inputs[input_name]
        child_value = kiara.data_registry.get_value(child_value_id)

        input_id = f"value:{child_value.value_id}"
        input_label = f"{input_name}:{input_name}"

        graph.add_node(
            input_id,
            label=input_label,
            node_type="value",
            data_type=child_value.data_type_name,
            data_type_config=child_value.data_type_config,
            level=(level * 2) + 2,
        )
        graph.add_edge(
            module_id,
            input_id,
            id=f"{module_id}:{input_id}",
            field_name=input_name,
            label=input_name,
        )

        if child_value.pedigree != ORPHAN:
            create_lineage_graph(
                kiara=kiara,
                value=child_value,
                graph=graph,
                parent=input_id,
                level=level + 1,
            )
    return graph


def create_lineage_graph_modules(
    kiara: "Kiara",
    value: Value,
    graph: Union[DiGraph, None] = None,
    parent: Union[None, str] = None,
    input_field: Union[None, str] = None,
    level: int = 1,
) -> DiGraph:

    if graph is None:
        graph = DiGraph()
        graph.add_node(
            f"value:{value.value_id}",
            data_type=value.data_type_name,
            label="[this value]",
            node_type="value",
            data_type_config=value.data_type_config,
            level=1,
        )

    module_id = f"module:{value.pedigree.job_hash}"
    module_label = value.pedigree.module_type
    graph.add_node(
        module_id,
        module_type=value.pedigree.module_type,
        module_config=value.pedigree.module_config,
        label=module_label,
        node_type="operation",
        level=(level * 2) + 1,
    )

    if parent is None:
        parent = f"value:{value.value_id}"
        graph.add_edge(
            parent,
            module_id,
            id=f"{parent}:{module_id}",
            field_name=value.pedigree_output_name,
            label=f"{value.pedigree_output_name} ({value.data_type_name})",
        )
    else:
        assert input_field is not None
        graph.add_edge(
            parent,
            module_id,
            id=f"{parent}:{input_field}",
            field_name=input_field,
            label=f"{input_field} ({value.data_type_name})",
        )

    for input_name in sorted(value.pedigree.inputs.keys()):

        child_value_id = value.pedigree.inputs[input_name]
        child_value = kiara.data_registry.get_value(child_value_id)

        if child_value.pedigree != ORPHAN:
            create_lineage_graph_modules(
                kiara=kiara,
                value=child_value,
                graph=graph,
                parent=module_id,
                input_field=input_name,
                level=level + 1,
            )
        else:
            input_id = f"value:{child_value.value_id}"
            input_label = f"{input_name} ({child_value.data_type_name})"

            graph.add_node(
                input_id,
                label=input_label,
                node_type="value",
                data_type=child_value.data_type_name,
                data_type_config=child_value.data_type_config,
                level=(level * 2) + 2,
            )
            graph.add_edge(
                module_id,
                input_id,
                id=f"{module_id}:{input_id}",
                field_name=input_name,
                label=f"{input_name} ({child_value.data_type_name})",
            )

    return graph


class ValueLineage(JupyterMixin):
    # @classmethod
    # def from_value(cls, value: Value) -> "ValueLineage":
    #     pass

    def __init__(self, kiara: "Kiara", value: Value) -> None:

        self._value: Value = value
        self._kiara: Kiara = kiara
        self._full_graph: Union[None, DiGraph] = None
        self._module_graph: Union[None, DiGraph] = None

    @property
    def full_graph(self) -> DiGraph:

        if self._full_graph is not None:
            return self._full_graph

        self._full_graph = create_lineage_graph(kiara=self._kiara, value=self._value)
        return self._full_graph

    @property
    def module_graph(self) -> DiGraph:

        if self._module_graph is not None:
            return self._module_graph

        self._module_graph = create_lineage_graph_modules(
            kiara=self._kiara, value=self._value
        ).reverse()
        return self._module_graph

    def create_renderable(self, **config: Any) -> RenderableType:

        include_ids: bool = config.get("include_ids", True)
        tree = fill_renderable_lineage_tree(
            kiara=self._kiara, pedigree=self._value.pedigree, include_ids=include_ids
        )
        return tree

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        yield self.create_renderable()
