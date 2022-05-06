# -*- coding: utf-8 -*-
from rich.console import RenderableType
from rich.tree import Tree
from typing import Any, Optional

from kiara.context import Kiara
from kiara.models.values.value import ORPHAN, Value, ValuePedigree

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


def fill_lineage_tree(
    kiara: Kiara,
    pedigree: ValuePedigree,
    node: Optional[Tree] = None,
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
            fill_lineage_tree(
                kiara=kiara,
                pedigree=child_value.pedigree,
                node=input_node,
                level=level + 1,
                include_ids=include_ids,
            )

    return main


class ValueLineage(object):
    @classmethod
    def from_value(cls, value: Value) -> "ValueLineage":

        pass

    def __init__(self, kiara: Kiara, value: Value):

        self._value: Value = value
        self._kiara: Kiara = kiara

    def create_renderable(self, **config: Any) -> RenderableType:

        include_ids: bool = config.get("include_ids", False)
        tree = fill_lineage_tree(
            kiara=self._kiara, pedigree=self._value.pedigree, include_ids=include_ids
        )
        return tree
