# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import networkx as nx
from pydantic.main import BaseModel
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Optional, Type

from kiara.defaults import KIARA_DEFAULT_ROOT_NODE_ID, PYDANTIC_USE_CONSTRUCT
from kiara.utils import log_message

if TYPE_CHECKING:
    from kiara.models import KiaraModel


def create_pydantic_model(
    model_cls: Type[BaseModel],
    _use_pydantic_construct: bool = PYDANTIC_USE_CONSTRUCT,
    **field_values: Any,
):

    if _use_pydantic_construct:
        raise NotImplementedError()
        return model_cls.construct(**field_values)
    else:
        return model_cls(**field_values)


def retrieve_data_subcomponent_keys(data: Any) -> Iterable[str]:

    if hasattr(data, "__custom_root_type__") and data.__custom_root_type__:
        if isinstance(data.__root__, Mapping):  # type: ignore
            result = set()
            for k, v in data.__root__.items():  # type: ignore
                if isinstance(v, BaseModel):
                    result.add(k.split(".")[0])
            return result
        else:
            return []
    elif isinstance(data, BaseModel):
        matches = []
        for x in data.__fields__.keys():
            _type = data.__fields__[x].type_
            if isinstance(_type, type) and issubclass(_type, BaseModel):
                matches.append(x)
        return matches
    else:
        log_message(
            f"No subcomponents retrieval supported for data of type: {type(data)}"
        )
        return []


def get_subcomponent_from_model(data: "KiaraModel", path: str) -> "KiaraModel":
    """Return subcomponents of a model under a specified path."""

    if "." in path:
        first_token, rest = path.split(".", maxsplit=1)
        sc = data.get_subcomponent(first_token)
        return sc.get_subcomponent(rest)

    if hasattr(data, "__custom_root_type__") and data.__custom_root_type__:
        if isinstance(data.__root__, Mapping):  # type: ignore
            if path in data.__root__.keys():  # type: ignore
                return data.__root__[path]  # type: ignore
            else:
                matches = {}
                for k in data.__root__.keys():  # type: ignore
                    if k.startswith(f"{path}."):
                        rest = k[len(path) + 1 :]  # noqa
                        matches[rest] = data.__root__[k]  # type: ignore

                if not matches:
                    raise KeyError(f"No child models under '{path}'.")
                else:
                    raise NotImplementedError()
                    # subcomponent_group = KiaraModelGroup.create_from_child_models(**matches)
                    # return subcomponent_group

        else:
            raise NotImplementedError()
    else:
        if path in data.__fields__.keys():
            return getattr(data, path)
        else:
            raise KeyError(
                f"No subcomponent for key '{path}' in model: {data.model_id}."
            )


def assemble_subcomponent_tree(data: "KiaraModel") -> Optional[nx.DiGraph]:

    graph = nx.DiGraph()

    def assemble_tree(info_model: KiaraModel, current_node_id, level: int = 0):
        graph.add_node(current_node_id, obj=info_model, level=level)
        scn = info_model.subcomponent_keys
        if not scn:
            return
        for child_path in scn:
            child_obj = info_model.get_subcomponent(child_path)
            new_node_id = f"{current_node_id}.{child_path}"
            graph.add_edge(current_node_id, new_node_id)
            assemble_tree(child_obj, new_node_id, level + 1)

    assemble_tree(data, KIARA_DEFAULT_ROOT_NODE_ID)
    return graph
