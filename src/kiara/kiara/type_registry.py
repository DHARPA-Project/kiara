# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
from bidict import bidict
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Type,
)

from kiara.data_types import DataType
from kiara.defaults import KIARA_ROOT_TYPE_NAME
from kiara.utils.class_loading import find_all_data_types

if TYPE_CHECKING:
    import networkx as nx

    from kiara.kiara import Kiara


TYPE_PROFILE_MAP = {
    "csv_file": "file",
    "text_file_bundle": "file_bundle",
    "csv_file_bundle": "file_bundle",
    "table": "table",
    "graphml_file": "file",
    "gexf_file": "file",
    "gml_file": "file",
    "shp_file": "file",
}


class TypeRegistry(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._data_types: Optional[bidict[str, Type[DataType]]] = None
        self._cached_data_type_objects: Dict[int, DataType] = {}
        # self._registered_python_classes: Dict[Type, typing.List[str]] = None  # type: ignore
        self._type_hierarchy: Optional[nx.DiGraph] = None
        self._lineages_cache: Dict[str, List[str]] = {}

    def invalidate_types(self):

        self._data_types = None
        # self._registered_python_classes = None

    def retrieve_data_type(
        self, data_type_name: str, data_type_config: Optional[Mapping[str, Any]] = None
    ) -> DataType:

        if data_type_config is None:
            data_type_config = {}
        else:
            data_type_config = dict(data_type_config)

        cls = self.get_data_type_cls(type_name=data_type_name)

        hash = cls._calculate_data_type_hash(data_type_config)
        if hash in self._cached_data_type_objects.keys():
            return self._cached_data_type_objects[hash]

        result = cls(**data_type_config)
        assert result.data_type_hash == hash
        self._cached_data_type_objects[result.data_type_hash] = result
        return result

    @property
    def data_type_classes(self) -> bidict[str, Type[DataType]]:

        if self._data_types is not None:
            return self._data_types

        self._data_types = bidict(find_all_data_types())

        return self._data_types

    @property
    def data_type_hierarchy(self) -> "nx.DiGraph":

        if self._type_hierarchy is not None:
            return self._type_hierarchy

        def recursive_base_find(cls: Type, current: Optional[List[str]] = None):

            if current is None:
                current = []

            for base in cls.__bases__:

                if base in self.data_type_classes.values():
                    current.append(self.data_type_classes.inverse[base])

                recursive_base_find(base, current=current)

            return current

        bases = {}
        for name, cls in self.data_type_classes.items():
            bases[name] = recursive_base_find(cls)
        import networkx as nx

        hierarchy = nx.DiGraph()
        hierarchy.add_node(KIARA_ROOT_TYPE_NAME)

        for name, _bases in bases.items():
            hierarchy.add_node(name, cls=self.data_type_classes[name])
            if not _bases:
                hierarchy.add_edge(KIARA_ROOT_TYPE_NAME, name)
            else:
                # we only need the first parent, all others will be taken care of by the parent of the parent
                hierarchy.add_edge(_bases[0], name)

        self._type_hierarchy = hierarchy
        return self._type_hierarchy

    def get_type_lineage(self, data_type_name: str) -> Iterable[str]:
        """Returns the shortest path between the specified type and the 'any' type, in reverse direction starting from the specified type."""

        if data_type_name not in self.data_type_classes.keys():
            raise Exception(f"No value type '{data_type_name}' registered.")

        if data_type_name in self._lineages_cache.keys():
            return self._lineages_cache[data_type_name]

        import networkx as nx

        path = nx.shortest_path(
            self.data_type_hierarchy, KIARA_ROOT_TYPE_NAME, data_type_name
        )
        path.remove(KIARA_ROOT_TYPE_NAME)
        self._lineages_cache[data_type_name] = list(reversed(path))
        return self._lineages_cache[data_type_name]

    def get_sub_types(self, data_type_name: str) -> Set[str]:

        if data_type_name not in self.data_type_classes.keys():
            raise Exception(f"No value type '{data_type_name}' registered.")

        import networkx as nx

        desc = nx.descendants(self.data_type_hierarchy, data_type_name)
        return desc

    @property
    def data_type_names(self) -> List[str]:
        return list(self.data_type_classes.keys())

    def render_value(self, value: "Value", render_target: str = "terminal", **config):

        pass

    # @property
    # def registered_python_classes(
    #     self,
    # ) -> Mapping[Type, Iterable[str]]:
    #
    #     if self._registered_python_classes is not None:
    #         return self._registered_python_classes
    #
    #     registered_types = {}
    #     for name, v_type in self.data_type_classes.items():
    #         rel = v_type.candidate_python_types()
    #         if rel:
    #             for cls in rel:
    #                 registered_types.setdefault(cls, []).append(name)
    #
    #     self._registered_python_classes = registered_types
    #     return self._registered_python_classes

    # def get_type_config_for_data_profile(
    #     self, profile_name: str
    # ) -> Mapping[str, Any]:
    #
    #     type_name = TYPE_PROFILE_MAP[profile_name]
    #     return {"type": type_name, "type_config": {}}

    # def determine_type(self, data: Any) -> Optional[ValueTypeOrm]:
    #
    #     if isinstance(data, ValueOrm):
    #         data = data.get_value_data()
    #
    #     result: List[ValueTypeOrm] = []
    #
    #     registered_types = set(self.registered_python_classes.get(data.__class__, []))
    #     for cls in data.__class__.__bases__:
    #         reg = self.registered_python_classes.get(cls)
    #         if reg:
    #             registered_types.update(reg)
    #
    #     if registered_types:
    #         for rt in registered_types:
    #             _cls: Type[ValueTypeOrm] = self.get_value_type_cls(rt)
    #             match = _cls.check_data(data)
    #             if match:
    #                 result.append(match)
    #
    #     # TODO: re-run all checks on all modules, not just the ones that registered interest in the class
    #
    #     if len(result) == 0:
    #         return None
    #     elif len(result) > 1:
    #         result_str = [x._value_type_name for x in result]  # type: ignore
    #         raise Exception(
    #             f"Multiple value data_types found for value: {', '.join(result_str)}."
    #         )
    #     else:
    #         return result[0]

    def get_data_type_cls(self, type_name: str) -> Type[DataType]:

        t = self.data_type_classes.get(type_name, None)
        if t is None:
            raise Exception(
                f"No value type '{type_name}', available types: {', '.join(self.data_type_classes.keys())}"
            )
        return t

    def find_data_type_classes_for_package(
        self, package_name: str
    ) -> Dict[str, Type[DataType]]:

        result = {}
        for data_type_name, data_type_cls in self.data_type_classes.items():

            value_md = data_type_cls.get_type_metadata()
            package = value_md.context.labels.get("package")
            if package == package_name:
                result[data_type_name] = data_type_cls

        return result
