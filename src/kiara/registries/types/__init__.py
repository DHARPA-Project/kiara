# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from bidict import bidict
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Set, Type, Union

from kiara.data_types import DataType
from kiara.defaults import KIARA_ROOT_TYPE_NAME
from kiara.exceptions import DataTypeUnknownException
from kiara.interfaces.python_api.models.info import (
    DataTypeClassesInfo,
    DataTypeClassInfo,
)
from kiara.utils.class_loading import find_all_data_types

if TYPE_CHECKING:
    import networkx as nx

    from kiara.context import Kiara


class TypeRegistry(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._data_types: Union[bidict[str, Type[DataType]], None] = None
        self._data_type_metadata: Dict[str, DataTypeClassInfo] = {}
        self._cached_data_type_objects: Dict[int, DataType] = {}
        # self._registered_python_classes: Dict[Type, typing.List[str]] = None  # type: ignore
        self._type_hierarchy: Union[nx.DiGraph, None] = None
        self._lineages_cache: Dict[str, List[str]] = {}

        self._type_profiles: Union[Dict[str, Mapping[str, Any]], None] = None

    def invalidate_types(self):

        self._data_types = None
        # self._registered_python_classes = None

    def retrieve_data_type(
        self,
        data_type_name: str,
        data_type_config: Union[Mapping[str, Any], None] = None,
    ) -> DataType:

        if data_type_config is None:
            data_type_config = {}
        else:
            data_type_config = dict(data_type_config)

        if data_type_name not in self.data_type_profiles.keys():
            raise Exception(f"Data type name not registered: {data_type_name}")

        data_type: str = self.data_type_profiles[data_type_name]["type_name"]
        type_config = self.data_type_profiles[data_type_name]["type_config"]

        if data_type_config:
            type_config = dict(type_config)
            type_config.update(data_type_config)

        cls = self.get_data_type_cls(type_name=data_type)

        hash = cls._calculate_data_type_hash(type_config)
        if hash in self._cached_data_type_objects.keys():
            return self._cached_data_type_objects[hash]

        result = cls(**type_config)
        assert result.data_type_hash == hash
        self._cached_data_type_objects[result.data_type_hash] = result
        return result

    @property
    def data_type_classes(self) -> bidict[str, Type[DataType]]:

        if self._data_types is not None:
            return self._data_types

        self._data_types = bidict(find_all_data_types())
        profiles: Dict[str, Mapping[str, Any]] = {
            dn: {"type_name": dn, "type_config": {}} for dn in self._data_types.keys()
        }

        for name, cls in self._data_types.items():
            cls_profiles = cls.retrieve_available_type_profiles()
            for profile_name, type_config in cls_profiles.items():
                if profile_name in profiles.keys():
                    raise Exception(f"Duplicate data type profile: {profile_name}")
                profiles[profile_name] = {"type_name": name, "type_config": type_config}

        self._type_profiles = profiles
        return self._data_types

    @property
    def data_type_profiles(self) -> Mapping[str, Mapping[str, Any]]:

        if self._type_profiles is None:
            self.data_type_classes  # noqa
        assert self._type_profiles is not None
        return self._type_profiles

    @property
    def data_type_hierarchy(self) -> "nx.DiGraph":

        if self._type_hierarchy is not None:
            return self._type_hierarchy

        def recursive_base_find(cls: Type, current: Union[List[str], None] = None):

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

        for profile_name, details in self.data_type_profiles.items():

            if not details["type_config"]:
                continue
            if profile_name in bases.keys():
                raise Exception(
                    f"Invalid profile name '{profile_name}': shadowing data type. This is most likely a bug."
                )
            bases[profile_name] = [details["type_name"]]

        import networkx as nx

        hierarchy = nx.DiGraph()
        hierarchy.add_node(KIARA_ROOT_TYPE_NAME)

        for name, _bases in bases.items():
            profile_details = self.data_type_profiles[name]
            cls = self.data_type_classes[profile_details["type_name"]]
            hierarchy.add_node(name, cls=cls)
            if not _bases:
                hierarchy.add_edge(KIARA_ROOT_TYPE_NAME, name)
            else:
                # we only need the first parent, all others will be taken care of by the parent of the parent
                hierarchy.add_edge(_bases[0], name)

        self._type_hierarchy = hierarchy
        return self._type_hierarchy

    def get_sub_hierarchy(self, data_type: str):

        import networkx as nx

        graph: nx.DiGraph = self.data_type_hierarchy

        desc = nx.descendants(graph, data_type)
        desc.add(data_type)
        sub_graph = graph.subgraph(desc)
        return sub_graph

    def get_type_lineage(self, data_type_name: str) -> List[str]:
        """Returns the shortest path between the specified type and the root, in reverse direction starting from the specified type."""

        if data_type_name not in self.data_type_profiles.keys():
            raise DataTypeUnknownException(data_type=data_type_name)

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
            raise Exception(f"No data type '{data_type_name}' registered.")

        import networkx as nx

        desc = nx.descendants(self.data_type_hierarchy, data_type_name)
        return desc

    def is_profile(self, data_type_name: str) -> bool:

        type_config = self.data_type_profiles.get(data_type_name, {}).get(
            "type_config", None
        )
        return True if type_config else False

    def get_profile_parent(self, data_type_name: str) -> Union[None, bool]:
        """Return the parent data type of the specified data type (if that is indeed a profile name).

        If the specified data type is not a profile name, 'None' will be returned.
        """

        return self.data_type_profiles.get(data_type_name, {}).get("type_name", None)

    def get_associated_profiles(
        self, data_type_name: str
    ) -> Mapping[str, Mapping[str, Any]]:

        if data_type_name not in self.data_type_classes.keys():
            raise Exception(f"No data type '{data_type_name}' registered.")

        result = {}
        for profile_name, details in self.data_type_profiles.items():
            if (
                profile_name != data_type_name
                and data_type_name == details["type_name"]
            ):
                result[profile_name] = details

        return result

    @property
    def data_type_names(self) -> List[str]:
        return list(self.data_type_profiles.keys())

    def get_data_type_cls(self, type_name: str) -> Type[DataType]:

        _type_details = self.data_type_profiles.get(type_name, None)
        if _type_details is None:
            raise Exception(
                f"No value type '{type_name}', available types: {', '.join(self.data_type_profiles.keys())}"
            )

        resolved_type_name: str = _type_details["type_name"]

        t = self.data_type_classes.get(resolved_type_name, None)
        if t is None:
            raise Exception(
                f"No value type '{type_name}', available types: {', '.join(self.data_type_profiles.keys())}"
            )
        return t

    def get_data_type_instance(
        self, type_name: str, type_config: Union[None, Mapping[str, Any]] = None
    ) -> DataType:

        cls = self.get_data_type_cls(type_name=type_name)
        if not type_config:
            obj = cls()
        else:
            obj = cls(**type_config)
        return obj

    def get_type_metadata(self, type_name: str) -> DataTypeClassInfo:

        md = self._data_type_metadata.get(type_name, None)
        if md is None:
            md = DataTypeClassInfo.create_from_type_class(
                type_cls=self.get_data_type_cls(type_name=type_name), kiara=self._kiara
            )
            self._data_type_metadata[type_name] = md
        return self._data_type_metadata[type_name]

    def get_context_metadata(
        self, alias: Union[str, None] = None, only_for_package: Union[str, None] = None
    ) -> DataTypeClassesInfo:

        result = {}
        for type_name in self.data_type_classes.keys():
            md = self.get_type_metadata(type_name=type_name)
            if only_for_package:
                if md.context.labels.get("package") == only_for_package:
                    result[type_name] = md
            else:
                result[type_name] = md

        _result = DataTypeClassesInfo.construct(group_alias=alias, item_infos=result)  # type: ignore
        _result._kiara = self._kiara
        return _result

    def is_internal_type(self, data_type_name: str) -> bool:

        if data_type_name not in self.data_type_profiles.keys():
            return False

        lineage = self.get_type_lineage(data_type_name=data_type_name)
        return "any" not in lineage
