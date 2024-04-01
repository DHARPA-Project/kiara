# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


import inspect
from typing import TYPE_CHECKING, Any, Dict, Iterable, Mapping, Type, Union

from pydantic import BaseModel, Field, create_model
from rich import box
from rich.table import Table

from kiara.models.runtime_environment import RuntimeEnvironment, logger
from kiara.utils import _get_all_subclasses, is_debug, to_camel_case

if TYPE_CHECKING:
    pass


class EnvironmentRegistry(object):

    _instance = None

    @classmethod
    def instance(cls) -> "EnvironmentRegistry":
        """The default *kiara* context. In most cases, it's recommended you create and manage your own, though."""
        if cls._instance is None:
            cls._instance = EnvironmentRegistry()
        return cls._instance

    def __init__(self) -> None:

        self._environments: Union[Dict[str, RuntimeEnvironment], None] = None
        self._environment_hashes: Union[Dict[str, Mapping[str, str]], None] = None

        self._full_env_model: Union[BaseModel, None] = None

        # self._kiara: Kiara = kiara

    def get_environment_for_cid(self, env_cid: str) -> RuntimeEnvironment:

        envs = [
            env
            for env in self.environments.values()
            if str(env.instance_cid) == env_cid
        ]
        if len(envs) == 0:
            raise Exception(f"No environment with id '{env_cid}' available.")
        elif len(envs) > 1:
            raise Exception(
                f"Multipe environments with id '{env_cid}' available. This is most likely a bug."
            )
        return envs[0]

    def has_environment(self, env_cid: str) -> bool:

        for env in self.environments.values():
            if str(env.instance_cid) == env_cid:
                return True
        return False

    @property
    def environment_hashes(self) -> Mapping[str, Mapping[str, str]]:

        if self._environment_hashes is not None:
            return self._environment_hashes

        result = {}
        for env_name, env in self.environments.items():
            result[env_name] = env.env_hashes

        self._environment_hashes = result
        return self._environment_hashes

    @property
    def environments(self) -> Mapping[str, RuntimeEnvironment]:
        """Return all environments in this kiara runtime context."""
        if self._environments is not None:
            return self._environments

        import kiara.models.runtime_environment.kiara
        import kiara.models.runtime_environment.operating_system  # nowa
        import kiara.models.runtime_environment.python  # noqa

        subclasses: Iterable[Type[RuntimeEnvironment]] = _get_all_subclasses(
            RuntimeEnvironment  # type: ignore
        )
        envs = {}
        for sc in subclasses:
            if inspect.isabstract(sc):
                if is_debug():
                    logger.warning("class_loading.ignore_subclass", subclass=sc)
                else:
                    logger.debug("class_loading.ignore_subclass", subclass=sc)

            name = sc.get_environment_type_name()
            envs[name] = sc.create_environment_model()

        self._environments = {k: envs[k] for k in sorted(envs.keys())}
        return self._environments

    @property
    def full_model(self) -> BaseModel:
        """A model containing all environment data, incl. schemas and hashes of each sub-environment."""
        if self._full_env_model is not None:
            return self._full_env_model

        attrs = {k: (v.__class__, ...) for k, v in self.environments.items()}

        models = {}
        hashes = {}
        schemas = {}

        for k, v in attrs.items():
            name = to_camel_case(f"{k}_environment")
            k_cls: Type[RuntimeEnvironment] = create_model(
                name,
                __base__=v[0],
                metadata_hash=(
                    str,
                    Field(
                        description="The hash for this metadata (excl. this and the 'metadata_schema' field)."
                    ),
                ),
                metadata_schema=(
                    str,
                    Field(
                        description="JsonSchema describing this metadata (excl. this and the 'metadata_hash' field)."
                    ),
                ),
            )
            models[k] = (
                k_cls,
                Field(description=f"Metadata describing the {k} environment."),
            )
            schemas[k] = v[0].schema_json()
            hashes[k] = self.environments[k].instance_cid

        cls: Type[BaseModel] = create_model("KiaraRuntimeInfo", **models)  # type: ignore
        data = {}
        for k2, v2 in self.environments.items():
            d = v2.model_dump()
            assert "metadata_hash" not in d.keys()
            assert "metadata_schema" not in d.keys()
            d["metadata_hash"] = str(hashes[k2])
            d["metadata_schema"] = schemas[k]
            data[k2] = d
        model = cls(**data)  # type: ignore
        self._full_env_model = model
        return self._full_env_model

    def create_renderable(self, **config: Any):

        full_details = config.get("full_details", False)

        table = Table(show_header=True, box=box.SIMPLE)
        table.add_column("environment key", style="b")
        table.add_column("details")

        for env_name, env in self.environments.items():
            renderable = env.create_renderable(summary=not full_details)
            table.add_row(env_name, renderable)

        return table
