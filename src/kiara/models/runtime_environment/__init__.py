# -*- coding: utf-8 -*-
import inspect
import structlog
from abc import abstractmethod
from pydantic.fields import Field
from pydantic.main import BaseModel, create_model
from typing import Any, Dict, Iterable, Mapping, Optional, Type, get_args

from kiara.defaults import ENVIRONMENT_TYPE_CATEGORY_ID
from kiara.models import KiaraModel
from kiara.utils import _get_all_subclasses, is_debug, to_camel_case

logger = structlog.get_logger()


class RuntimeEnvironment(KiaraModel):
    class Config:
        underscore_attrs_are_private = False
        allow_mutation = False

    @classmethod
    def get_environment_type_name(cls) -> str:

        env_type = cls.__fields__["environment_type"]
        args = get_args(env_type.type_)
        assert len(args) == 1

        return args[0]

    @classmethod
    def create_environment_model(cls):

        try:
            type_name = cls.get_environment_type_name()
            data = cls.retrieve_environment_data()
            assert (
                "environment_type" not in data.keys()
                or data["environment_keys"] == type_name
            )
            data["environment_type"] = type_name

        except Exception as e:
            raise Exception(f"Can't create environment model for '{cls.__name__}': {e}")

        return cls(**data)

    def get_category_alias(self) -> str:
        return f"{ENVIRONMENT_TYPE_CATEGORY_ID}.{self.environment_type}"  # type: ignore

    @classmethod
    @abstractmethod
    def retrieve_environment_data(cls) -> Dict[str, Any]:
        pass

    def _retrieve_id(self) -> str:
        return self.__class__.get_environment_type_name()

    def _retrieve_category_id(self) -> str:
        return ENVIRONMENT_TYPE_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return self.dict()

    def as_dict_with_schema(self):
        return {
            "data": self.dict(),
            "schema": self.schema()
        }


class RuntimeEnvironmentMgmt(object):

    _instance = None

    @classmethod
    def instance(cls):
        """The default *kiara* context. In most cases, it's recommended you create and manage your own, though."""

        if cls._instance is None:
            cls._instance = RuntimeEnvironmentMgmt()
        return cls._instance

    def __init__(
        self,
    ):
        self._environments: Optional[Dict[str, RuntimeEnvironment]] = None

        self._full_env_model: Optional[BaseModel] = None

    def get_environment_for_hash(self, env_hash: int) -> RuntimeEnvironment:

        envs = [env for env in self.environments.values() if env.model_data_hash == env_hash]
        if len(envs) == 0:
            raise Exception(f"No environment with hash '{env_hash}' available.")
        elif len(envs) > 1:
            raise Exception(f"Multipe environments with hash '{env_hash}' available. This is most likely a bug.")
        return envs[0]

    @property
    def environments(self) -> Mapping[str, RuntimeEnvironment]:
        """Return all environments in this kiara runtime context."""

        if self._environments is not None:
            return self._environments

        import kiara.models.runtime_environment.operating_system  # noqa
        import kiara.models.runtime_environment.python  # noqa

        subclasses: Iterable[Type[RuntimeEnvironment]] = _get_all_subclasses(
            RuntimeEnvironment
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

        self._environments = envs
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
            hashes[k] = self.environments[k].model_data_hash

        cls: typing.Type[BaseModel] = create_model("KiaraRuntimeInfo", **models)  # type: ignore
        data = {}
        for k2, v2 in self.environments.items():
            d = v2.dict()
            assert "metadata_hash" not in d.keys()
            assert "metadata_schema" not in d.keys()
            d["metadata_hash"] = hashes[k2]
            d["metadata_schema"] = schemas[k]
            data[k2] = d
        model = cls.construct(**data)  # type: ignore
        self._full_env_model = model
        return self._full_env_model
