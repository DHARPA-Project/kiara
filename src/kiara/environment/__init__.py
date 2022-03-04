# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import abc
import inspect
import logging
import typing
from deepdiff import DeepHash
from pydantic import BaseModel, Field, create_model

from kiara.defaults import ENVIRONMENT_TYPE_CATEGORY_ALIAS
from kiara.metadata.core_models import HashedMetadataModel
from kiara.utils import _get_all_subclasses, is_debug, to_camel_case

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara

log = logging.getLogger("kiara")


class RuntimeEnvironmentConfig(BaseModel):
    class Config:
        allow_mutation = False

    include_all_info: bool = Field(
        default=False,
        description="Whether to include all properties, even if it might include potentially private/sensitive information. This is mainly used for debug purposes.",
    )


class BaseRuntimeEnvironment(HashedMetadataModel):
    class Config:
        underscore_attrs_are_private = False
        allow_mutation = False

    @classmethod
    def get_type_name(cls) -> str:

        env_type = cls.__fields__["environment_type"]
        args = typing.get_args(env_type.type_)
        assert len(args) == 1

        return args[0]

    @classmethod
    def create_environment_model(cls, config: RuntimeEnvironmentConfig):

        try:
            type_name = cls.get_type_name()
            data = cls.retrieve_environment_data(config=config)
            assert (
                "environment_type" not in data.keys()
                or data["environment_keys"] == type_name
            )
            data["environment_type"] = type_name

        except Exception as e:
            raise Exception(f"Can't create environment model for '{cls.__name__}': {e}")

        return cls(**data)

    def get_category_alias(self) -> str:
        return f"{ENVIRONMENT_TYPE_CATEGORY_ALIAS}.{self.environment_type}"  # type: ignore

    def _obj_to_hash(self) -> typing.Any:
        return self.dict()

    @classmethod
    @abc.abstractmethod
    def retrieve_environment_data(
        cls, config: RuntimeEnvironmentConfig
    ) -> typing.Dict[str, typing.Any]:
        pass

    def calculate_runtime_hash(self) -> str:

        properties = self.dict()
        dh = DeepHash(properties)
        return dh[properties]


class RuntimeEnvironmentMgmt(object):
    def __init__(
        self,
        kiara: "Kiara",
    ):

        self._kiara: "Kiara" = kiara
        self._config: RuntimeEnvironmentConfig = RuntimeEnvironmentConfig(
            include_all_info=True
        )
        self._environments: typing.Optional[
            typing.Dict[str, BaseRuntimeEnvironment]
        ] = None

        self._full_env_model: typing.Optional[BaseModel] = None

    @property
    def environments(self) -> typing.Mapping[str, BaseRuntimeEnvironment]:

        if self._environments is not None:
            return self._environments

        import kiara.environment.operating_system  # noqa
        import kiara.environment.python  # noqa

        subclasses: typing.Iterable[
            typing.Type[BaseRuntimeEnvironment]
        ] = _get_all_subclasses(BaseRuntimeEnvironment)
        envs = {}
        for sc in subclasses:
            if inspect.isabstract(sc):
                if is_debug():
                    log.warning(f"Ignoring abstract subclass: {sc}")
                else:
                    log.debug(f"Ignoring abstract subclass: {sc}")

            name = sc.get_type_name()
            envs[name] = sc.create_environment_model(config=self._config)

        self._environments = envs
        return self._environments

    @property
    def full_model(self) -> BaseModel:

        if self._full_env_model is not None:
            return self._full_env_model

        attrs = {k: (v.__class__, ...) for k, v in self.environments.items()}

        models = {}
        hashes = {}
        schemas = {}

        for k, v in attrs.items():
            name = to_camel_case(f"{k}_environment")
            k_cls: typing.Type[BaseRuntimeEnvironment] = create_model(
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
            hashes[k] = self.environments[k].calculate_runtime_hash()

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
