# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import orjson
import structlog
from abc import abstractmethod
from pydantic import PrivateAttr
from rich import box
from rich.console import RenderableType
from rich.syntax import Syntax
from rich.table import Table
from typing import Any, Dict, Mapping, Union, get_args

from kiara.defaults import DEFAULT_ENV_HASH_KEY, ENVIRONMENT_TYPE_CATEGORY_ID
from kiara.models import KiaraModel
from kiara.utils.hashing import compute_cid
from kiara.utils.json import orjson_dumps
from kiara.utils.output import extract_renderable

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

    _env_hashes: Union[Mapping[str, str], None] = PrivateAttr(default=None)

    def _create_renderable_for_field(
        self, field_name: str, for_summary: bool = False
    ) -> Union[RenderableType, None]:

        return extract_renderable(getattr(self, field_name))

    def _retrieve_id(self) -> str:
        return self.__class__.get_environment_type_name()

    @property
    def env_hashes(self) -> Mapping[str, str]:

        if self._env_hashes is not None:
            return self._env_hashes

        result = {}
        for k, v in self._retrieve_sub_profile_env_data().items():
            _, cid = compute_cid(data=v)
            result[k] = str(cid)

        self._env_hashes = result
        return self._env_hashes

    def _retrieve_sub_profile_env_data(self) -> Mapping[str, Any]:
        """Return a dictionary with data that identifies one hash profile per key.

        In most cases, this will only return a single-key dictionary containing all the data in that environment. But
        in some cases one might want to have several sub-hash profiles, for example a list of Python packages with and
        without version information, to have more fine-grained control about when to consider two environments functionally
        equal.
        """

        return {DEFAULT_ENV_HASH_KEY: self._retrieve_data_to_hash()}

    def create_renderable(self, **config: Any) -> RenderableType:

        summary = config.get("summary", False)

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("field")
        table.add_column("summary")

        hashes_str = orjson_dumps(self.env_hashes, option=orjson.OPT_INDENT_2)
        table.add_row(
            "environment hashes", Syntax(hashes_str, "json", background_color="default")
        )

        for field_name, field in self.__fields__.items():
            summary_item = self._create_renderable_for_field(
                field_name, for_summary=summary
            )
            if summary_item is not None:
                table.add_row(field_name, summary_item)

        return table
