# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Union,
)

from kiara.models import KiaraModel

# from kiara.models.info import TypeInfo

if TYPE_CHECKING:
    from kiara.models.values.value import Value


class ValueMetadata(KiaraModel):
    @classmethod
    @abc.abstractmethod
    def retrieve_supported_data_types(cls) -> Iterable[str]:
        pass

    @classmethod
    @abc.abstractmethod
    def create_value_metadata(
        cls, value: "Value"
    ) -> Union["ValueMetadata", Dict[str, Any]]:
        pass

    # @property
    # def metadata_key(self) -> str:
    #     return self._metadata_key  # type: ignore  # this is added by the kiara class loading functionality

    def _retrieve_id(self) -> str:
        return self._metadata_key  # type: ignore

    def _retrieve_data_to_hash(self) -> Any:
        return {"metadata": self.model_dump(), "schema": self.schema_json()}
