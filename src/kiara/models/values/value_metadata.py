# -*- coding: utf-8 -*-
import abc
from pydantic import Field
from typing import TYPE_CHECKING, Any, Dict, Iterable, Union

from kiara.defaults import VALUE_METADATA_CATEGORY_ID
from kiara.models import KiaraModel
from kiara.models.python_class import PythonClass

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

    def _retrieve_category_id(self) -> str:
        return f"{VALUE_METADATA_CATEGORY_ID}.{self._metadata_key}"  # type: ignore

    def _retrieve_data_to_hash(self) -> Any:
        return {"metadata": self.dict(), "schema": self.schema_json()}


class PythonClassMetadata(ValueMetadata):
    @classmethod
    def retrieve_supported_data_types(cls) -> Iterable[str]:
        return ["any"]

    @classmethod
    def create_value_metadata(cls, value: "Value") -> "PythonClassMetadata":

        return PythonClassMetadata.construct(
            python_class=PythonClass.from_class(value.data.__class__)
        )

    # metadata_key: Literal["python_class"]
    python_class: PythonClass = Field(
        description="Details about the Python class that backs this value."
    )
