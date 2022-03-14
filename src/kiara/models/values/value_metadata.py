import abc
from typing import Any, Iterable, TYPE_CHECKING

from pydantic import Field

from kiara.defaults import VALUE_METADATA_CATEGORY_ID
from kiara.models import KiaraModel
from kiara.models.python_class import PythonClass
if TYPE_CHECKING:
    from kiara.models.values.value import Value


class ValueMetadata(KiaraModel):

    @classmethod
    @abc.abstractmethod
    def retrieve_supported_value_types(cls) -> Iterable[str]:
        pass

    @classmethod
    @abc.abstractmethod
    def create_value_metadata(cls, value: "Value") -> "ValueMetadata":
        pass

    def _retrieve_id(self) -> str:
        return self.metadata_key

    def _retrieve_category_id(self) -> str:
        return VALUE_METADATA_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return {
            "metadata": self.dict(),
            "schema": self.schema_json()
        }

class PythonClassMetadata(ValueMetadata):

    @classmethod
    def retrieve_supported_value_types(cls) -> Iterable[str]:
        return ["any"]

    @classmethod
    def create_value_metadata(cls, value: "Value") -> "PythonClassMetadata":

        return PythonClassMetadata.construct(python_class=PythonClass.from_class(value.data.__class__))

    python_class: PythonClass = Field(description="Details about the Python class that backs this value.")
