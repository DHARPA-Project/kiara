from typing import Dict, Type, Any, TYPE_CHECKING

from pydantic import Field, PrivateAttr

from kiara.models.module.manifest import Manifest
from kiara.models.values.value import Value
from kiara.value_types import ValueTypeConfigSchema, ValueType

if TYPE_CHECKING:
    from kiara.kiara import JobsMgmt


class LoadConfigSchema(ValueTypeConfigSchema):

    persistence_target: str = Field(description="A hint as to the persistence target (e.g. disk).")
    persistence_format: str = Field(description="A hint as to the persistence format (e.g. pickle).")


class LoadConfig(Manifest):

    inputs: Dict[str, Any] = Field(description="The inputs to use to re-load the previously persisted value.")
    output_name: str = Field(description="The name of the field that contains the persisted value details.")

    def _retrieve_data_to_hash(self) -> Any:
        return self.dict()

    def __repr__(self):

        return f"{self.__class__.__name__}(module_type={self.module_type}, output_name={self.output_name})"

    def __str__(self):
        return self.__repr__()


class LoadConfigValue(Value):

    _load_config: LoadConfig = PrivateAttr(default=None)
    _jobs_mgmt: "JobsMgmt" = PrivateAttr(default=None)

    def init_data(self, **data: Any):
        self._load_config = data["load_config"]
        self._jobs_mgmt = data["jobs_mgmt"]

    def is_initialized(self) -> bool:
        return self._load_config is not None and self._jobs_mgmt is not None

    def _retrieve_value_data(self) -> Any:

        data = self._jobs_mgmt.load_data_from_config(load_config=self._load_config)
        return data

class LoadConfigValueType(ValueType[LoadConfig, LoadConfigSchema]):

    _value_type_name = "load_config"

    @classmethod
    def python_class(cls) -> Type:
        return LoadConfig

    @classmethod
    def value_type_config_class(cls) -> Type[ValueTypeConfigSchema]:
        return LoadConfigSchema

    def is_immutable(self) -> bool:
        return True

    def calculate_hash(self, value: LoadConfig) -> int:
        """Calculate the hash of the value."""

        return value.model_data_hash

    def calculate_size(self, value: LoadConfig) -> int:
        return -1
        return value.value_size

    @property
    def persistence_target(self) -> str:
        return self.type_config.persistence_target

    @property
    def persistence_format(self) -> str:
        return self.type_config.persistence_format
