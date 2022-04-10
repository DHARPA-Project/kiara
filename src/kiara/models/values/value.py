# -*- coding: utf-8 -*-
import abc
import logging
import uuid
from pydantic import PrivateAttr
from pydantic.fields import Field
from rich import box
from rich.console import RenderableType
from rich.table import Table
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Union,
)

from kiara.defaults import (
    NO_MODULE_TYPE,
    UNOLOADABLE_DATA_CATEGORY_ID,
    VALUE_CATEGORY_ID,
    VALUE_PEDIGREE_TYPE_CATEGORY_ID,
    VALUES_CATEGORY_ID,
    VOID_KIARA_ID,
    SpecialValue,
)
from kiara.exceptions import InvalidValuesException
from kiara.models import KiaraModel
from kiara.models.module.manifest import InputsManifest
from kiara.models.module.persistence import LoadConfig
from kiara.models.python_class import PythonClass
from kiara.models.values import ValueStatus
from kiara.models.values.value_schema import ValueSchema
from kiara.utils import StringYAML, is_debug

log = logging.getLogger("kiara")
yaml = StringYAML()

if TYPE_CHECKING:
    from kiara.data_types import DataType
    from kiara.kiara import Kiara
    from kiara.registries.data import DataRegistry


class ValuePedigree(InputsManifest):

    kiara_id: uuid.UUID = Field(
        description="The id of the kiara context a value was created in."
    )
    environments: Dict[str, int] = Field(
        description="References to the runtime environment details a value was created in."
    )

    def _retrieve_id(self) -> str:
        return str(self.model_data_hash)

    def _retrieve_category_id(self) -> str:
        return VALUE_PEDIGREE_TYPE_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return {
            "manifest": self.manifest_hash,
            "inputs": self.inputs_hash,
            # "environments": self.environments
        }

    def __repr__(self):
        return f"ValuePedigree(module_type={self.module_type}, inputs=[{', '.join(self.inputs.keys())}], hash={self.model_data_hash})"

    def __str__(self):
        return self.__repr__()


class ValueDetails(KiaraModel):
    """A wrapper class that manages and retieves value data and its details."""

    value_id: uuid.UUID = Field(description="The id of the value.")

    kiara_id: uuid.UUID = Field(
        description="The id of the kiara context this value belongs to."
    )

    value_schema: ValueSchema = Field(
        description="The schema that was used for this Value."
    )

    value_status: ValueStatus = Field(description="The set/unset status of this value.")
    value_size: int = Field(description="The size of this value, in bytes.")
    value_hash: int = Field(description="The hash of this value.")
    pedigree: ValuePedigree = Field(
        description="Information about the module and inputs that went into creating this value."
    )
    pedigree_output_name: str = Field(
        description="The output name that produced this value (using the manifest inside the pedigree)."
    )
    data_type_class: PythonClass = Field(
        description="The python class that is associtated with this model."
    )

    def _retrieve_id(self) -> str:
        return str(self.value_id)

    def _retrieve_category_id(self) -> str:
        return VALUE_CATEGORY_ID

    # @property
    # def model_data_hash(self) -> int:
    #     return self._retrieve_data_to_hash()

    def _retrieve_data_to_hash(self) -> Any:
        return {"value_type": self.value_schema.type, "value_hash": self.value_hash}

    @property
    def data_type_name(self) -> str:
        return self.value_schema.type

    @property
    def data_type_config(self) -> Mapping[str, Any]:
        return self.value_schema.type_config

    @property
    def is_optional(self) -> bool:
        return self.value_schema.optional

    @property
    def is_valid(self) -> bool:
        """Check whether the current value is valid"""

        if self.is_optional:
            return True
        else:
            return self.value_status == ValueStatus.SET

    @property
    def is_set(self) -> bool:
        return self.value_status in [ValueStatus.SET, ValueStatus.DEFAULT]

    @property
    def value_status_string(self) -> str:
        """Print a human readable short description of this values status."""

        if self.value_status == ValueStatus.DEFAULT:
            return "set (default)"
        elif self.value_status == ValueStatus.SET:
            return "set"
        elif self.value_status == ValueStatus.NONE:
            result = "no value"
        elif self.value_status == ValueStatus.NOT_SET:
            result = "not set"
        else:
            raise Exception(
                f"Invalid internal status of value '{self.value_id}'. This is most likely a bug."
            )

        if self.is_optional:
            result = f"{result} (not required)"
        return result

    def __repr__(self):

        return f"{self.__class__.__name__}(id={self.value_id}, type={self.data_type_name}, status={self.value_status.value})"

    def __str__(self):

        return self.__repr__()


class Value(ValueDetails):

    _value_data: Any = PrivateAttr(default=SpecialValue.NOT_SET)
    _data_retrieved: bool = PrivateAttr(default=False)
    _data_registry: "DataRegistry" = PrivateAttr(default=None)
    _data_type: "DataType" = PrivateAttr(default=None)
    _is_stored: bool = PrivateAttr(default=False)
    _cached_properties: Optional["ValueSet"] = PrivateAttr(default=None)

    property_refs: Mapping[str, uuid.UUID] = Field(
        description="Links to values that are properties of this value.",
        default_factory=dict,
    )
    destiny_details: Mapping[uuid.UUID, str] = Field(
        description="Backlinks to values that this value acts as destiny/or property for.",
        default_factory=dict,
    )

    def add_property(
        self,
        value_id: Union[uuid.UUID, "Value"],
        property_path: str,
        add_origin_to_property_value: bool = True,
    ):

        value = None
        try:
            value_temp = value
            value_id = value_id.value_id
            value = value_temp
        except Exception:
            # in case a Value object was provided
            pass
        finally:
            del value_temp

        if add_origin_to_property_value:
            if value is None:
                value = self._data_registry.get_value(value_id=value_id)  # type: ignore

            if value._is_stored:
                raise Exception(
                    f"Can't add property to value '{self.value_id}': referenced value '{value.value_id}' already locked, so it's not possible to add the property backlink (as requested)."
                )

        if self._is_stored:
            raise Exception(
                f"Can't add property to value '{self.value_id}': value already locked."
            )

        if property_path in self.property_refs.keys():
            raise Exception(
                f"Can't add property to value '{self.value_id}': property '{property_path}' already set."
            )

        self.property_refs[property_path] = value_id  # type: ignore

        if add_origin_to_property_value:
            value.add_destiny_details(
                value_id=self.value_id, destiny_alias=property_path
            )

        self._cached_properties = None

    def add_destiny_details(self, value_id: uuid.UUID, destiny_alias: str):

        if self._is_stored:
            raise Exception(
                f"Can't set destiny_refs to value '{self.value_id}': value already locked."
            )

        self.destiny_details[value_id] = destiny_alias  # type: ignore

    @property
    def data(self) -> Any:
        if not self.is_initialized:
            raise Exception(
                f"Can't retrieve data for value '{self.value_id}': value not initialized yet. This is most likely a bug."
            )
        return self._retrieve_data()

    def _retrieve_data(self) -> Any:

        if self._value_data is not SpecialValue.NOT_SET:
            return self._value_data

        if self.value_status in [ValueStatus.NOT_SET, ValueStatus.NONE]:
            self._value_data = None
            return self._value_data
        elif self.value_status not in [ValueStatus.SET, ValueStatus.DEFAULT]:
            raise Exception(f"Invalid internal state of value '{self.value_id}'.")

        retrieved = self._data_registry.retrieve_value_data(value_id=self.value_id)

        if retrieved is None or isinstance(retrieved, SpecialValue):
            raise Exception(
                f"Can't set value data, invalid data type: {type(retrieved)}"
            )

        self._value_data = retrieved
        self._data_retrieved = True
        return self._value_data

    @property
    def load_config(self) -> Optional[LoadConfig]:
        return self._data_registry.retrieve_load_config(value_id=self.value_id)

    def __repr__(self):

        return f"{self.__class__.__name__}(id={self.value_id}, type={self.data_type_name}, status={self.value_status.value}, initialized={self.is_initialized} optional={self.value_schema.optional})"

    def _set_registry(self, data_registry: "DataRegistry") -> None:
        self._data_registry = data_registry

    @property
    def is_initialized(self) -> bool:
        result = not self.is_set or self._data_registry is not None
        return result

    @property
    def is_stored(self) -> bool:
        return self._is_stored

    @property
    def data_type(self) -> "DataType":

        if self._data_type is not None:
            return self._data_type

        self._data_type = self.data_type_class.get_class()(
            **self.value_schema.type_config
        )
        return self._data_type

    @property
    def property_values(self) -> "ValueSet":

        if self._cached_properties is not None:
            return self._cached_properties

        self._cached_properties = self._data_registry.load_values(self.property_refs)
        return self._cached_properties

    @property
    def property_data(self) -> Mapping[str, Any]:

        return self._data_registry.load_data(self.property_refs)

    def create_renderable(self, **render_config: Any) -> RenderableType:

        from kiara.utils.output import extract_renderable

        show_metadata = render_config.get("show_metadata", True)
        show_pedigree = render_config.get("show_pedigree", False)
        show_load_config = render_config.get("show_load_config", False)
        show_data = render_config.get("show_data", False)

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Key", style="i")
        table.add_column("Value")
        for k in self.__fields__.keys():

            attr = getattr(self, k)
            if k == "pedigree_output_name":
                if not show_pedigree:
                    continue
                if attr == "__void__":
                    continue

            v = None
            if k == "pedigree":
                if not show_pedigree:
                    continue
                if attr == ORPHAN:
                    v = "[i]-- external data --[/i]"
                else:
                    v = extract_renderable(attr)

            elif k == "value_status":
                v = f"[i]-- {attr.value} --[/i]"
            else:
                v = extract_renderable(attr)
            table.add_row(k, v)

        if show_load_config:
            load_config = self._data_registry.retrieve_load_config(self.value_id)
            if load_config is None:
                table.add_row("load_config", "-- no load config (yet?) --")
            else:
                table.add_row("load_config", load_config.create_renderable())

        return table


class UnloadableData(KiaraModel):
    """A special 'marker' model, indicating that the data of value can't be loaded.

    In most cases, the reason this happens is because the current kiara context is missing some value types and/or modules."""

    value: Value = Field(description="A reference to the value.")
    load_config: LoadConfig = Field(description="The load config")

    def _retrieve_id(self) -> str:
        return self.value.model_id

    def _retrieve_category_id(self) -> str:
        return UNOLOADABLE_DATA_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return self.value.model_data_hash


class ValueSet(KiaraModel, MutableMapping[str, Value]):  # type: ignore

    values_schema: Dict[str, ValueSchema] = Field(
        description="The schemas for all the values in this set."
    )

    @property
    def field_names(self) -> Iterable[str]:
        return sorted(self.values_schema.keys())

    @abc.abstractmethod
    def get_value_obj(self, field_name: str) -> Value:
        pass

    @property
    def all_items_valid(self) -> bool:
        for field_name in self.values_schema.keys():
            item = self.get_value_obj(field_name)
            if not item.is_valid:
                return False
        return True

    def check_invalid(self) -> Dict[str, str]:
        """Check whether the value set is invalid, if it is, return a description of what's wrong."""

        invalid: Dict[str, str] = {}
        for field_name in self.values_schema.keys():
            item = self.get_value_obj(field_name)
            if not item.is_valid:
                if item.value_schema.is_required():
                    if not item.is_set:
                        msg = "not set"
                    elif item.value_status == ValueStatus.NONE:
                        msg = "no value"
                    else:
                        msg = "n/a"
                else:
                    msg = "n/a"
                invalid[field_name] = msg
        return invalid

    def get_value_data_for_fields(
        self, *field_names: str, raise_exception_when_unset: bool = False
    ) -> Dict[str, Any]:
        """Return the data for a one or several fields of this ValueSet.

        If a value is unset, by default 'None' is returned for it. Unless 'raise_exception_when_unset' is set to 'True',
        in which case an Exception will be raised (obviously).
        """

        if raise_exception_when_unset:
            unset: List[str] = []
            for k in field_names:
                v = self.get_value_obj(k)
                if not v.is_set:
                    if raise_exception_when_unset:
                        unset.append(k)
            if unset:
                raise Exception(
                    f"Can't get data for fields, one or several of the requested fields are not set yet: {', '.join(unset)}."
                )

        result: Dict[str, Any] = {}
        for k in field_names:
            v = self.get_value_obj(k)
            if not v.is_set:
                result[k] = None
            else:
                result[k] = v.data
        return result

    def get_value_data(
        self, field_name: str, raise_exception_when_unset: bool = False
    ) -> Any:
        return self.get_value_data_for_fields(
            field_name, raise_exception_when_unset=raise_exception_when_unset
        )[field_name]

    def get_all_value_ids(self) -> Dict[str, uuid.UUID]:
        return {k: self.get_value_obj(k).value_id for k in self.field_names}

    def get_all_value_data(
        self, raise_exception_when_unset: bool = False
    ) -> Dict[str, Any]:
        return self.get_value_data_for_fields(
            *self.field_names,
            raise_exception_when_unset=raise_exception_when_unset,
        )

    def set_values(self, **values) -> None:

        for k, v in values.items():
            self.set_value(k, v)

    def set_value(self, field_name: str, data: Any) -> None:
        raise Exception(
            f"The value set implementation '{self.__class__.__name__}' is read-only, and does not support the setting or changing of values."
        )

    def __getitem__(self, item: str) -> Value:

        return self.get_value_obj(item)

    def __setitem__(self, key: str, value):

        raise NotImplementedError()
        # self.set_value(key, value)

    def __delitem__(self, key: str):

        raise Exception(f"Removing items not supported: {key}")

    def __iter__(self):
        return iter(self.field_names)

    def __len__(self):
        return len(list(self.values_schema))

    def __repr__(self):
        return f"{self.__class__.__name__}(field_names={self.field_names})"

    def __str__(self):
        return self.__repr__()

    def create_renderable(self, **config: Any) -> RenderableType:

        render_value_data = config.get("render_value_data", True)

        table = Table(show_lines=False, show_header=True, box=box.SIMPLE)
        table.add_column("field", style="b")
        table.add_column("value", style="i")

        for field_name in self.field_names:
            value = self.get_value_obj(field_name=field_name)
            if render_value_data:
                rendered = value._data_registry.render_data(
                    value_id=value.value_id, target_type="terminal_renderable", **config
                )
            else:
                rendered = value.create_renderable(**config)
            table.add_row(field_name, rendered)

        return table


class ValueSetReadOnly(ValueSet):  # type: ignore
    @classmethod
    def create_from_ids(cls, data_registry: "DataRegistry", **value_ids: uuid.UUID):

        values = {k: data_registry.get_value(v) for k, v in value_ids.items()}
        return ValueSetReadOnly.construct(value_items=values)

    value_items: Dict[str, Value] = Field(
        description="The values contained in this set."
    )

    def _retrieve_id(self) -> str:
        return str(uuid.uuid4())

    def _retrieve_category_id(self) -> str:
        return VALUES_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return {
            k: self.get_value_obj(k).model_data_hash for k in self.values_schema.keys()
        }

    def get_value_obj(self, field_name: str) -> Value:

        if field_name not in self.value_items.keys():
            raise KeyError(
                f"Field '{field_name}' not available in value set. Available fields: {', '.join(self.field_names)}"
            )
        return self.value_items[field_name]


class ValueSetWritable(ValueSet):  # type: ignore
    @classmethod
    def create_from_schema(
        cls, kiara: "Kiara", schema: Mapping[str, ValueSchema], pedigree: ValuePedigree
    ) -> "ValueSetWritable":

        v = ValueSetWritable(values_schema=schema, pedigree=pedigree)
        v._data_registry = kiara.data_registry
        return v

    value_items: Dict[str, Value] = Field(
        description="The values contained in this set.", default_factory=dict
    )
    pedigree: ValuePedigree = Field(
        description="The pedigree to add to all of the result values."
    )

    _values_uncommitted: Dict[str, Any] = PrivateAttr(default_factory=dict)
    _data_registry: "DataRegistry" = PrivateAttr(default=None)
    _auto_commit: bool = PrivateAttr(default=True)

    def _retrieve_id(self) -> str:
        return str(uuid.uuid4())

    def _retrieve_category_id(self) -> str:
        return VALUES_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return {
            k: self.get_value_obj(k).model_data_hash for k in self.values_schema.keys()
        }

    def get_value_obj(self, field_name: str) -> Value:
        """Retrieve the value object for the specified field.

        This class only creates the actual value object the first time it is requested, because there is a potential
        cost to assembling it, and it might not be needed ever.
        """

        if field_name not in self.values_schema.keys():
            raise Exception(
                f"Can't set data for field '{field_name}': field not valid, valid field names: {', '.join(self.field_names)}."
            )

        if field_name in self.value_items.keys():
            return self.value_items[field_name]
        elif field_name not in self._values_uncommitted.keys():
            raise Exception(
                f"Can't retrieve value for field '{field_name}': value not set (yet)."
            )

        schema = self.values_schema[field_name]
        value_data = self._values_uncommitted[field_name]
        if isinstance(value_data, Value):
            value = value_data
        elif isinstance(value_data, uuid.UUID):
            value = self._data_registry.get_value(value_data)
        else:
            value = self._data_registry.register_data(
                data=value_data,
                schema=schema,
                pedigree=self.pedigree,
                pedigree_output_name=field_name,
                reuse_existing=False,
            )

        self._values_uncommitted.pop(field_name)
        self.value_items[field_name] = value
        return self.value_items[field_name]

    def sync_values(self):

        for field_name in self.field_names:
            self.get_value_obj(field_name)

        invalid = self.check_invalid()
        if invalid:
            if is_debug():
                import traceback

                traceback.print_stack()
            raise InvalidValuesException(invalid_values=invalid)

    def set_value(self, field_name: str, data: Any) -> None:
        """Set the value for the specified field."""

        if field_name not in self.field_names:
            raise Exception(
                f"Can't set data for field '{field_name}': field not valid, valid field names: {', '.join(self.field_names)}."
            )
        if self.value_items.get(field_name, False):
            raise Exception(
                f"Can't set data for field '{field_name}': field already committed."
            )
        if self._values_uncommitted.get(field_name, None) is not None:
            raise Exception(
                f"Can't set data for field '{field_name}': field already set."
            )

        self._values_uncommitted[field_name] = data
        if self._auto_commit:
            self.get_value_obj(field_name=field_name)


ValuePedigree.update_forward_refs()
ORPHAN = ValuePedigree(
    kiara_id=VOID_KIARA_ID, environments={}, module_type=NO_MODULE_TYPE, inputs={}
)
# GENESIS_PEDIGREE = None
