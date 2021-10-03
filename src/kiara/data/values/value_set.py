# -*- coding: utf-8 -*-
import abc
import typing
from rich import box
from rich.console import Console, ConsoleOptions, RenderResult
from rich.panel import Panel
from rich.table import Table

from kiara.data.registry import DataRegistry
from kiara.data.values import Value, ValueSchema, ValueSlot
from kiara.defaults import INVALID_VALUE_NAMES, SpecialValue

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara
    from kiara.pipeline import PipelineValuesInfo


def check_valueset_valid(value_set: typing.Mapping[str, "Value"]) -> bool:

    for field_name, item in value_set.items():
        if not item.item_is_valid():
            return False
    return True


class ValueSet(typing.MutableMapping[str, "Value"]):
    def __init__(
        self,
        read_only: bool,
        title: typing.Optional[str] = None,
        kiara: typing.Optional["Kiara"] = None,
    ):

        # from kiara.data.values import ValueSchema
        # schema = ValueSchema(type="any", default=None, doc="-- n/a --")
        # self._schema = schema

        if kiara is None:
            from kiara.kiara import Kiara

            kiara = Kiara.instance()

        self._kiara: "Kiara" = kiara

        self._read_only: bool = read_only
        if title is None:
            title = "-- n/a --"
        self._title = title

    @abc.abstractmethod
    def get_all_field_names(self) -> typing.Iterable[str]:
        pass

    @abc.abstractmethod
    def _get_value_obj(self, field_name: str):
        pass

    @abc.abstractmethod
    def _set_values(
        self, **values: typing.Any
    ) -> typing.Mapping[str, typing.Union[bool, Exception]]:
        pass

    def get_value_obj(
        self,
        field_name: str,
        ensure_metadata: typing.Union[bool, typing.Iterable[str], str] = False,
    ):

        if field_name not in list(self.get_all_field_names()):
            raise KeyError(
                f"Field '{field_name}' not available in value set. Available fields: {', '.join(self.get_all_field_names())}"
            )

        obj: Value = self._get_value_obj(field_name)

        if ensure_metadata:
            if isinstance(ensure_metadata, bool):
                obj.get_metadata()
            elif isinstance(ensure_metadata, str):
                obj.get_metadata(ensure_metadata)
            elif isinstance(ensure_metadata, typing.Iterable):
                obj.get_metadata(*ensure_metadata)
            else:
                raise ValueError(
                    f"Invalid type '{type(ensure_metadata)}' for 'ensure_metadata' argument."
                )

        return obj

    def set_value(self, key: str, value: typing.Any) -> Value:

        result = self.set_values(**{key: value})
        if isinstance(result[key], Exception):
            raise result[key]  # type: ignore
        return result[key]  # type: ignore

    def set_values(
        self, **values: typing.Any
    ) -> typing.Mapping[str, typing.Union[Value, Exception]]:

        if self.is_read_only():
            raise Exception("Can't set values: this value set is read-only.")

        invalid: typing.List[str] = []

        for k in values.keys():
            if k not in self.get_all_field_names():
                invalid.append(k)

        if invalid:
            raise ValueError(
                f"No field(s) with name(s) {', '.join(invalid)} available, valid names: {', '.join(self.get_all_field_names())}"
            )

        resolved_values = {}
        for field_name, data in values.items():
            if isinstance(data, str) and data.startswith("value:"):
                v = self._kiara.get_value(data)

                resolved_values[field_name] = v
            else:
                resolved_values[field_name] = data

        value_set_result = self._set_values(**resolved_values)

        result: typing.Dict[str, typing.Union[Value, Exception]] = {}
        for field in values.keys():
            if isinstance(value_set_result[field], Exception):
                result[field] = value_set_result[field]  # type: ignore
            else:
                result[field] = self.get_value_obj(field)
        return result

    def is_read_only(self):
        return self._read_only

    def items_are_valid(self) -> bool:

        return check_valueset_valid(self)
        # for field_name in self.get_all_field_names():
        #     item = self.get_value_obj(field_name)
        #     if not item.item_is_valid():
        #         return False
        # return True

    def check_invalid(self) -> typing.Optional[typing.Dict[str, str]]:
        """Check whether the value set is invalid, if it is, return a description of what's wrong."""

        invalid = {}
        for field_name, item in self.items():
            if not item.item_is_valid():
                if item.value_schema.is_required():
                    if not item.is_set:
                        msg = "not set"
                    elif item.is_none:
                        msg = "no value"
                    else:
                        msg = "n/a"
                else:
                    msg = "n/a"
                invalid[field_name] = msg
        return invalid

    def get_all_value_objects(self) -> typing.Mapping[str, typing.Any]:
        return {fn: self.get_value_obj(fn) for fn in self.get_all_field_names()}

    def get_value_data_for_fields(
        self, *field_names: str, raise_exception_when_unset: bool = False
    ) -> typing.Dict[str, typing.Any]:
        """Return the data for a one or several fields of this ValueSet.

        If a value is unset, by default 'None' is returned for it. Unless 'raise_exception_when_unset' is set to 'True',
        in which case an Exception will be raised (obviously).
        """

        result: typing.Dict[str, typing.Any] = {}
        unset: typing.List[str] = []
        for k in field_names:
            v = self.get_value_obj(k)
            if not v.is_set:
                if raise_exception_when_unset:
                    unset.append(k)
                else:
                    result[k] = None
            else:
                data = v.get_value_data()
                result[k] = data

        if unset:
            raise Exception(
                f"Can't get data for fields, one or several of the requested fields are not set yet: {', '.join(unset)}."
            )

        return result

    def get_value_data(
        self, field_name: str, raise_exception_when_unset: bool = False
    ) -> typing.Any:
        return self.get_value_data_for_fields(
            field_name, raise_exception_when_unset=raise_exception_when_unset
        )[field_name]

    def get_all_value_data(
        self, raise_exception_when_unset: bool = False
    ) -> typing.Dict[str, typing.Any]:
        return self.get_value_data_for_fields(
            *self.get_all_field_names(),
            raise_exception_when_unset=raise_exception_when_unset,
        )

    def save_all(self, aliases: typing.Optional[typing.Iterable[str]] = None):

        if aliases:
            aliases = set(aliases)

        for k, v in self.items():
            field_aliases = None
            if aliases:
                field_aliases = [f"{a}__{k}" for a in aliases]
            v.save(aliases=field_aliases)

    def __getitem__(self, item: str) -> "Value":

        return self.get_value_obj(item)

    def __setitem__(self, key: str, value):

        self.set_value(key, value)

    def __delitem__(self, key: str):

        raise Exception(f"Removing items not supported: {key}")

    def __iter__(self) -> typing.Iterator[str]:
        return iter(self.get_all_field_names())

    def __len__(self):
        return len(list(self.get_all_field_names()))

    def to_details(self, ensure_metadata: bool = False) -> "PipelineValuesInfo":

        from kiara.pipeline import PipelineValueInfo, PipelineValuesInfo

        result = {}
        for name in self.get_all_field_names():
            item = self.get_value_obj(name)
            result[name] = PipelineValueInfo.from_value_obj(
                item, ensure_metadata=ensure_metadata
            )

        return PipelineValuesInfo(values=result)

    def _create_rich_table(
        self, show_headers: bool = True, ensure_metadata: bool = False
    ) -> Table:

        table = Table(box=box.SIMPLE, show_header=show_headers)
        table.add_column("Field name", style="i")
        table.add_column("Type")
        table.add_column("Description")
        table.add_column("Required")
        table.add_column("Is set")

        for k in self.get_all_field_names():
            v = self.get_value_obj(k, ensure_metadata=ensure_metadata)
            t = v.value_schema.type
            desc = v.value_schema.doc
            if not v.value_schema.is_required():
                req = "no"
            else:
                if (
                    v.value_schema.default
                    and v.value_schema.default != SpecialValue.NO_VALUE
                    and v.value_schema.default != SpecialValue.NOT_SET
                ):
                    req = "no"
                else:
                    req = "[red]yes[/red]"
            is_set = "yes" if v.item_is_valid() else "no"
            table.add_row(k, t, desc, req, is_set)

        return table

    def __repr__(self):

        title_str = ""
        if self._title:
            title_str = f" title='{self._title}'"
        return f"{self.__class__.__name__}(field_names={list(self.keys())}{title_str})"

    def __str__(self):

        return self.__repr__()

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        title = self._title
        if title:
            postfix = f" [b]{title}[/b]"
        else:
            postfix = ""

        yield Panel(
            self._create_rich_table(show_headers=True),
            box=box.ROUNDED,
            title_align="left",
            title=f"Value-Set:{postfix}",
        )


class SlottedValueSet(ValueSet):
    @classmethod
    def from_schemas(
        cls,
        schemas: typing.Mapping[str, "ValueSchema"],
        read_only: bool = True,
        check_for_sameness=True,
        initial_values: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        title: typing.Optional[str] = None,
        default_value: typing.Any = SpecialValue.NO_VALUE,
        kiara: typing.Optional["Kiara"] = None,
        registry: typing.Optional[DataRegistry] = None,
    ) -> "SlottedValueSet":

        if kiara is None:
            from kiara.kiara import Kiara

            kiara = Kiara.instance()

        if registry is None:
            registry = kiara.data_registry

        values = {}
        for field_name, schema in schemas.items():

            def_val = default_value
            if callable(default_value):
                def_val = default_value()

            _init_value = def_val

            if initial_values and initial_values.get(field_name, None) is not None:
                _init_value = initial_values[field_name]

            if not isinstance(_init_value, Value):
                value: Value = registry.register_data(
                    value_data=_init_value, value_schema=schema
                )
                # value: Value = Value(value_schema=schema, value_data=_init_value, registry=registry)  # type: ignore
            else:
                value = _init_value

            values[field_name] = value

        return cls(
            items=values,
            title=title,
            read_only=read_only,
            check_for_sameness=check_for_sameness,
            kiara=kiara,
            registry=registry,
        )

    def __init__(
        self,
        items: typing.Mapping[str, typing.Union["ValueSlot", "Value"]],
        read_only: bool,
        check_for_sameness: bool = False,
        title: typing.Optional[str] = None,
        kiara: typing.Optional["Kiara"] = None,
        registry: typing.Optional[DataRegistry] = None,
    ):
        """A `ValueSet` implementation that keeps a history of each fields value.

        Arguments:
            items: the value slots
            read_only: whether it is allowed to set new values to fields in this set
            check_for_sameness: whether a check should be performed that checks for equality of the new and old values, if equal, skip the update
            title: An optional title for this value set
            kiara: the kiara context
            registry: the registry to use to register the values to

        """

        if not items:
            raise ValueError("Can't create ValueSet: no values provided")

        self._check_for_sameness: bool = check_for_sameness

        super().__init__(read_only=read_only, title=title, kiara=kiara)

        if registry is None:
            registry = self._kiara.data_registry
        self._registry: DataRegistry = registry

        _value_slots: typing.Dict[str, ValueSlot] = {}
        for item, value in items.items():

            if value is None:
                raise Exception(
                    f"Can't create value set, item '{item}' does not have a value (yet)."
                )

            if item.startswith("_"):
                raise ValueError(f"Value name can't start with '_': {item}")
            if item in INVALID_VALUE_NAMES:
                raise ValueError(f"Invalid value name '{item}'.")

            if isinstance(value, Value):
                slot = self._registry.register_alias(value)
            elif isinstance(value, ValueSlot):
                slot = value
            else:
                raise TypeError(f"Invalid type: '{type(value)}'")

            _value_slots[item] = slot

        self._value_slots: typing.Dict[str, ValueSlot] = _value_slots

    def get_all_field_names(self) -> typing.Iterable[str]:
        return self._value_slots.keys()

    def _get_value_obj(self, field_name: str) -> "Value":

        slot = self._value_slots[field_name]
        return slot.get_latest_value()

    def _set_values(
        self, **values: typing.Any
    ) -> typing.Mapping[str, typing.Union[bool, Exception]]:

        # we want to set all registry-type values seperately, in one go, because it's more efficient
        registries: typing.Dict[
            DataRegistry, typing.Dict[ValueSlot, typing.Union["Value", typing.Any]]
        ] = {}
        field_slot_map: typing.Dict[ValueSlot, str] = {}

        result: typing.Dict[str, typing.Union[bool, Exception]] = {}

        for field_name, value_or_data in values.items():

            value_slot: ValueSlot = self._value_slots[field_name]
            if self._check_for_sameness:
                latest_val = value_slot.get_latest_value()
                if isinstance(value_or_data, Value):
                    if latest_val.id == value_or_data.id:
                        result[field_name] = False
                        continue
                else:
                    if (
                        latest_val.is_set
                        and latest_val.get_value_data() == value_or_data
                    ):
                        result[field_name] = False
                        continue

            registries.setdefault(value_slot._registry, {})[value_slot] = value_or_data
            field_slot_map[value_slot] = field_name

        for registry, value_slots_details in registries.items():

            _r = registry.update_value_slots(value_slots_details)  # type: ignore

            for value_slot, details in _r.items():
                result[field_slot_map[value_slot]] = details

        return result


class ValuesInfo(object):
    def __init__(self, value_set: ValueSet, title: typing.Optional[str] = None):

        self._value_set: ValueSet = value_set
        self._title: typing.Optional[str] = title

    def create_value_info_table(
        self, show_headers: bool = False, ensure_metadata: bool = False
    ) -> Table:

        table = Table(show_header=show_headers, box=box.SIMPLE)
        table.add_column("Field name", style="i")
        table.add_column("Value info")

        for field_name in self._value_set.get_all_field_names():
            details = self._value_set.get_value_obj(field_name)
            if not details.is_set:
                if details.item_is_valid():
                    value_info: typing.Union[
                        str, Table
                    ] = "[green]-- not set --[/green]"
                else:
                    value_info = "[red]-- not set --[/red]"
            elif details.is_none:
                if details.item_is_valid():
                    value_info = "-- no value --"
                else:
                    value_info = "[red]-- no value --[/red]"
            else:
                if ensure_metadata:
                    details.get_metadata()

                value_info = details._create_value_table()
            table.add_row(field_name, value_info)

        return table
