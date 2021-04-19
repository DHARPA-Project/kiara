# -*- coding: utf-8 -*-

import logging
import typing
import uuid

from kiara.data.types import ValueType
from kiara.data.values import (
    DataValue,
    LinkedValue,
    Value,
    ValueField,
    ValueSchema,
    ValueUpdateHandler,
)

log = logging.getLogger("kiara")


def generate_random_value_id():

    return str(uuid.uuid4())


class DataRegistry(object):
    """Contains and manages all [Value][kiara.data.values.Value] objects for *Kiara*.

    This is one of the central classes in *Kiara*, as it manages all data that is set by users or which results from
    processing steps in [KiaraModule][kiara.module.KiaraModule]s. Basically, the registry keeps a record of every ``Value`` object that is produced
    or consumed within *Kiara* by associating it with a unique id. This id then can be used to retrieve or set/replace the
    current data (bytes) for a value, and subscribe to events that happens on such ``Value`` object (which is needed in
    [PipelineController][kiara.pipeline.controller.PipelineController]s).

    Note:
        In the future, this will probably become an abstract base class, so it'll be possible to implement different
        ways of storing/managing values and data.

    """

    def __init__(self):

        self._id: str = str(uuid.uuid4())
        self._value_items: typing.Dict[str, DataValue] = {}
        """PipelineValues that have a actual data associated to it. key is the value id, value is the value wrapper object."""
        self._linked_value_items: typing.Dict[str, LinkedValue] = {}
        """PipelineValues that track one or several other values. key is the value id, value is a dictionary with the tracked value id as key and an optional sub-value query string as value (if not the whole value is used)."""
        self._linked_value_items_reverse: typing.Dict[str, typing.List[str]] = {}
        self._values: typing.Dict[str, typing.Any] = {}
        self._callbacks: typing.Dict[str, typing.List[ValueUpdateHandler]] = {}

    def get_value_item(self, item: typing.Union[str, Value]) -> Value:
        """Get the [Value][kiara.data.values.Value] object for an id.

        If a string is provided, it is interpreted as value id. If a ``Value`` object is provided, the registry will check whether its id is registered with it, and return the object that is registered with it.

        If the provided id could not be found, an Exception is thrown.

        Arguments:
            item: a value id or ``Value`` object

        Returns: the ``Value`` objectssh 1
        """

        if isinstance(item, str):
            value_id = item
        elif isinstance(item, Value):
            value_id = item.id
        else:
            raise TypeError(
                f"Invalid type '{type(item)}', need PipelineValue or string."
            )

        if value_id in self._value_items.keys():
            return self._value_items[value_id]
        elif value_id in self._linked_value_items.keys():
            return self._linked_value_items[value_id]
        else:
            raise Exception(f"No value with id: {value_id}")

    def register_value(
        self,
        value_schema: ValueSchema,
        value_fields: typing.Union[
            ValueField, typing.Iterable[ValueField], None
        ] = None,
        value_id: typing.Optional[str] = None,
        callbacks: typing.Optional[typing.Iterable[ValueUpdateHandler]] = None,
        initial_value: typing.Any = None,
        origin: typing.Optional[str] = None,
        is_constant: bool = False,
    ) -> DataValue:
        """Register a value in this registry.

        This registers an unique id, along with a data schema and other metadata which can then be 'filled' with actual
        data.

        Arguments:
             value_schema: the allowed schema for the data that is held in this value
             value_fields: the field(s) within a [PipelineStructure][kiara.pipeline.structure.PipelineStructure] that is associated with this value
             value_id: the (unique) id for this value, if not provided one will be generated
             callbacks: the callbacks to register for this value (can be added later too)
             initial_value: if provided, this value will be set
             origin: a string describing the type of field the value is coming from (e.g. user input, step output, ...)
             is_constant: whether this value is a constant or not

        Returns:
            the newly created value object
        """

        if value_id is not None and value_id in self._values.keys():
            raise Exception(f"Value id '{id}' already registered.")

        if value_id is None:
            value_id = generate_random_value_id()

        if is_constant and initial_value is None:
            raise Exception("Can't register constant, no initial value provided.")

        if value_fields is None:
            _value_fields: typing.Tuple[ValueField, ...] = tuple()
        elif isinstance(value_fields, ValueField):
            _value_fields = (value_fields,)
        elif isinstance(value_fields, typing.Iterable):
            for vf in value_fields:
                assert isinstance(vf, ValueField)
            _value_fields = tuple(value_fields)  # type: ignore
        else:
            raise TypeError(
                f"Invalid type for 'value_fields' argument: {type(value_fields)}"
            )

        value_item = DataValue(  # type: ignore
            id=value_id,
            value_schema=value_schema,
            value_fields=_value_fields,
            registry=self,  # type: ignore
            origin=origin,
            is_constant=is_constant,
        )

        self._value_items[value_id] = value_item
        self._values[value_id] = None

        if callbacks:
            for cb in callbacks:
                self.register_callback(cb, value_item)

        if initial_value is not None:
            self.set_value(value_id, initial_value)

        return value_item

    def register_linked_value(
        self,
        linked_values: typing.Union[
            typing.Dict[str, typing.Dict[str, str]],
            str,
            Value,
            typing.Iterable[typing.Union[str, Value]],
        ],
        value_fields: typing.Union[
            ValueField, typing.Iterable[ValueField], None
        ] = None,
        value_id: typing.Optional[str] = None,
        callbacks: typing.Optional[typing.Iterable[ValueUpdateHandler]] = None,
        origin: typing.Optional[str] = None,
    ) -> LinkedValue:
        """Register a linked value in this registry.

        This registers an unique id, along with one or several other, already existing 'parent' ``Value`` objects. The
        'value' of the resulting [LinkedValue][kiara.data.values.LinkedValue] and its schema is determined by those upstream objects.

        Note:
            Currently only one-to-one mappings of ``Value``/``LinkedValue`` is allowed. This will be more flexible in the future.

        Arguments:
             value_fields: field(s) within a [PipelineStructure][kiara.pipeline.structure.PipelineStructure] that is associated with this value
             value_id: the (unique) id for this value, if not provided one will be generated
             callbacks: the callbacks to register for this value (can be added later too)
             origin: a string describing the type of field the value is coming from (e.g. user input, step output, ...)

        Returns:
            the newly created value object
        """

        if value_id is not None and value_id in self._values.keys():
            raise Exception(f"Value id '{id}' already registered.")

        if value_id is not None and value_id in self._linked_value_items.keys():
            raise Exception(f"Value id '{id}' already registered as a linked value.")

        if value_id is None:
            value_id = generate_random_value_id()

        if value_fields is None:
            _value_fields: typing.Tuple[ValueField, ...] = tuple()
        elif isinstance(value_fields, ValueField):
            _value_fields = (value_fields,)
        elif isinstance(value_fields, typing.Iterable):
            for vf in value_fields:
                assert isinstance(vf, ValueField)
            _value_fields = tuple(value_fields)  # type: ignore
        else:
            raise TypeError(
                f"Invalid type for 'value_fields' argument: {type(value_fields)}"
            )

        _linked_values: typing.Dict[str, typing.Dict[str, str]] = {}
        _linked_value_objs: typing.List[DataValue] = []
        # TODO: allow duplicate ids as long as subvalues are different
        if isinstance(linked_values, str):
            if linked_values in _linked_values.keys():
                raise Exception(f"Duplicate linked value id: {linked_values}")
            _linked_values[linked_values] = {}
        elif isinstance(linked_values, Value):
            if linked_values.id in _linked_values.keys():
                raise Exception(f"Duplicate linked value id: {linked_values.id}")
            _linked_values[linked_values.id] = {}
        elif isinstance(linked_values, typing.Mapping):
            for k, v in linked_values.items():
                if k in _linked_values.keys():
                    raise Exception(f"Duplicate linked value id: {k}")
                if not v:
                    _linked_values[k] = {}
                else:
                    raise NotImplementedError()
        elif isinstance(linked_values, typing.Iterable):
            for linked_value in linked_values:
                _v = self.get_value_item(linked_value)  # type: ignore
                if _v.id in _linked_values.keys():
                    raise Exception(f"Duplicate linked value id: {_v.id}")
                _linked_values[_v.id] = {}
        else:
            raise TypeError(
                f"Invalid type '{type(linked_values)}' for linked values: {linked_values}"
            )

        if not _linked_values:
            raise Exception("Can't create linked value without any links.")
        for linked_value, details in _linked_values.items():
            if details:
                raise NotImplementedError()
            # make sure the value exists
            _i = self.get_value_item(linked_value)
            if not isinstance(_i, DataValue):
                raise NotImplementedError()
            _linked_value_objs.append(_i)

        # TODO: auto-generate doc string
        schema = ValueSchema(type=ValueType.any, doc="-- linked value --")

        value_item = LinkedValue(  # type: ignore
            id=value_id,
            value_schema=schema,
            value_fields=_value_fields,
            registry=self,  # type: ignore
            origin=origin,
            links=_linked_values,
        )

        self._update_linked_value(
            item=value_item, changed_upstream_values=_linked_value_objs
        )
        self._linked_value_items[value_id] = value_item
        for linked_value_id in _linked_values.keys():
            self._linked_value_items_reverse.setdefault(linked_value_id, []).append(
                value_item.id
            )

        if callbacks:
            for cb in callbacks:
                self.register_callback(cb, value_item)

        return self._linked_value_items[value_id]

    def register_callback(
        self, callback: ValueUpdateHandler, *items: typing.Union[str, Value]
    ):
        """Register a callback function that is called when one or several of the provided data items were changed.

        This callback needs to have a signature that takes in one or several objects of the class [Value][kiara.data.values.Value]
        as positional parameters (``*args``). If the callback has keyword arguments ``(**kwargs)``, those will be ignored.

        Arguments:
            callback: the callback
            *items: the value items (or their ids) to get notified for

        """

        for item in items:
            item = self.get_value_item(item)
            self._callbacks.setdefault(item.id, []).append(callback)

    def get_value_data(self, item: typing.Union[str, Value]) -> typing.Any:
        """Request the actual data for a value item or its id.

        Arguments:
            item: the value or its id

        Returns:
            The data wrapped in a Python object.
        """

        item = self.get_value_item(item)
        value: typing.Any = None
        if item.id in self._value_items.keys():
            return self._values[item.id]
        elif item.id in self._linked_value_items.keys():
            linked_item = self._linked_value_items[item.id]
            if len(linked_item.links) != 1:
                raise NotImplementedError()
            for linked_id, details in linked_item.links.items():
                if details:
                    raise NotImplementedError()
                value = self.get_value_data(linked_id)

        return value

    def set_value(self, item: typing.Union[str, Value], value: typing.Any) -> bool:
        """Set a single value.

        In most cases, the [set_values][kiara.data.registry.DataRegistry.set_values] method will be used, which is
        always recommended if multiple values are updated, since otherwise callbacks will be sent out seperately
        which might be inefficient.


        Arguments:
            item: the value object or id to be set
            value: the data (a Python object)

        Returns:
            whether the value was changed (``True``) or not (``False``)
        """

        item = self.get_value_item(item)

        result = self.set_values({item: value})  # type: ignore
        return result[item]

    def set_values(
        self, values: typing.Mapping[typing.Union[str, DataValue], typing.Any]
    ) -> typing.Dict[Value, bool]:
        """Set data on values.

        Args:
            values: a dict where the key is the value to set (or it's id), and the value is the data to set

        Returns:
            a dict where the key is the value and the value a bool that indicates whether the
                                      value was changed or not for that value
        """

        # ensure we are only dealing with values that can be set
        for _item, value in values.items():

            item: DataValue = self.get_value_item(_item)  # type: ignore
            if not isinstance(item, DataValue):
                raise Exception(f"Can't set non-datavalue '{item.id}'.")

            if item.is_constant:
                if self._values.get(item.id) is not None:
                    raise Exception(f"Can't set value '{item.id}', it's a constant.")

            if value is None:
                raise ValueError("Value can't be None")

        result: typing.Dict[Value, bool] = {}
        callbacks: typing.Dict[typing.Callable, typing.List[Value]] = {}
        linked_values_to_update: typing.Dict[str, typing.List[DataValue]] = {}

        # set all values, record callbacks and downstream dependencies that also need to be changed
        for _item, value in values.items():

            item: DataValue = self.get_value_item(_item)  # type:ignore

            old_value = self.get_value_data(item)
            changed = True
            if old_value == value:
                changed = False
            else:
                # TODO: validate value
                self._values[item.id] = value
                self._value_items[item.id].is_valid = True
                for cb in self._callbacks.get(item.id, []):
                    callbacks.setdefault(cb, []).append(item)

                _downstream_values = self._linked_value_items_reverse.get(item.id, None)
                if _downstream_values:
                    for _up in _downstream_values:
                        linked_values_to_update.setdefault(_up, []).append(item)
            result[item] = changed

        # now we need to re-compute all the linked values that are dependent on one or several of the changed items
        for linked_value, upstream_values in linked_values_to_update.items():
            _i: LinkedValue = self.get_value_item(linked_value)  # type: ignore
            if not isinstance(_i, LinkedValue):
                raise NotImplementedError()
            self._update_linked_value(item=_i, changed_upstream_values=upstream_values)
            for cb in self._callbacks.get(linked_value, []):
                callbacks.setdefault(cb, []).append(_i)

        for cb, v in callbacks.items():
            cb(*v)

        return result

    def _update_linked_value(
        self, item: LinkedValue, changed_upstream_values: typing.List[DataValue]
    ):
        """Update metadata for a linked value after one or several of it's 'parents' changed.

        Arguments:
            item: the value to update
            changed_upstream_values: a list of parent values that were changed
        """

        assert isinstance(item, LinkedValue)

        valid = True
        for value_id, details in item.links.items():
            linked_item = self.get_value_item(value_id)
            if not linked_item.is_valid:
                valid = False
                break

        item.is_valid = valid

    def get_stats(self) -> typing.Dict:

        return self._values

    def __eq__(self, other):

        if not isinstance(other, DataRegistry):
            return False

        return self._id == other._id

    def __hash__(self):

        return hash(self._id)
