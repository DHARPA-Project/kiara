# -*- coding: utf-8 -*-
import abc
import logging
import typing
import uuid

from kiara.data.values import Value, ValueMetadata, ValueSchema
from kiara.defaults import SpecialValue
from kiara.pipeline.values import LinkedValue, ValueField, ValueUpdateHandler

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara

log = logging.getLogger("kiara")


def generate_random_value_id():

    return str(uuid.uuid4())


class DataRegistry(abc.ABC):
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

    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._id: str = generate_random_value_id()

    @abc.abstractmethod
    def get_value_item(self, item: typing.Union[str, Value]) -> Value:
        """Get the [Value][kiara.data.values.Value] object for an id.

        If a string is provided, it is interpreted as value id. If a ``Value`` object is provided, the registry will check whether its id is registered with it, and return the object that is registered with it.

        If the provided id could not be found, an Exception is thrown.

        Arguments:
            item: a value id or ``Value`` object

        Returns: the ``Value`` objectssh 1
        """

    @abc.abstractmethod
    def register_value(
        self,
        value_schema: ValueSchema,
        value_fields: typing.Union[
            ValueField, typing.Iterable[ValueField], None
        ] = None,
        callbacks: typing.Optional[typing.Iterable[ValueUpdateHandler]] = None,
        initial_value: typing.Any = SpecialValue.NOT_SET,
        is_constant: bool = False,
        value_metadata: typing.Union[
            None, typing.Mapping[str, typing.Any], ValueMetadata
        ] = None,
    ) -> Value:
        """Register a value in this registry.

        This registers an unique id, along with a data schema and other metadata which can then be 'filled' with actual
        data.

        Arguments:
             value_schema: the allowed schema for the data that is held in this value
             value_fields: the field(s) within a [PipelineStructure][kiara.pipeline.structure.PipelineStructure] that is associated with this value
             value_id: the (unique) id for this value, if not provided one will be generated
             callbacks: the callbacks to register for this value (can be added later too)
             initial_value: if provided, this value will be set
             is_constant: whether this value is a constant or not
             value_metadata: value metadata (not related to the actual data)

        Returns:
            the newly created value object
        """

    @abc.abstractmethod
    def register_linked_value(
        self,
        linked_values: typing.Union[
            typing.Dict[typing.Union[str, Value], typing.Dict[str, typing.Any]],
            str,
            Value,
            typing.Iterable[typing.Union[str, Value]],
        ],
        value_schema: ValueSchema,
        value_fields: typing.Union[
            ValueField, typing.Iterable[ValueField], None
        ] = None,
        value_id: typing.Optional[str] = None,
        callbacks: typing.Optional[typing.Iterable[ValueUpdateHandler]] = None,
        value_metadata: typing.Union[
            None, typing.Mapping[str, typing.Any], ValueMetadata
        ] = None,
    ) -> LinkedValue:
        """Register a linked value in this registry.

        This registers an unique id, along with one or several other, already existing 'parent' ``Value`` objects. The
        'value' of the resulting [LinkedValue][kiara.data.values.LinkedValue] and its schema is determined by those upstream objects.

        Note:
            Currently only one-to-one mappings of ``Value``/``LinkedValue`` is allowed. This will be more flexible in the future.

        Arguments:
             value_schema: the schema of the linked value
             value_fields: field(s) within a [PipelineStructure][kiara.pipeline.structure.PipelineStructure] that is associated with this value
             value_id: the (unique) id for this value, if not provided one will be generated
             callbacks: the callbacks to register for this value (can be added later too)
             value_metadata: value metadata (not related to the actual data)

        Returns:
            the newly created value object
        """

    @abc.abstractmethod
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

    @abc.abstractmethod
    def get_value_data(self, item: typing.Union[str, Value]) -> typing.Any:
        """Request the actual data for a value item or its id.

        Arguments:
            item: the value or its id

        Returns:
            The data wrapped in a Python object.
        """

    # def calculate_sub_value(self, linked_id: str, subvalue: typing.Dict[str, str]):
    #
    #     linked_obj = self.get_value_item(linked_id)
    #
    #     if linked_obj.value_schema.type != "table":
    #         raise NotImplementedError()
    #
    #     column_name = subvalue["config"]
    #     table_metadata = self._kiara.metadata_mgmt.get_value_metadata(
    #         linked_obj, "table"
    #     )
    #     column_names = table_metadata["table"]["column_names"]
    #     if column_name not in column_names:
    #         raise Exception(
    #             f"Can't retrieve subvalue column '{column_name}'. Table does not contain a column with that name. Available column names: {', '.join(column_names)}"
    #         )
    #
    #     value: Table = self.get_value_data(linked_id)
    #     return value.column(column_name)
    #
    # def calculate_multiple_linked_value(self, value_id: str):
    #
    #     linked_item = self._linked_value_items[value_id]
    #     assert isinstance(linked_item, LinkedValue)
    #     assert linked_item.value_schema.type == "dict"
    #
    #     result = {}
    #     for l_id, v in linked_item.links.items():
    #
    #         value = self.get_value_data(l_id)
    #         key = v["config"]
    #         result[key] = value
    #
    #     return result

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

        if value == SpecialValue.NOT_SET:
            raise Exception("'not_set' is not a valid value to set.")

        item = self.get_value_item(item)
        result = self.set_values({item: value})  # type: ignore
        return result[item]

    @abc.abstractmethod
    def set_values(
        self,
        values: typing.Mapping[typing.Union[str, Value], typing.Any],
    ) -> typing.Dict[Value, bool]:
        """Set data on values.

        Args:
            values: a dict where the key is the value to set (or its id), and the value is the data to set

        Returns:
            a dict where the key is the value and the value a bool that indicates whether the
                                      value was changed or not for that value
        """

    def __eq__(self, other):

        if not isinstance(other, DataRegistry):
            return False

        return self._id == other._id

    def __hash__(self):

        return hash(self._id)
