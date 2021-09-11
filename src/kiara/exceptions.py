# -*- coding: utf-8 -*-
import typing

if typing.TYPE_CHECKING:
    from kiara import KiaraModule
    from kiara.data.types import ValueType
    from kiara.data.values.value_set import ValueSet


class KiaraException(Exception):
    pass


class KiaraModuleConfigException(Exception):
    def __init__(
        self,
        msg: str,
        module_cls: typing.Type["KiaraModule"],
        config: typing.Mapping[str, typing.Any],
        parent: typing.Optional[Exception] = None,
    ):

        self._module_cls = module_cls
        self._config = config

        self._parent: typing.Optional[Exception] = parent

        if not msg.endswith("."):
            _msg = msg + "."
        else:
            _msg = msg

        super().__init__(_msg)


class ValueTypeConfigException(Exception):
    def __init__(
        self,
        msg: str,
        type_cls: typing.Type["ValueType"],
        config: typing.Mapping[str, typing.Any],
        parent: typing.Optional[Exception] = None,
    ):

        self._type_cls = type_cls
        self._config = config

        self._parent: typing.Optional[Exception] = parent

        if not msg.endswith("."):
            _msg = msg + "."
        else:
            _msg = msg

        super().__init__(_msg)


class KiaraValueException(Exception):
    def __init__(
        self,
        value_type: typing.Type["ValueType"],
        value_data: typing.Any,
        exception: Exception,
    ):
        self._value_type: typing.Type["ValueType"] = value_type
        self._value_data: typing.Any = value_data
        self._exception: Exception = exception

        exc_msg = str(self._exception)
        if not exc_msg:
            exc_msg = "no details available"

        super().__init__(f"Invalid value of type '{value_type._value_type_name}': {exc_msg}")  # type: ignore


class KiaraProcessingException(Exception):
    def __init__(
        self,
        msg: typing.Union[str, Exception],
        module: typing.Optional["KiaraModule"] = None,
        inputs: typing.Optional["ValueSet"] = None,
    ):
        self._module: typing.Optional["KiaraModule"] = module
        self._inputs: typing.Optional["ValueSet"] = inputs
        if isinstance(msg, Exception):
            self._parent: typing.Optional[Exception] = msg
            _msg = str(msg)
        else:
            self._parent = None
            _msg = msg
        super().__init__(_msg)

    @property
    def module(self) -> "KiaraModule":
        return self._module  # type: ignore

    @property
    def inputs(self) -> "ValueSet":
        return self._inputs  # type: ignore

    @property
    def parent_exception(self) -> typing.Optional[Exception]:
        return self._parent
