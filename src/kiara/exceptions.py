# -*- coding: utf-8 -*-
import typing

if typing.TYPE_CHECKING:
    from kiara import KiaraModule
    from kiara.data.values import ValueSet


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
