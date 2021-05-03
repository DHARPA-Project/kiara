# -*- coding: utf-8 -*-
import typing

if typing.TYPE_CHECKING:
    from kiara import KiaraModule
    from kiara.module import StepInputs


class KiaraException(Exception):
    pass


class KiaraProcessingException(Exception):
    def __init__(
        self,
        msg: typing.Union[str, Exception],
        module: typing.Optional["KiaraModule"] = None,
        inputs: typing.Optional["StepInputs"] = None,
    ):
        self._module: typing.Optional["KiaraModule"] = module
        self._inputs: typing.Optional["StepInputs"] = inputs
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
    def inputs(self) -> "KiaraModule":
        return self._inputs  # type: ignore

    @property
    def parent_exception(self) -> typing.Optional[Exception]:
        return self._parent
