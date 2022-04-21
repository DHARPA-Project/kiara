# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from rich import box
from rich.console import RenderableType
from rich.table import Table
from typing import TYPE_CHECKING, Any, Iterable, List, Mapping, Optional, Type, Union

if TYPE_CHECKING:
    from kiara import KiaraModule
    from kiara.data_types import DataType
    from kiara.models.module.jobs import ActiveJob
    from kiara.models.module.manifest import Manifest
    from kiara.models.values.value import Value


class KiaraException(Exception):
    pass


class KiaraModuleConfigException(Exception):
    def __init__(
        self,
        msg: str,
        module_cls: Type["KiaraModule"],
        config: Mapping[str, Any],
        parent: Optional[Exception] = None,
    ):

        self._module_cls = module_cls
        self._config = config

        self._parent: Optional[Exception] = parent

        if not msg.endswith("."):
            _msg = msg + "."
        else:
            _msg = msg

        super().__init__(_msg)


class ValueTypeConfigException(Exception):
    def __init__(
        self,
        msg: str,
        type_cls: Type["DataType"],
        config: Mapping[str, Any],
        parent: Optional[Exception] = None,
    ):

        self._type_cls = type_cls
        self._config = config

        self._parent: Optional[Exception] = parent

        if not msg.endswith("."):
            _msg = msg + "."
        else:
            _msg = msg

        super().__init__(_msg)


class KiaraValueException(Exception):
    def __init__(
        self,
        data_type: Type["DataType"],
        value_data: Any,
        exception: Exception,
    ):
        self._data_type: Type["DataType"] = data_type
        self._value_data: Any = value_data
        self._exception: Exception = exception

        exc_msg = str(self._exception)
        if not exc_msg:
            exc_msg = "no details available"

        super().__init__(f"Invalid value of type '{data_type._data_type_name}': {exc_msg}")  # type: ignore


class NoSuchExecutionTargetException(Exception):
    def __init__(
        self,
        selected_target: str,
        available_targets: Iterable[str],
        msg: Optional[str] = None,
    ):

        if msg is None:
            msg = f"Specified run target '{selected_target}' is an operation, additional module configuration is not allowed."

        self.avaliable_targets: Iterable[str] = available_targets
        super().__init__(msg)


class KiaraProcessingException(Exception):
    def __init__(
        self,
        msg: Union[str, Exception],
        module: Optional["KiaraModule"] = None,
        inputs: Optional[Mapping[str, "Value"]] = None,
    ):
        self._module: Optional["KiaraModule"] = module
        self._inputs: Optional[Mapping[str, Value]] = inputs
        if isinstance(msg, Exception):
            self._parent: Optional[Exception] = msg
            _msg = str(msg)
        else:
            self._parent = None
            _msg = msg
        super().__init__(_msg)

    @property
    def module(self) -> "KiaraModule":
        return self._module  # type: ignore

    @property
    def inputs(self) -> Mapping[str, "Value"]:
        return self._inputs  # type: ignore

    @property
    def parent_exception(self) -> Optional[Exception]:
        return self._parent


class InvalidValuesException(Exception):
    def __init__(
        self,
        msg: Union[None, str, Exception] = None,
        invalid_values: Mapping[str, str] = None,
    ):

        if invalid_values is None:
            invalid_values = {}

        self.invalid_inputs: Mapping[str, str] = invalid_values

        if msg is None:
            if not self.invalid_inputs:
                _msg = "Invalid values. No details available."
            else:
                msg_parts = []
                for k, v in invalid_values.items():
                    msg_parts.append(f"{k}: {v}")
                _msg = f"Invalid values: {', '.join(msg_parts)}"
        elif isinstance(msg, Exception):
            self._parent: Optional[Exception] = msg
            _msg = str(msg)
        else:
            self._parent = None
            _msg = msg

        super().__init__(_msg)

    def create_renderable(self, **config: Any) -> Table:

        table = Table(box=box.SIMPLE, show_header=True)

        table.add_column("field name", style="i")
        table.add_column("[red]error[/red]")

        for field_name, error in self.invalid_inputs.items():

            row: List[RenderableType] = [field_name]
            row.append(error)
            table.add_row(*row)

        return table


class JobConfigException(Exception):
    def __init__(
        self,
        msg: Union[str, Exception],
        manifest: "Manifest",
        inputs: Mapping[str, Any],
    ):

        self._manifest: Manifest = manifest
        self._inputs: Mapping[str, Any] = inputs

        if isinstance(msg, Exception):
            self._parent: Optional[Exception] = msg
            _msg = str(msg)
        else:
            self._parent = None
            _msg = msg

        super().__init__(_msg)

    @property
    def manifest(self) -> "Manifest":
        return self._manifest

    @property
    def inputs(self) -> Mapping[str, Any]:
        return self._inputs


class FailedJobException(Exception):
    def __init__(self, job: "ActiveJob", msg: Optional[str] = None):

        self.job: ActiveJob = job
        if msg is None:
            msg = "Job failed."
        super().__init__(msg)
