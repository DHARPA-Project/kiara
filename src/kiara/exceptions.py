# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import uuid
from typing import TYPE_CHECKING, Any, Iterable, List, Mapping, Type, Union

if TYPE_CHECKING:
    from rich.table import Table

    from kiara.data_types import DataType
    from kiara.models.module.jobs import ActiveJob
    from kiara.models.module.manifest import Manifest
    from kiara.models.values.value import Value
    from kiara.modules import KiaraModule


class KiaraException(Exception):
    pass


class KiaraModuleConfigException(Exception):
    def __init__(
        self,
        msg: str,
        module_cls: Type["KiaraModule"],
        config: Mapping[str, Any],
        parent: Union[Exception, None] = None,
    ):

        self._module_cls = module_cls
        self._config = config

        self._parent: Union[Exception, None] = parent

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
        parent: Union[Exception, None] = None,
    ):

        self._type_cls = type_cls
        self._config = config

        self._parent: Union[Exception, None] = parent

        if not msg.endswith("."):
            _msg = msg + "."
        else:
            _msg = msg

        super().__init__(_msg)


class DataTypeUnknownException(Exception):
    def __init__(
        self,
        data_type: str,
        msg: Union[str, None] = None,
        value: Union[None, "Value"] = None,
    ):

        self._data_type = data_type
        if msg is None:
            msg = f"Data type '{data_type}' not registered in current context."
        self._msg = msg
        self._value = value

        super().__init__(msg)

    @property
    def data_type(self) -> str:
        return self._data_type

    @property
    def value(self) -> Union[None, "Value"]:
        return self._value

    def create_renderable(self, **config: Any) -> "Table":

        from rich import box
        from rich.table import Table

        table = Table(box=box.SIMPLE, show_header=False)

        table.add_column("key", style="i")
        table.add_column("value")

        table.add_row("error", self._msg)
        table.add_row("data type", self._data_type)
        table.add_row(
            "solution", "Install the Python package that provides this data type."
        )

        if self._value is not None:
            table.add_row("value", self._value.create_renderable())

        return table


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
        msg: Union[str, None] = None,
    ):

        if msg is None:
            msg = f"Specified run target '{selected_target}' is an operation, additional module configuration is not allowed."

        self.avaliable_targets: Iterable[str] = available_targets
        super().__init__(msg)


class KiaraProcessingException(Exception):
    def __init__(
        self,
        msg: Union[str, Exception],
        module: Union["KiaraModule", None] = None,
        inputs: Union[Mapping[str, "Value"], None] = None,
    ):
        self._module: Union["KiaraModule", None] = module
        self._inputs: Union[Mapping[str, Value], None] = inputs
        if isinstance(msg, Exception):
            self._parent: Union[Exception, None] = msg
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
    def parent_exception(self) -> Union[Exception, None]:
        return self._parent


class InvalidValuesException(Exception):
    def __init__(
        self,
        msg: Union[None, str, Exception] = None,
        invalid_values: Union[Mapping[str, str], None] = None,
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
            self._parent: Union[Exception, None] = msg
            _msg = str(msg)
        else:
            self._parent = None
            _msg = msg

        super().__init__(_msg)

    def create_renderable(self, **config: Any) -> "Table":

        from rich import box
        from rich.console import RenderableType
        from rich.table import Table

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
            self._parent: Union[Exception, None] = msg
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
    def __init__(self, job: "ActiveJob", msg: Union[str, None] = None):

        self.job: ActiveJob = job
        if msg is None:
            msg = "Job failed."
        self.msg = msg
        super().__init__(msg)

    def create_renderable(self, **config: Any):

        from rich import box
        from rich.console import Group
        from rich.panel import Panel
        from rich.table import Table

        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("key", style="i")
        table.add_column("value")

        table.add_row("job_id", str(self.job.job_id))
        table.add_row("module_type", self.job.job_config.module_type)

        group = Group(
            Panel(f"[red]Error[/red]: [i]{self.msg}[i]", box=box.SIMPLE), table
        )
        return group


class NoSuchValueException(Exception):

    pass


class NoSuchValueIdException(NoSuchValueException):
    def __init__(self, value_id: uuid.UUID, msg: Union[str, None] = None):
        self.value_id: uuid.UUID
        if not msg:
            msg = f"No value with id: {value_id}."
        super().__init__(msg)


class NoSuchValueAliasException(NoSuchValueException):
    def __init__(self, alias: str, msg: Union[str, None] = None):
        self.value_id: uuid.UUID
        if not msg:
            msg = f"No value with alias: {alias}."
        super().__init__(msg)


class NoSuchWorkflowException(Exception):
    def __init__(self, workflow: Union[uuid.UUID, str], msg: Union[str, None] = None):
        self._workflow: Union[str, uuid.UUID] = workflow
        if not msg:
            msg = f"No such workflow: {workflow}"
        super().__init__(msg)

    @property
    def alias_requested(self) -> bool:

        if isinstance(self._workflow, str):
            try:
                uuid.UUID(self._workflow)
                return False
            except Exception:
                return True
        else:
            return False


class NoSuchOperationException(Exception):
    def __init__(
        self,
        operation_id: str,
        available_operations: Iterable[str],
        msg: Union[None, str] = None,
    ):

        self._operation_id: str = operation_id
        self._available_operations: Iterable[str] = available_operations

        if not msg:
            msg = f"No operation with id: {operation_id} available."

        super().__init__(msg)

    @property
    def available_operations(self) -> Iterable[str]:
        return self._available_operations

    @property
    def operation_id(self) -> str:
        return self._operation_id
