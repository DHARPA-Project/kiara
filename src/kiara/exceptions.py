# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import uuid
from typing import TYPE_CHECKING, Any, Iterable, List, Mapping, Type, Union

import orjson

from kiara.defaults import NOT_AVAILBLE_MARKER
from kiara.utils.json import orjson_dumps

if TYPE_CHECKING:
    from rich.console import RenderableType
    from rich.table import Table

    from kiara.data_types import DataType
    from kiara.models.module.jobs import ActiveJob
    from kiara.models.module.manifest import Manifest
    from kiara.models.module.pipeline import PipelineConfig
    from kiara.models.values.value import Value
    from kiara.modules import KiaraModule


class KiaraException(Exception):
    @classmethod
    def get_root_details(
        cls, e: Exception, default: Union[None, str] = None
    ) -> Union[str, None]:

        if isinstance(e, KiaraException):
            return e.root_details()
        else:
            if default is None:
                return str(e)
            else:
                return default

    def __init__(self, msg: str, parent: Union[Exception, None] = None, **kwargs):

        self._msg = msg
        self._parent: Union[Exception, None] = parent
        self._properties = kwargs
        super().__init__(msg)

    @property
    def msg(self):
        return self._msg

    @property
    def details(self) -> Union[str, None]:

        result: Union[None, str] = self._properties.get("details", None)
        return result

    @property
    def parent(self) -> Union[Exception, None]:
        return self._parent

    @property
    def root_cause(self) -> Exception:

        current: Exception = self
        while hasattr(current, "parent") and current.parent is not None:  # type: ignore
            current = current.parent  # type: ignore

        return current

    def root_details(self) -> Union[str, None]:

        current: Exception = self
        if hasattr(self, "details"):
            current_details = self.details  # type: ignore
        else:
            current_details = None
        while hasattr(current, "parent") and current.parent is not None:  # type: ignore
            current = current.parent  # type: ignore

            if hasattr(current, "details") and current.details:  # type: ignore
                current_details = current.details  # type: ignore
            else:
                current_details = str(current)

        return current_details

    def create_renderable(self, **config) -> "RenderableType":

        from rich.console import Group

        rows: List[RenderableType] = [f"[red]Error[/red]: {self._msg}"]
        root_details = self.root_details()
        if root_details:
            from rich.markdown import Markdown

            rows.append("")
            rows.append(Markdown(root_details))

        return Group(*rows)

    def __str__(self) -> str:
        msg = super().__str__()
        root_details = self.root_details()
        if root_details:
            msg += f"\n\n{root_details}"
        return msg


class InvalidCommandLineInvocation(KiaraException):
    def __init__(
        self,
        msg: str,
        parent: Union[Exception, None] = None,
        error_code: int = 0,
        **kwargs,
    ):

        self.error_code: int = error_code

        super().__init__(msg, parent=parent, **kwargs)


class KiaraContextException(KiaraException):
    def __init__(self, msg: str, context_id: uuid.UUID):

        self._context_id: uuid.UUID = context_id
        super().__init__(msg)


class KiaraModuleConfigException(KiaraException):
    def __init__(
        self,
        msg: str,
        module_cls: Type["KiaraModule"],
        config: Mapping[str, Any],
        parent: Union[Exception, None] = None,
    ):

        self._module_cls = module_cls
        self._config = config

        if not msg.endswith("."):
            _msg = msg + "."
        else:
            _msg = msg

        super().__init__(_msg, parent=parent)


class InvalidManifestException(KiaraException):
    def __init__(
        self,
        msg: str,
        module_type: str,
        module_config: Union[None, Mapping[str, Any]] = None,
        available_module_types: Union[None, Iterable[str]] = None,
        parent: Union[Exception, None] = None,
    ):

        self._module_type = module_type
        self._module_config = module_config
        self._available_module_types = available_module_types
        super().__init__(msg, parent=parent)

    @property
    def details(self) -> Union[str, None]:

        if not self._available_module_types:
            return None

        else:
            msg = "Available module types:\n\n"
            for module_type in self._available_module_types:
                msg += f"- {module_type}\n"
            return msg


class ValueTypeConfigException(KiaraException):
    def __init__(
        self,
        msg: str,
        type_cls: Type["DataType"],
        config: Mapping[str, Any],
        parent: Union[Exception, None] = None,
    ):

        self._type_cls = type_cls
        self._config = config

        if not msg.endswith("."):
            _msg = msg + "."
        else:
            _msg = msg

        super().__init__(_msg, parent=parent)


class DataTypeUnknownException(KiaraException):
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


class KiaraValueException(KiaraException):
    def __init__(
        self,
        data_type: Type["DataType"],
        value_data: Any,
        parent: Exception,
    ):
        self._data_type: Type["DataType"] = data_type
        self._value_data: Any = value_data

        exc_msg = str(parent)
        if not exc_msg:
            exc_msg = "no details available"

        super().__init__(f"Invalid value of type '{data_type._data_type_name}': {exc_msg}", parent=parent)  # type: ignore


class NoSuchExecutionTargetException(KiaraException):
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


class KiaraProcessingException(KiaraException):
    def __init__(
        self,
        msg: Union[str, Exception],
        module: Union["KiaraModule", None] = None,
        inputs: Union[Mapping[str, "Value"], None] = None,
    ):
        self._module: Union["KiaraModule", None] = module
        self._inputs: Union[Mapping[str, Value], None] = inputs
        _properties = None

        if isinstance(msg, KiaraException):
            _parent: Union[Exception, None] = msg.parent
            _msg = msg.msg
            _properties = msg._properties
        elif isinstance(msg, Exception):
            _parent = msg
            _msg = str(msg)
        else:
            _parent = None
            _msg = msg
        if _properties:
            super().__init__(msg=_msg, parent=_parent, **_properties)
        else:
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


class InvalidValuesException(KiaraException):
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

    @property
    def details(self) -> str:
        result = ""
        for k, v in self.invalid_inputs.items():
            result += f" - {k}: {v}"

        return result

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


class JobConfigException(KiaraException):
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


class FailedJobException(KiaraException):
    def __init__(
        self,
        job: "ActiveJob",
        msg: Union[str, None] = None,
        parent: Union[Exception, None] = None,
    ):
        self.job: ActiveJob = job

        if isinstance(parent, KiaraException):
            super().__init__(msg=parent.msg, parent=parent.parent, **parent._properties)
        else:
            if msg is None:
                msg = "Job failed."
            super().__init__(msg=msg, parent=parent)

    # @property
    # def details(self) -> Union[str, None]:
    #     return None

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


class NoSuchValueException(KiaraException):

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


class NoSuchWorkflowException(KiaraException):
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


class NoSuchOperationException(KiaraException):
    def __init__(
        self,
        operation_id: str,
        available_operations: Iterable[str],
        msg: Union[None, str] = None,
    ):

        self._operation_id: str = operation_id
        self._available_operations: Iterable[str] = available_operations

        if not msg:
            msg = f"No operation with id '{operation_id}' available."

        super().__init__(msg)

    @property
    def available_operations(self) -> Iterable[str]:
        return self._available_operations

    @property
    def operation_id(self) -> str:
        return self._operation_id


class InvalidOperationException(KiaraException):
    def __init__(self, operation_details: Mapping[str, Any]):

        self._all_details: Mapping[str, Any] = operation_details
        msg = operation_details.get("operation_id", None)
        if msg is None:
            msg = operation_details.get("pipeline_name", None)
        parent = operation_details.get("parent", None)
        super().__init__(f"Invalid operation: {msg}", parent=parent)

    @property
    def module_id(self) -> str:
        return self._all_details.get("module_id", NOT_AVAILBLE_MARKER)

    @property
    def module_config(self) -> Union[Mapping[str, Any], str]:
        return self._all_details.get("module_config", {})

    @property
    def details(self) -> Union[str, None]:
        if self.parent:
            if hasattr(self.parent, "details"):
                return self.parent.details  # type: ignore
            else:
                return None
        else:
            return self._all_details.get("details", None)


class InvalidPipelineStepConfig(KiaraException):
    def __init__(self, msg: str, step_config: Mapping[str, Any]):

        self._step_config: Mapping[str, Any] = step_config
        super().__init__(msg)

    @property
    def details(self) -> str:
        config = orjson_dumps(self._step_config, option=orjson.OPT_INDENT_2)

        details = f"Invalid step config:\n\n```\n{config}\n```"
        return details


class InvalidPipelineConfig(KiaraException):
    def __init__(self, msg: str, config: "PipelineConfig", details: str):

        self._config = config
        self._details = details
        super().__init__(msg)

    @property
    def pipeline_config(self) -> "PipelineConfig":
        return self._config

    @property
    def details(self) -> Union[str, None]:

        return self._details
