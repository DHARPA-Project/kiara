# -*- coding: utf-8 -*-
import inspect
import textwrap
import typing
from abc import abstractmethod
from pydantic import BaseModel, Extra, Field, PrivateAttr, root_validator
from rich import box
from rich.console import Console, ConsoleOptions, RenderResult
from rich.syntax import Syntax
from rich.table import Table

from kiara.config import KIARA_CONFIG, KiaraModuleConfig
from kiara.data.values import ValueSchema, ValueSet
from kiara.utils import (
    StringYAML,
    create_table_from_config_class,
    get_doc_for_module_class,
)

if typing.TYPE_CHECKING:
    from kiara import Kiara


yaml = StringYAML()


class StepInputs(object):
    """Wrapper class to hold a set of inputs for a pipeline processing step.

    This is necessary because we can't assume the processing will be done on the same machine (or in the same process)
    as the pipeline controller. By disconnecting the value from the processing code, we can react appropriately to
    those circumstances.

    Arguments:
        inputs (ValueSet): the input values of a pipeline step
    """

    def __init__(self, inputs: ValueSet):
        self._inputs: ValueSet = inputs

    def __getattr__(self, key):

        if key == "_inputs":
            raise KeyError()
        elif key in self.__dict__["_inputs"].keys():
            return self.__dict__["_inputs"][key].get_value_data()
        else:
            return super().__getattribute__(key)


class StepOutputs(object):
    """Wrapper class to hold a set of outputs for a pipeline processing step.

    This is necessary because we can't assume the processing will be done on the same machine (or in the same process)
    as the pipeline controller. By disconnecting the value from the processing code, we can react appropriately to
    those circumstances.

    Arguments:
        outputs (ValueSet): the output values of a pipeline step
    """

    def __init__(self, outputs: ValueSet):
        super().__setattr__("_outputs_staging", {})
        super().__setattr__("_outputs", outputs)

    def __getattr__(self, key):

        if key == "_outputs":
            raise KeyError()
        elif key in self.__dict__["_outputs"].keys():
            return self.__dict__["_outputs"][key].get_value_data()
        else:
            return super().__getattribute__(key)

    def __setattr__(self, key, value):

        self.set_values(**{key: value})

    def set_values(self, **values: typing.Any):

        wrong = []
        for key in values.keys():
            if key not in self._outputs.keys():
                wrong.append(key)

        if wrong:
            av = ", ".join(self._outputs.keys())
            raise Exception(
                f"Can't set output value(s), invalid key name(s): {', '.join(wrong)}. Available: {av}"
            )

        self._outputs_staging.update(values)

    def _sync(self):

        self._outputs.update(self._outputs_staging)


class KiaraModule(typing.Generic[KIARA_CONFIG]):
    """The base class that every custom module in *Kiara* needs to inherit from.

    The core of every ``KiaraModule`` is the [``process``][kiara.module.KiaraModule.process] method, which needs to be
    a pure, (ideally, but not strictly) idempotent function that creates one or several output values from the given
    input(s).

    Examples:

        A simple example would be an 'addition' module, with ``a`` and ``b`` configured as inputs, and ``z`` as the output field name.

        An implementing class would look something like this:

        TODO

    Arguments:
        id (str): the id for this module (needs to be unique within a pipeline)
        parent_id (typing.Optional[str]): the id of the parent, in case this module is part of a pipeline
        module_config (typing.Any): the configuation for this module
        meta (typing.Mapping[str, typing.Any]): metadata for this module (not implemented yet)
    """

    # TODO: not quite sure about this generic type here, mypy doesn't seem to like it
    _config_cls: typing.Type[KIARA_CONFIG] = KiaraModuleConfig  # type: ignore

    @classmethod
    def is_pipeline(cls) -> bool:
        return False

    def __init__(
        self,
        id: str,
        parent_id: typing.Optional[str] = None,
        module_config: typing.Union[
            None, KIARA_CONFIG, typing.Mapping[str, typing.Any]
        ] = None,
        meta: typing.Mapping[str, typing.Any] = None,
        kiara: typing.Optional["Kiara"] = None,
    ):

        self._id: str = id
        self._parent_id = parent_id

        if kiara is None:
            kiara = Kiara.instance()
        self._kiara = kiara

        if isinstance(module_config, KiaraModuleConfig):
            self._config: KIARA_CONFIG = module_config  # type: ignore
        elif module_config is None:
            self._config = self.__class__._config_cls()
        elif isinstance(module_config, typing.Mapping):
            self._config = self.__class__._config_cls(**module_config)
        else:
            raise TypeError(f"Invalid type for module config: {type(module_config)}")

        if meta is None:
            meta = {}
        self._meta = meta

        self._input_schemas: typing.Mapping[str, ValueSchema] = None  # type: ignore
        self._output_schemas: typing.Mapping[str, ValueSchema] = None  # type: ignore

    @property
    def id(self) -> str:
        """The id of this module.

        This is only unique within a pipeline.
        """
        return self._id

    @property
    def parent_id(self) -> typing.Optional[str]:
        """The id of the parent of this module (if part of a pipeline)."""
        return self._parent_id

    @property
    def full_id(self) -> str:
        """The full id for this module."""

        if self.parent_id:
            return f"{self.parent_id}.{self.id}"
        else:
            return self.id

    @property
    def config(self) -> KIARA_CONFIG:
        """Retrieve the configuration object for this module.

        Returns:
            the module-class-specific config object
        """
        return self._config

    def get_config_value(self, key: str) -> typing.Any:
        """Retrieve the value for a specific configuration option.

        Arguments:
            key: the config key

        Returns:
            the value for the provided key
        """

        return self.config.get(key)

    @abstractmethod
    def create_input_schema(self) -> typing.Mapping[str, ValueSchema]:
        """Abstract method to implement by child classes, returns a description of the input schema of this module."""

    @abstractmethod
    def create_output_schema(self) -> typing.Mapping[str, ValueSchema]:
        """Abstract method to implement by child classes, returns a description of the output schema of this module."""

    @property
    def input_schemas(self) -> typing.Mapping[str, ValueSchema]:
        """The input schema for this module."""

        if self._input_schemas is None:
            self._input_schemas = self.create_input_schema()
        if not self._input_schemas:
            raise Exception(
                f"Invalid module implementation for '{self.__class__.__name__}': empty input schema"
            )
        return self._input_schemas

    @property
    def output_schemas(self) -> typing.Mapping[str, ValueSchema]:
        """The output schema for this module."""

        if self._output_schemas is None:
            self._output_schemas = self.create_output_schema()
            if not self._output_schemas:
                raise Exception(
                    f"Invalid module implementation for '{self.__class__.__name__}': empty output schema"
                )
        return self._output_schemas

    @property
    def input_names(self) -> typing.Iterable[str]:
        """A list of input field names for this module."""
        return self.input_schemas.keys()

    @property
    def output_names(self) -> typing.Iterable[str]:
        """A list of output field names for this module."""
        return self.output_schemas.keys()

    def process_step(self, inputs: ValueSet, outputs: ValueSet) -> None:
        """Kick off processing for a specific set of input/outputs.

        This method calls the implemented [process][kiara.module.KiaraModule.process] method of the inheriting class,
        as well as wrapping input/output-data related functionality.

        Arguments:
            inputs: the input value set
            outputs: the output value set
        """

        input_wrap: StepInputs = StepInputs(inputs=inputs)
        output_wrap: StepOutputs = StepOutputs(outputs=outputs)

        self.process(inputs=input_wrap, outputs=output_wrap)

        output_wrap._sync()

    @abstractmethod
    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:
        """Abstract method to implement by child classes, should be a pure, idempotent function that uses the values from ``inputs``, and stores results in the provided ``outputs`` object.

        Arguments:
            inputs: the input value set
            outputs: the output value set
        """

    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False
        return (self.full_id, self.config) == (self.full_id, other.config)

    def __hash__(self):
        return hash((self.__class__, self.full_id, self.config))

    def __repr__(self):
        return f"{self.__class__.__name__}(input_names={list(self.input_names)} output_names={list(self.output_names)})"

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        if not hasattr(self.__class__, "_module_type_id"):
            raise Exception(
                "Invalid model class, no '_module_type_id' attribute added. This is a bug"
            )

        data = {
            # "module id": self.full_id,
            "module type": self.__class__._module_type_id,  # type: ignore
            "module_config": self.config.dict(),
            "inputs": {},
            "outputs": {},
        }

        for field_name, schema in self.input_schemas.items():
            d = "-- no default --" if schema.default is None else str(schema.default)
            data["inputs"][field_name] = {
                "type": schema.type,
                "doc": schema.doc,
                "default": d,
            }
        for field_name, schema in self.output_schemas.items():
            data["outputs"][field_name] = {"type": schema.type, "doc": schema.doc}

        yaml_str = yaml.dump(data)
        yield Syntax(yaml_str, "yaml", background_color="default")


class ModuleInfo(BaseModel):
    """A simple model class to hold and display information about a module.

    This is not used in processing at all, it is really only there to make it easier to communicate module characteristics..
    """

    class Config:
        extra = Extra.forbid
        allow_mutation = False

    module_type: str = Field(description="The name the module is registered under.")
    module_cls: typing.Type[KiaraModule] = Field(description="The module to describe.")
    doc: str = Field(description="The documentation of the module.")
    process_doc: str = Field(
        description="In-depth documentation of the processing step of this module.",
        default="-- n/a --",
    )
    process_src: str = Field(
        description="The source code of the processing method of this module."
    )
    config_cls: typing.Type[KiaraModuleConfig] = Field(
        description="The configuration class for this module."
    )
    _kiara: "Kiara" = PrivateAttr()

    def __init__(self, **data):  # type: ignore
        kiara = data.get("_kiara", None)
        if kiara is None:
            kiara = Kiara.instance()
            data["_kiara"] = kiara
        self._kiara: Kiara = kiara
        super().__init__(**data)

    @root_validator(pre=True)
    def ensure_type(cls, values):

        kiara = values.pop("_kiara")

        module_type = values.pop("module_type", None)
        assert module_type is not None

        if values:
            raise ValueError(
                f"Only 'module_type' allowed in constructor, not: {values.keys()}"
            )

        module_cls = kiara.get_module_class(module_type)
        values["module_type"] = module_type
        values["module_cls"] = module_cls

        doc = get_doc_for_module_class(module_cls)

        values["doc"] = doc
        proc_doc = module_cls.process.__doc__
        if not proc_doc:
            proc_doc = "-- n/a --"
        else:
            proc_doc = inspect.cleandoc(proc_doc)
        values["process_doc"] = proc_doc

        proc_src = inspect.getsource(module_cls.process)
        values["process_src"] = textwrap.dedent(proc_src)
        values["config_cls"] = module_cls._config_cls

        return values

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:
        yield f"[i]Module[/i]: [b]{self.module_type}[/b]"
        my_table = Table(box=box.SIMPLE, show_lines=True, show_header=False)
        my_table.add_column("Property", style="i")
        my_table.add_column("Value")
        my_table.add_row(
            "class", f"{self.module_cls.__module__}.{self.module_cls.__qualname__}"
        )
        my_table.add_row("doc", self.doc)
        my_table.add_row(
            "config class",
            f"{self.config_cls.__module__}.{self.config_cls.__qualname__}",
        )
        my_table.add_row("config", create_table_from_config_class(self.config_cls))
        syn_src = Syntax(self.process_src, "python")
        my_table.add_row("src", syn_src)

        yield my_table
