# -*- coding: utf-8 -*-
import abc
import deepdiff
import inspect
import typing
import uuid
from abc import abstractmethod
from pydantic import BaseModel, Extra, Field, ValidationError
from rich import box
from rich.console import Console, ConsoleOptions, RenderGroup, RenderResult
from rich.panel import Panel
from rich.table import Table

from kiara.data.values import (
    NonRegistryValue,
    Value,
    ValueSchema,
    ValueSet,
    ValueSetImpl,
)
from kiara.exceptions import KiaraModuleConfigException
from kiara.metadata.module_models import (
    KiaraModuleInstanceMetadata,
    KiaraModuleTypeMetadata,
)
from kiara.module_config import KIARA_CONFIG, KiaraModuleConfig
from kiara.processing import JobLog
from kiara.utils import StringYAML, is_debug
from kiara.utils.modules import create_schemas, overlay_constants_and_defaults

if typing.TYPE_CHECKING:
    from kiara import Kiara


yaml = StringYAML()


class StepInputs(ValueSet):
    """Wrapper class to hold a set of inputs for a pipeline processing step.

    This is necessary because we can't assume the processing will be done on the same machine (or in the same process)
    as the pipeline controller. By disconnecting the value from the processing code, we can react appropriately to
    those circumstances.

    Arguments:
        inputs (ValueSet): the input values of a pipeline step
    """

    def __init__(self, inputs: ValueSet):
        self._inputs: ValueSet = inputs

    # def __getattr__(self, key):
    #
    #     if key == "_inputs":
    #         raise KeyError()
    #     elif key in self.__dict__["_inputs"].keys():
    #         return self.get_value_data(key)
    #     else:
    #         return super().__getattribute__(key)

    def get_all_field_names(self) -> typing.Iterable[str]:
        return self.__dict__["_inputs"].keys()

    def get_value_data_for_fields(self, *field_names) -> typing.Dict[str, typing.Any]:

        result = {}

        for input_name in field_names:
            value = self.__dict__["_inputs"][input_name].get_value_data()
            if hasattr(value, "as_py"):
                result[input_name] = value.as_py()
            else:
                result[input_name] = value
        return result

    def get_value_obj(
        self,
        field_name: str,
        ensure_metadata: typing.Union[bool, typing.Iterable[str], str] = False,
    ) -> Value:

        if ensure_metadata:
            raise NotImplementedError()

        return self.__dict__["_inputs"][field_name]

    def _set_values(self, **values: typing.Any) -> typing.Dict[Value, bool]:
        raise Exception("Inputs are read-only.")

    def is_read_only(self) -> bool:
        return True

    def __getitem__(self, item: str) -> Value:

        return self.get_value_obj(field_name=item)

    def __setitem__(self, key: str, value: Value):

        raise Exception("Inputs are read-only.")

    def __delitem__(self, key: str):

        raise Exception(f"Removing items not supported: {key}")

    def __iter__(self) -> typing.Iterator[str]:
        return iter(self._inputs.__iter__())

    def __len__(self):
        return len(self._inputs.__len__())


class StepOutputs(ValueSet):
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

    # def __getattr__(self, key):
    #
    #     print("xxxx")
    #     raise Exception()
    #
    #     if key == "_outputs":
    #         raise KeyError()
    #     elif key in self.__dict__["_outputs"].keys():
    #         return self.get_value_data(key)
    #     else:
    #         return super().__getattribute__(key)

    # def __setattr__(self, key, value):
    #     print("XXXXXXXXXXX")
    #     raise Exception()
    #
    #     self.set_values(**{key: value})

    def get_all_field_names(self) -> typing.Iterable[str]:
        return self.__dict__["_outputs"].keys()

    def _set_values(self, **values: typing.Any) -> typing.Dict[Value, bool]:

        wrong = []
        for key in values.keys():
            if key not in self._outputs.keys():  # type: ignore
                wrong.append(key)

        if wrong:
            av = ", ".join(self._outputs.keys())  # type: ignore
            raise Exception(
                f"Can't set output value(s), invalid key name(s): {', '.join(wrong)}. Available: {av}"
            )

        result = {}
        for output_name, value in values.items():
            value_obj = self.__dict__["_outputs"][output_name]
            if (
                output_name not in self._outputs_staging.keys()  # type: ignore
                or value != self._outputs_staging[output_name]  # type: ignore
            ):
                result[value_obj] = True
                self._outputs_staging[output_name] = value  # type: ignore
            else:
                result[value_obj] = False

        return result

    def get_value_data_for_fields(
        self, *field_names: str
    ) -> typing.Dict[str, typing.Any]:
        self.sync()
        result = {}
        for output_name in field_names:
            data = self.__dict__["_outputs"][field_names].get_value_data()
            result[output_name] = data
        return result

    def get_value_obj(self, output_name):
        self.sync()
        return self.__dict__["_outputs"][output_name]

    def is_read_only(self) -> bool:
        return False

    def sync(self):
        self._outputs.set_values(**self._outputs_staging)  # type: ignore
        self._outputs_staging.clear()  # type: ignore

    def __getitem__(self, item: str) -> Value:

        return self.get_value_obj(output_name=item)

    def __setitem__(self, key: str, value: Value):

        self.set_value(key, value)

    def __delitem__(self, key: str):

        raise Exception(f"Removing items not supported: {key}")

    def __iter__(self) -> typing.Iterator[str]:
        return iter(self.__dict__["_outputs"].__iter__())

    def __len__(self):
        return len(self.__dict__["_outputs"].__len__())


class KiaraModule(typing.Generic[KIARA_CONFIG], abc.ABC):
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
        metadata (typing.Mapping[str, typing.Any]): metadata for this module (not implemented yet)
    """

    # TODO: not quite sure about this generic type here, mypy doesn't seem to like it
    _config_cls: typing.Type[KIARA_CONFIG] = KiaraModuleConfig  # type: ignore

    @classmethod
    def get_type_metadata(cls) -> KiaraModuleTypeMetadata:

        return KiaraModuleTypeMetadata.from_module_class(cls)

    @classmethod
    def profiles(
        cls,
    ) -> typing.Optional[typing.Mapping[str, typing.Mapping[str, typing.Any]]]:
        return None

    @classmethod
    def is_pipeline(cls) -> bool:
        return False

    def __init__(
        self,
        id: typing.Optional[str] = None,
        parent_id: typing.Optional[str] = None,
        module_config: typing.Union[
            None, KIARA_CONFIG, typing.Mapping[str, typing.Any]
        ] = None,
        kiara: typing.Optional["Kiara"] = None,
    ):

        if id is None:
            id = str(uuid.uuid4())
        self._id: str = id
        self._parent_id = parent_id

        if kiara is None:
            from kiara import Kiara

            kiara = Kiara.instance()
        self._kiara = kiara

        if isinstance(module_config, KiaraModuleConfig):
            self._config: KIARA_CONFIG = module_config  # type: ignore
        elif module_config is None:
            self._config = self.__class__._config_cls()
        elif isinstance(module_config, typing.Mapping):
            try:
                self._config = self.__class__._config_cls(**module_config)
            except ValidationError as ve:
                raise KiaraModuleConfigException(
                    f"Error creating module '{id}'. {ve}",
                    self.__class__,
                    module_config,
                    ve,
                )
        else:
            raise TypeError(f"Invalid type for module config: {type(module_config)}")

        self._module_hash: typing.Optional[int] = None
        self._info: typing.Optional[KiaraModuleInstanceMetadata] = None

        self._input_schemas: typing.Mapping[str, ValueSchema] = None  # type: ignore
        self._output_schemas: typing.Mapping[str, ValueSchema] = None  # type: ignore

    @property
    def id(self) -> str:
        """The id of this module.

        This is only unique within a pipeline.
        """
        return self._id

    @property
    def type_name(self) -> str:
        return self._module_type_id  # type:ignore

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

    def input_required(self, input_name: str):

        if input_name not in self._input_schemas.keys():
            raise Exception(f"No input '{input_name}' for module '{self.id}'.")

        if not self._input_schemas[input_name].is_required():
            return False

        if input_name in self.get_config_value("constants"):
            return False
        else:
            return True

    def get_config_value(self, key: str) -> typing.Any:
        """Retrieve the value for a specific configuration option.

        Arguments:
            key: the config key

        Returns:
            the value for the provided key
        """

        return self.config.get(key)

    @abstractmethod
    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        """Abstract method to implement by child classes, returns a description of the input schema of this module."""

    @abstractmethod
    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        """Abstract method to implement by child classes, returns a description of the output schema of this module."""

    @property
    def input_schemas(self) -> typing.Mapping[str, ValueSchema]:
        """The input schema for this module."""

        if self._input_schemas is not None:
            return self._input_schemas

        _input_schemas = self.create_input_schema()

        if not _input_schemas:
            raise Exception(
                f"Invalid module implementation for '{self.__class__.__name__}': empty input schema"
            )

        try:
            self._input_schemas = create_schemas(
                schema_config=_input_schemas, kiara=self._kiara
            )
        except Exception as e:
            raise Exception(
                f"Can't create input schemas for module {self.full_id}: {e}"
            )

        defaults = self.config.defaults
        constants = self.config.constants
        overlay_constants_and_defaults(
            self._input_schemas, defaults=defaults, constants=constants
        )

        return self._input_schemas

    @property
    def output_schemas(self) -> typing.Mapping[str, ValueSchema]:
        """The output schema for this module."""

        if self._output_schemas is not None:
            return self._output_schemas

        _output_schema = self.create_output_schema()

        if not _output_schema:
            raise Exception(
                f"Invalid module implementation for '{self.__class__.__name__}': empty output schema"
            )

        try:
            self._output_schemas = create_schemas(
                schema_config=_output_schema, kiara=self._kiara
            )
        except Exception as e:
            raise Exception(
                f"Can't create output schemas for module {self.full_id}: {e}"
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

    def process_step(
        self, inputs: ValueSet, outputs: ValueSet, job_log: JobLog
    ) -> None:
        """Kick off processing for a specific set of input/outputs.

        This method calls the implemented [process][kiara.module.KiaraModule.process] method of the inheriting class,
        as well as wrapping input/output-data related functionality.

        Arguments:
            inputs: the input value set
            outputs: the output value set
        """

        signature = inspect.signature(self.process)  # type: ignore

        if "job_log" not in signature.parameters.keys():

            try:
                self.process(inputs=inputs, outputs=outputs)  # type: ignore
            except Exception as e:
                if is_debug():
                    try:
                        import traceback

                        traceback.print_exc()
                    except Exception:
                        pass
                raise e

        else:

            try:
                self.process(inputs=inputs, outputs=outputs, job_log=job_log)  # type: ignore
            except Exception as e:
                if is_debug():
                    try:
                        import traceback

                        traceback.print_exc()
                    except Exception:
                        pass
                raise e

    # @typing.overload
    # def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
    #     """Abstract method to implement by child classes, should be a pure, idempotent function that uses the values from ``inputs``, and stores results in the provided ``outputs`` object.
    #
    #     Arguments:
    #         inputs: the input value set
    #         outputs: the output value set
    #     """
    #     pass
    #
    # @typing.overload
    # def process(self, inputs: ValueSet, outputs: ValueSet, job_log: typing.Optional[JobLog]=None) -> None:
    #     pass
    #
    # def process(self, inputs, outputs, job_log=None) -> None:
    #     pass

    def run(self, **inputs: typing.Any) -> ValueSet:
        """Execute the module with the provided inputs directly.

        Arguments:
            inputs: a map of the input values (as described by the input schema
        Returns:
            a map of the output values (as described by the output schema)
        """

        # TODO: find a generic way to do this kind of stuff
        def clean_value(v: typing.Any) -> typing.Any:
            if hasattr(v, "as_py"):
                return v.as_py()  # type: ignore
            else:
                return v

        resolved_inputs = {}
        for k, v in inputs.items():
            v = clean_value(v)
            if not isinstance(v, Value):
                if k not in self.input_schemas.keys():
                    raise Exception(
                        f"Invalid input name '{k} for module {self._module_type_id}. Not part of the schema, allowed input names: {', '.join(self.input_names)}"  # type: ignore
                    )
                schema = self.input_schemas[k]
                v = NonRegistryValue(
                    _init_value=v,  # type: ignore
                    value_schema=schema,
                    is_constant=False,
                    kiara=self._kiara,
                )
            resolved_inputs[k] = v

        input_value_set = ValueSetImpl.from_schemas(
            kiara=self._kiara,
            schemas=self.input_schemas,
            read_only=True,
            initial_values=resolved_inputs,
        )
        output_value_set = ValueSetImpl.from_schemas(
            kiara=self._kiara, schemas=self.output_schemas, read_only=False
        )

        # m_inputs = StepInputs(inputs=input_value_set)
        # m_outputs = StepOutputs(outputs=output_value_set)

        self.process(inputs=input_value_set, outputs=output_value_set)  # type: ignore

        result = output_value_set.get_all_value_objects()
        return ValueSetImpl(items=result, read_only=True)

    @property
    def module_instance_doc(self) -> str:
        """Return documentation for this instance of the module.

        If not overwritten, will return this class' method ``doc()``.
        """

        # TODO: auto create instance doc?
        return self.get_type_metadata().documentation.full_doc

    @property
    def module_instance_hash(self) -> int:
        """Return this modules 'module_hash'.

        If two module instances ``module_instance_hash`` values are the same, it is guaranteed that their ``process`` methods will
        return the same output, given the same inputs (except if that processing step uses randomness). It can also be
        assumed that the two instances have the same input and output fields, with the same schemas.

        !!! note
        This implementation is preliminary, since it's not yet 100% clear to me how much that will be needed, and
        in which situations. Also, module versioning needs to be implemented before this can work reliably. Also, for now
        it is assumed that a module configuration is not changed once set, this also might change in the future

        Returns:
            this modules 'module_instance_hash'
        """

        # TODO:
        if self._module_hash is None:
            _d = {
                "module_cls": f"{self.__class__.__module__}.{self.__class__.__name__}",
                "version": "0.0.0",  # TODO: implement module versioning, package name might also need to be included here
                "config_hash": self.config.config_hash,
            }
            hashes = deepdiff.DeepHash(_d)
            self._module_hash = hashes[_d]
        return self._module_hash

    @property
    def info(self) -> KiaraModuleInstanceMetadata:

        if self._info is None:
            self._info = KiaraModuleInstanceMetadata.from_module_obj(self)
        return self._info

    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False
        return (self.full_id, self.config) == (self.full_id, other.config)

    def __hash__(self):
        return hash((self.__class__, self.full_id, self.config))

    def __repr__(self):
        return f"{self.__class__.__name__}(id={self.id} input_names={list(self.input_names)} output_names={list(self.output_names)})"

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        if not hasattr(self.__class__, "_module_type_id"):
            raise Exception(
                "Invalid model class, no '_module_type_id' attribute added. This is a bug"
            )

        r_gro: typing.List[typing.Any] = []
        md = self.info
        table = md.create_renderable()
        r_gro.append(table)

        yield Panel(
            RenderGroup(*r_gro),
            box=box.ROUNDED,
            title_align="left",
            title=f"Module: [b]{self.id}[/b]",
        )


class ModuleInfo(BaseModel):
    """A simple model class to hold and display information about a module.

    This is not used in processing at all, it is really only there to make it easier to communicate module characteristics..
    """

    @classmethod
    def from_module_cls(cls, module_cls: typing.Type[KiaraModule]):

        return ModuleInfo(
            metadata=KiaraModuleTypeMetadata.from_module_class(module_cls=module_cls)
        )

    class Config:
        extra = Extra.forbid
        allow_mutation = False

    metadata: KiaraModuleTypeMetadata = Field(description="The metadata of the module.")

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        table = self.metadata.create_renderable()
        yield Panel(
            table,
            box=box.ROUNDED,
            title=f"Module: [b]{self.metadata.type_name}[/b]",
            title_align="left",
        )


class ModulesList(object):
    def __init__(
        self, modules: typing.Iterable[str], kiara: typing.Optional["Kiara"] = None
    ):

        if kiara is None:
            from kiara import Kiara

            kiara = Kiara.instance()

        self._kiara: Kiara = kiara
        self._modules: typing.Iterable[str] = modules
        self._info_map: typing.Optional[typing.Dict[str, ModuleInfo]] = None
        self._print_only_first_line: bool = True

    @property
    def module_info_map(self) -> typing.Mapping[str, ModuleInfo]:

        if self._info_map is not None:
            return self._info_map

        from kiara.module import ModuleInfo

        result = {}

        for m in self._modules:
            cls = self._kiara.get_module_class(m)
            info = ModuleInfo.from_module_cls(cls)
            result[m] = info

        self._info_map = result
        return self._info_map

    def __repr__(self):

        return str(list(self._modules.keys()))

    def __str__(self):

        return self.__repr__()

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        table = Table(show_header=False, box=box.SIMPLE, show_lines=True)
        table.add_column("name", style="b")
        table.add_column("desc", style="i")

        for name, details in self.module_info_map.items():
            if self._print_only_first_line:
                table.add_row(name, details.metadata.documentation.description)
            else:
                table.add_row(name, details.metadata.documentation.full_doc)

        yield table
