# -*- coding: utf-8 -*-
import deepdiff
import inspect
import json
import textwrap
import typing
from abc import abstractmethod
from pydantic import (
    BaseModel,
    Extra,
    Field,
    PrivateAttr,
    ValidationError,
    root_validator,
)
from rich import box
from rich.console import Console, ConsoleOptions, RenderGroup, RenderResult
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from kiara.config import KIARA_CONFIG, KiaraModuleConfig
from kiara.data.values import (
    NonRegistryValue,
    Value,
    ValueSchema,
    ValueSet,
    ValueSetImpl,
)
from kiara.exceptions import KiaraModuleConfigException
from kiara.utils import (
    StringYAML,
    create_table_from_config_class,
    create_table_from_field_schemas,
    is_debug,
)

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

    def get_value_obj(self, input_name) -> Value:

        return self.__dict__["_inputs"][input_name]

    def _set_values(self, **values: typing.Any) -> typing.Dict[Value, bool]:
        raise Exception("Inputs are read-only.")

    def is_read_only(self) -> bool:
        return True


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
    def doc(cls) -> str:
        doc = cls.__doc__
        if not doc:
            doc = "-- n/a --"
        else:
            doc = inspect.cleandoc(doc)
        return doc

    @classmethod
    def is_pipeline(cls) -> bool:
        return False

    @classmethod
    def doc_link(cls) -> typing.Optional[str]:

        if cls.is_pipeline():
            x = "pipelines_list"
        else:
            x = "modules_list"

        if hasattr(cls, "_module_type_id") and cls.__module__.startswith(
            "kiara_modules.default"
        ):
            link = f"https://dharpa.org/kiara_modules.default/{x}/#{cls._module_type_id}"  # type: ignore
            return link
        else:
            return None

    @classmethod
    def source_link(cls) -> typing.Optional[str]:

        if cls.is_pipeline():
            return None
        else:
            if cls.__module__.startswith("kiara_modules.default"):
                base_url = "https://dharpa.org/kiara_modules.default/api_reference"
                url = f"{base_url}/{cls.__module__}/#{cls.__module__}.{cls.__name__}"
                return url
            else:
                return None

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

        result = {}
        for k, v in _input_schemas.items():
            if isinstance(v, ValueSchema):
                result[k] = v
            elif isinstance(v, typing.Mapping):
                schema = ValueSchema(**v)
                schema.validate_types(self._kiara)
                result[k] = schema
            else:
                raise Exception(
                    f"Invalid return type when tryping to create schema for '{self.id}': {type(v)}"
                )

        self._input_schemas = result

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

        result = {}
        for k, v in _output_schema.items():
            if isinstance(v, ValueSchema):
                result[k] = v
            elif isinstance(v, typing.Mapping):
                schema = ValueSchema(**v)
                schema.validate_types(self._kiara)
                result[k] = schema
            else:
                raise Exception(
                    f"Invalid return type when tryping to create schema for '{self.id}': {type(v)}"
                )

        self._output_schemas = result

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

        try:
            self.process(inputs=inputs, outputs=outputs)
        except Exception as e:
            if is_debug():
                try:
                    import traceback

                    traceback.print_exc()
                except Exception:
                    pass
            raise e

    @abstractmethod
    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
        """Abstract method to implement by child classes, should be a pure, idempotent function that uses the values from ``inputs``, and stores results in the provided ``outputs`` object.

        Arguments:
            inputs: the input value set
            outputs: the output value set
        """

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

        self.process(inputs=input_value_set, outputs=output_value_set)

        result = output_value_set.get_all_value_objects()
        return ValueSetImpl(items=result, read_only=True)

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
        doc = self.doc()
        if doc and doc != "-- n/a --":
            r_gro.append(f"\n  {self.doc()}\n")

        table = Table(box=box.SIMPLE, show_header=False)
        table.add_column("property", style="i")
        table.add_column("value")

        doc_link = self.doc_link()
        if doc_link:
            m_str = f"[link={doc_link}]{self.__class__._module_type_id}[/link]"  # type: ignore
        else:
            m_str = self.__class__._module_type_id  # type: ignore
        table.add_row("module_type", m_str)
        table.add_row(
            "module_class", f"{self.__class__.__module__}.{self.__class__.__name__}"
        )
        table.add_row("is pipeline", "yes" if self.__class__.is_pipeline() else "no")
        config = self.config.dict()
        config.pop("doc", None)
        config.pop("steps", None)
        config.pop("input_aliases", None)
        config.pop("output_aliases", None)
        config.pop("module_type_name", None)
        config_str = json.dumps(config, indent=2)
        c = Syntax(config_str, "json", background_color="default")
        table.add_row("config", c)
        inputs_table = create_table_from_field_schemas(
            _show_header=True, **self.input_schemas
        )
        table.add_row("inputs", inputs_table)
        outputs_table = create_table_from_field_schemas(
            _add_required=False,
            _add_default=False,
            _show_header=True,
            **self.output_schemas,
        )
        table.add_row("outputs", outputs_table)
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
            from kiara import Kiara

            kiara = Kiara.instance()
            data["_kiara"] = kiara
        self._kiara: "Kiara" = kiara
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

        doc = module_cls.doc()

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

        my_table = Table(box=box.SIMPLE, show_lines=True, show_header=False)
        my_table.add_column("Property", style="i")
        my_table.add_column("Value")
        my_table.add_row(
            "class", f"{self.module_cls.__module__}.{self.module_cls.__qualname__}"
        )
        my_table.add_row(
            "is pipeline", "yes" if self.module_cls.is_pipeline() else "no"
        )
        my_table.add_row("doc", self.doc)
        source_link = self.module_cls.source_link()
        if source_link is None:
            source_link = "-- n/a --"
        else:
            source_link = f"[i link={source_link}]kiara_modules.default.{self.module_type}[/i link]"
        my_table.add_row("source repo", source_link)
        my_table.add_row(
            "config class",
            f"{self.config_cls.__module__}.{self.config_cls.__qualname__}",
        )
        my_table.add_row("config", create_table_from_config_class(self.config_cls))
        if not self.module_cls.is_pipeline():
            syn_src = Syntax(self.process_src, "python")
        else:
            base_pipeline_config = self.module_cls._base_pipeline_config.dict()  # type: ignore
            yaml_str = yaml.dump(base_pipeline_config)
            syn_src = Syntax(yaml_str, "yaml", background_color="default")
        my_table.add_row("src", syn_src)

        yield Panel(
            my_table,
            box=box.ROUNDED,
            title=f"Module: [b]{self.module_type}[/b]",
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

    @property
    def module_info_map(self) -> typing.Mapping[str, ModuleInfo]:

        if self._info_map is not None:
            return self._info_map

        from kiara.module import ModuleInfo

        result = {}
        for m in self._modules:
            info = ModuleInfo(_kiara=self._kiara, module_type=m)
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
            table.add_row(name, details.doc)

        yield table
