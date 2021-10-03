# -*- coding: utf-8 -*-
import abc
import deepdiff
import inspect
import typing
import uuid
from abc import abstractmethod
from pydantic import ValidationError
from rich import box
from rich.console import Console, ConsoleOptions, RenderGroup, RenderResult
from rich.panel import Panel

from kiara.data.values import Value, ValueLineage, ValueSchema
from kiara.data.values.value_set import SlottedValueSet, ValueSet
from kiara.defaults import SpecialValue
from kiara.exceptions import KiaraModuleConfigException
from kiara.metadata.module_models import (
    KiaraModuleInstanceMetadata,
    KiaraModuleTypeMetadata,
)
from kiara.module_config import KIARA_CONFIG, ModuleConfig, ModuleTypeConfigSchema
from kiara.operations import Operation
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

    def __init__(
        self,
        inputs: typing.Mapping[str, Value],
        title: typing.Optional[str] = None,
        kiara: typing.Optional["Kiara"] = None,
    ):

        self._inputs: typing.Mapping[str, Value] = inputs
        super().__init__(read_only=True, title=title, kiara=kiara)

    def get_all_field_names(self) -> typing.Iterable[str]:
        """All field names included in this ValueSet."""

        return self._inputs.keys()

    def _get_value_obj(
        self,
        field_name: str,
        ensure_metadata: typing.Union[bool, typing.Iterable[str], str] = False,
    ) -> Value:
        """Retrieve the value object for the specified field."""

        value = self._inputs[field_name]
        if ensure_metadata:
            if isinstance(ensure_metadata, bool):
                value.get_metadata()
            elif isinstance(ensure_metadata, str):
                value.get_metadata(ensure_metadata)
            elif isinstance(ensure_metadata, typing.Iterable):
                value.get_metadata(*ensure_metadata)
            else:
                raise ValueError(
                    f"Invalid type '{type(ensure_metadata)}' for 'ensure_metadata' argument."
                )
        return value

    def _set_values(
        self, **values: typing.Any
    ) -> typing.Mapping[str, typing.Union[bool, Exception]]:
        raise Exception("Inputs are read-only.")


class StepOutputs(ValueSet):
    """Wrapper class to hold a set of outputs for a pipeline processing step.

    This is necessary because we can't assume the processing will be done on the same machine (or in the same process)
    as the pipeline controller. By disconnecting the value from the processing code, we can react appropriately to
    those circumstances.

    Internally, this class stores two sets of its values: the 'actual', up-to-date values, and the referenced (original)
    ones that were used when creating an object of this class. It's not a good idea to keep both synced all the time,
    because that could potentially involve unnecessary data transfer and I/O.

    Also, in some cases a developer might want to avoid events that could be triggered by a changed value.

    Both value sets can be synced manually using the 'sync()' method.

    Arguments:
        outputs (ValueSet): the output values of a pipeline step
    """

    def __init__(
        self,
        outputs: ValueSet,
        title: typing.Optional[str] = None,
        kiara: typing.Optional["Kiara"] = None,
    ):

        self._outputs_staging: typing.Dict[str, typing.Any] = {}
        self._outputs: ValueSet = outputs
        super().__init__(read_only=False, title=title, kiara=kiara)

    def get_all_field_names(self) -> typing.Iterable[str]:
        """All field names included in this ValueSet."""

        return self._outputs.get_all_field_names()

    def _set_values(
        self, **values: typing.Any
    ) -> typing.Mapping[str, typing.Union[bool, Exception]]:

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
            # value_obj = self._outputs.get_value_obj(output_name)
            if (
                output_name not in self._outputs_staging.keys()  # type: ignore
                or value != self._outputs_staging[output_name]  # type: ignore
            ):
                self._outputs_staging[output_name] = value  # type: ignore
                result[output_name] = True
            else:
                result[output_name] = False

        return result

    def _get_value_obj(self, output_name):
        """Retrieve the value object for the specified field."""

        self.sync()
        return self._outputs.get_value_obj(output_name)

    def sync(self):
        """Sync this value sets 'shadow' values with the ones a user would retrieve."""

        self._outputs.set_values(**self._outputs_staging)  # type: ignore
        self._outputs_staging.clear()  # type: ignore


class KiaraModule(typing.Generic[KIARA_CONFIG], abc.ABC):
    """The base class that every custom module in *Kiara* needs to inherit from.

    The core of every ``KiaraModule`` is a ``process`` method, which should be a 'pure',
     idempotent function that creates one or several output values from the given input(s), and its purpose is to transfor
     a set of inputs into a set of outputs.

     Every module can be configured. The module configuration schema can differ, but every one such configuration needs to
     subclass the [ModuleTypeConfigSchema][kiara.module_config.ModuleTypeConfigSchema] class and set as the value to the
     ``_config_cls`` attribute of the module class. This is useful, because it allows for some modules to serve a much
     larger variety of use-cases than non-configurable modules would be, which would mean more code duplication because
     of very simlilar, but slightly different module types.

     Each module class (type) has a unique -- within a *kiara* context -- module type id which can be accessed via the
     ``_module_type_id`` class attribute.

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
    _config_cls: typing.Type[KIARA_CONFIG] = ModuleTypeConfigSchema  # type: ignore

    @classmethod
    def create_instance(
        cls,
        module_type: typing.Optional[str] = None,
        module_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        kiara: typing.Optional["Kiara"] = None,
    ) -> "KiaraModule":
        """Create an instance of a *kiara* module.

        This class method is overloaded in a way that you can either provide the `module_type` argument, in which case
        the relevant sub-class will be queried from the *kiara* context, or you can call this method directly on any of the
        inehreting sub-classes. You can't do both, though.

        Arguments:
            module_type: must be None if called on the ``KiaraModule`` base class, otherwise the module or operation id
            module_config: the configuration of the module instance
            kiara: the *kiara* context
        """

        if cls == KiaraModule:
            if not module_type:
                raise Exception(
                    "This method must be either called on a subclass of KiaraModule, not KiaraModule itself, or it needs the 'module_type' argument specified."
                )
        else:
            if module_type:
                raise Exception(
                    "This method must be either called without the 'module_type' argument specified, or on a subclass of the KiaraModule class, but not both."
                )

        if cls == KiaraModule:
            assert module_type is not None
            module_conf = ModuleConfig.create_module_config(
                config=module_type, module_config=module_config, kiara=kiara
            )
        else:
            module_conf = ModuleConfig.create_module_config(
                config=cls, module_config=module_config, kiara=kiara
            )

        return module_conf.create_module(kiara=kiara)

    @classmethod
    def retrieve_module_profiles(
        cls, kiara: "Kiara"
    ) -> typing.Mapping[str, typing.Union[typing.Mapping[str, typing.Any], Operation]]:
        """Retrieve a collection of profiles (pre-set module configs) for this *kiara* module type.

        This is used to automatically create generally useful operations (incl. their ids).
        """

    @classmethod
    def get_type_metadata(cls) -> KiaraModuleTypeMetadata:
        """Return all metadata associated with this module type."""

        return KiaraModuleTypeMetadata.from_module_class(cls)

    # @classmethod
    # def profiles(
    #     cls,
    # ) -> typing.Optional[typing.Mapping[str, typing.Mapping[str, typing.Any]]]:
    #     """Retrieve a collection of profiles (pre-set module configs) for this *kiara* module type.
    #
    #     This is used to automatically create generally useful operations (incl. their ids).
    #     """
    #     return None

    @classmethod
    def is_pipeline(cls) -> bool:
        """Check whether this module type is a pipeline, or not."""
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

        if isinstance(module_config, ModuleTypeConfigSchema):
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
        self._constants: typing.Mapping[str, ValueSchema] = None  # type: ignore
        self._merged_input_schemas: typing.Mapping[str, ValueSchema] = None  # type: ignore
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

    def input_required(self, input_name: str):

        if input_name not in self._input_schemas.keys():
            raise Exception(f"No input '{input_name}' for module '{self.id}'.")

        if not self._input_schemas[input_name].is_required():
            return False

        if input_name in self.constants.keys():
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
        """Abstract method to implement by child classes, returns a description of the input schema of this module.

        If returning a dictionary of dictionaries, the format of the return value is as follows (items with '*' are optional):

        {
          "[input_field_name]: {
              "type": "[value_type]",
              "doc*": "[a description of this input]",
              "optional*': [boolean whether this input is optional or required (defaults to 'False')]
          "[other_input_field_name]: {
              "type: ...
              ...
          }

        """

    @abstractmethod
    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        """Abstract method to implement by child classes, returns a description of the output schema of this module.

        If returning a dictionary of dictionaries, the format of the return value is as follows (items with '*' are optional):

        {
          "[output_field_name]: {
              "type": "[value_type]",
              "doc*": "[a description of this output]"
          "[other_input_field_name]: {
              "type: ...
              ...
          }
        """

    @property
    def input_schemas(self) -> typing.Mapping[str, ValueSchema]:
        """The input schema for this module."""

        if self._input_schemas is None:
            self._create_input_schemas()

        return self._input_schemas  # type: ignore

    @property
    def full_input_schemas(self) -> typing.Mapping[str, ValueSchema]:

        if self._merged_input_schemas is not None:
            return self._merged_input_schemas

        self._merged_input_schemas = dict(self.input_schemas)
        self._merged_input_schemas.update(self.constants)
        return self._merged_input_schemas

    @property
    def constants(self) -> typing.Mapping[str, ValueSchema]:

        if self._constants is None:
            self._create_input_schemas()
        return self._constants  # type: ignore

    def _create_input_schemas(self) -> None:

        try:
            _input_schemas_data = self.create_input_schema()

            if not _input_schemas_data:
                raise Exception(
                    f"Invalid module implementation for '{self.__class__.__name__}': empty input schema"
                )

            try:
                _input_schemas = create_schemas(
                    schema_config=_input_schemas_data, kiara=self._kiara
                )
            except Exception as e:
                raise Exception(
                    f"Can't create input schemas for module {self.full_id}: {e}"
                )

            defaults = self.config.defaults
            constants = self.config.constants

            for k, v in defaults.items():
                if k not in _input_schemas.keys():
                    raise Exception(
                        f"Can't create inputs for module '{self._module_type_id}', invalid default field name '{k}'. Available field names: '{', '.join(_input_schemas.keys())}'"  # type: ignore
                    )

            for k, v in constants.items():
                if k not in _input_schemas.keys():
                    raise Exception(
                        f"Can't create inputs for module '{self._module_type_id}', invalid constant field name '{k}'. Available field names: '{', '.join(_input_schemas.keys())}'"  # type: ignore
                    )

            self._input_schemas, self._constants = overlay_constants_and_defaults(
                _input_schemas, defaults=defaults, constants=constants
            )
        except Exception as e:
            raise Exception(f"Can't create input schemas for module of type '{self.__class__._module_type_id}': {e}")  # type: ignore

    @property
    def output_schemas(self) -> typing.Mapping[str, ValueSchema]:
        """The output schema for this module."""

        if self._output_schemas is not None:
            return self._output_schemas

        try:
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
        except Exception as e:
            raise Exception(f"Can't create output schemas for module of type '{self.__class__._module_type_id}': {e}")  # type: ignore

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

    def create_full_inputs(self, **inputs: typing.Any) -> typing.Mapping[str, Value]:

        # TODO: find a generic way to do this kind of stuff
        def clean_value(v: typing.Any) -> typing.Any:
            if hasattr(v, "as_py"):
                return v.as_py()  # type: ignore
            else:
                return v

        resolved_inputs: typing.Dict[str, Value] = {}

        for k, v in self.constants.items():
            if k in inputs.keys():
                raise Exception(f"Invalid input: value provided for constant '{k}'")
            inputs[k] = v

        for k, value in inputs.items():
            value = clean_value(value)
            if not isinstance(value, Value):
                if (
                    k not in self.input_schemas.keys()
                    and k not in self.constants.keys()
                ):
                    raise Exception(
                        f"Invalid input name '{k} for module {self._module_type_id}. Not part of the schema, allowed input names: {', '.join(self.input_names)}"  # type: ignore
                    )
                if k in self.input_schemas.keys():
                    schema = self.input_schemas[k]
                    value = self._kiara.data_registry.register_data(
                        value_data=value, value_schema=schema
                    )
                    # value = Value(
                    #     value_data=value,  # type: ignore
                    #     value_schema=schema,
                    #     is_constant=False,
                    #     registry=self._kiara.data_registry,  # type: ignore
                    # )
                else:
                    schema = self.constants[k]
                    value = self._kiara.data_registry.register_data(
                        value_data=SpecialValue.NOT_SET,
                        value_schema=schema,
                    )
                    # value = Value(
                    #     value_schema=schema,
                    #     is_constant=False,
                    #     kiara=self._kiara,  # type: ignore
                    #     registry=self._kiara.data_registry,  # type: ignore
                    # )
            resolved_inputs[k] = value

        return resolved_inputs

    def run(self, _attach_lineage: bool = True, **inputs: typing.Any) -> ValueSet:
        """Execute the module with the provided inputs directly.

        Arguments:
            inputs: a map of the input values (as described by the input schema
        Returns:
            a map of the output values (as described by the output schema)
        """

        resolved_inputs = self.create_full_inputs(**inputs)

        # TODO: introduce a 'temp' value set implementation and use that here
        input_value_set = SlottedValueSet.from_schemas(
            kiara=self._kiara,
            schemas=self.full_input_schemas,
            read_only=True,
            initial_values=resolved_inputs,
            title=f"module_inputs_{self.id}",
        )

        if not input_value_set.items_are_valid():

            invalid_details = input_value_set.check_invalid()
            raise Exception(
                f"Can't process module '{self._module_type_name}', input field(s) not valid: {', '.join(invalid_details.keys())}"  # type: ignore
            )

        output_value_set = SlottedValueSet.from_schemas(
            kiara=self._kiara,
            schemas=self.output_schemas,
            read_only=False,
            title=f"{self._module_type_name}_module_outputs_{self.id}",  # type: ignore
            default_value=SpecialValue.NOT_SET,
        )

        self.process(inputs=input_value_set, outputs=output_value_set)  # type: ignore

        result_outputs: typing.MutableMapping[str, Value] = {}
        if _attach_lineage:
            input_infos = {k: v.get_info() for k, v in resolved_inputs.items()}
            for field_name, output in output_value_set.items():
                value_lineage = ValueLineage.from_module_and_inputs(
                    module=self, output_name=field_name, inputs=input_infos
                )
                # value_lineage = None
                output_val = self._kiara.data_registry.register_data(
                    value_data=output, value_lineage=value_lineage
                )
                result_outputs[field_name] = output_val
        else:
            result_outputs = output_value_set

        result_set = SlottedValueSet.from_schemas(
            kiara=self._kiara,
            schemas=self.output_schemas,
            read_only=True,
            initial_values=result_outputs,
            title=f"{self._module_type_name}_module_outputs_{self.id}",  # type: ignore
        )

        return result_set

        # result = output_value_set.get_all_value_objects()
        # return output_value_set
        # return ValueSetImpl(items=result, read_only=True)

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
        """Return an info wrapper class for this module."""

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
