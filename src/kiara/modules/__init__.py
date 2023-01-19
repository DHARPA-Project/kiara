# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
import inspect
import structlog
import uuid
from abc import abstractmethod
from multiformats import CID
from pydantic import BaseModel, Field, ValidationError
from rich.console import RenderableType
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    Iterable,
    Mapping,
    Type,
    TypeVar,
    Union,
)

from kiara.exceptions import (
    InvalidValuesException,
    KiaraException,
    KiaraModuleConfigException,
    KiaraProcessingException,
)
from kiara.models.module import KiaraModuleConfig
from kiara.models.module.jobs import JobLog
from kiara.models.values.value_schema import ValueSchema
from kiara.utils import is_debug, log_exception
from kiara.utils.hashing import compute_cid
from kiara.utils.values import (
    augment_values,
    create_schema_dict,
    overlay_constants_and_defaults,
)
from kiara.utils.yaml import StringYAML

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.models.module.manifest import Manifest
    from kiara.models.values.value import ValueMap
    from kiara.operations import Operation

yaml = StringYAML()

KIARA_CONFIG = TypeVar("KIARA_CONFIG", bound=KiaraModuleConfig)

ValueMapSchema = Mapping[str, Union[ValueSchema, Mapping[str, Any]]]
ValueSetSchema = Mapping[
    str, Union[ValueSchema, Mapping[str, Any]]
]  # TODO: remove once all references to this are gone (legacy)

log = structlog.getLogger()


class InputOutputObject(abc.ABC):
    """Abstract base class for classes that define inputs and outputs schemas.

    Both the 'create_inputs_schema` and `creawte_outputs_schema` methods implemented by child classes return a description of the input schema of this module.

    If returning a dictionary of dictionaries, the format of the return value is as follows (items with '*' are optional):

    ```
        {
          "[input_field_name]: {
              "type": "[type]",
              "doc*": "[a description of this input]",
              "optional*': [boolean whether this input is optional or required (defaults to 'False')]
          "[other_input_field_name]: {
              "type: ...
              ...
          }
              ```
    """

    def __init__(
        self,
        alias: str,
        config: Union[KiaraModuleConfig, None] = None,
        allow_empty_inputs_schema: bool = False,
        allow_empty_outputs_schema: bool = False,
    ):

        self._alias: str = alias
        self._inputs_schema: Mapping[str, ValueSchema] = None  # type: ignore
        self._full_inputs_schema: Mapping[str, ValueSchema] = None  # type: ignore
        self._outputs_schema: Mapping[str, ValueSchema] = None  # type: ignore
        self._constants: Mapping[str, ValueSchema] = None  # type: ignore

        if config is None:
            config = KiaraModuleConfig()
        self._config: KiaraModuleConfig = config

        self._allow_empty_inputs: bool = allow_empty_inputs_schema
        self._allow_empty_outputs: bool = allow_empty_outputs_schema

    @property
    def alias(self) -> str:
        return self._alias

    def input_required(self, input_name: str):

        if input_name not in self._inputs_schema.keys():
            raise Exception(
                f"No input '{input_name}', available inputs: {', '.join(self._inputs_schema)}"
            )

        if not self._inputs_schema[input_name].is_required():
            return False

        if input_name in self.constants.keys():
            return False
        else:
            return True

    @abstractmethod
    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:
        """Return the schema for this types' inputs."""

    @abstractmethod
    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:
        """Return the schema for this types' outputs."""

    @property
    def inputs_schema(self) -> Mapping[str, ValueSchema]:
        """The input schema for this module."""

        if self._inputs_schema is None:
            self._create_inputs_schema()

        return self._inputs_schema  # type: ignore

    @property
    def full_inputs_schema(self) -> Mapping[str, ValueSchema]:

        if self._full_inputs_schema is not None:
            return self._full_inputs_schema

        self._full_inputs_schema = dict(self.inputs_schema)
        self._full_inputs_schema.update(self._constants)
        return self._full_inputs_schema

    @property
    def constants(self) -> Mapping[str, ValueSchema]:

        if self._constants is None:
            self._create_inputs_schema()
        return self._constants  # type: ignore

    def _create_inputs_schema(self) -> None:
        """Assemble the inputs schema and assign it to the approriate instance attributes.

        DEV NOTE: if anything in this method is changed, also change the method of the AutoInputsKiaraModule
        in the kiara_pluginc.core_types package, since it's a carbon copy if this, except for a small change.
        """
        try:
            _input_schemas_data = self.create_inputs_schema()

            if _input_schemas_data is None:
                raise Exception(
                    f"Invalid inputs implementation for '{self.alias}': no inputs schema"
                )

            if not _input_schemas_data and not self._allow_empty_inputs:
                raise Exception(
                    f"Invalid inputs implementation for '{self.alias}': empty inputs schema"
                )
            try:
                _input_schemas = create_schema_dict(schema_config=_input_schemas_data)
            except Exception as e:
                raise Exception(f"Can't create input schemas for '{self.alias}': {e}")

            defaults = self._config.defaults
            constants = self._config.constants

            for k, v in defaults.items():
                if k not in _input_schemas.keys():
                    raise Exception(
                        f"Can't create inputs for '{self.alias}', invalid default field name '{k}'. Available field names: '{', '.join(_input_schemas.keys())}'"  # type: ignore
                    )

            for k, v in constants.items():
                if k not in _input_schemas.keys():
                    raise Exception(
                        f"Can't create inputs for '{self.alias}', invalid constant field name '{k}'. Available field names: '{', '.join(_input_schemas.keys())}'"  # type: ignore
                    )

            self._inputs_schema, self._constants = overlay_constants_and_defaults(
                _input_schemas, defaults=defaults, constants=constants
            )

        except Exception as e:
            raise KiaraException(f"Can't create input schemas for instance of '{self.alias}'.", parent=e)  # type: ignore

    @property
    def outputs_schema(self) -> Mapping[str, ValueSchema]:
        """The output schema for this module."""

        if self._outputs_schema is not None:
            return self._outputs_schema

        try:
            _output_schema = self.create_outputs_schema()

            if _output_schema is None:
                raise Exception(
                    f"Invalid outputs implementation for '{self.alias}': no outputs schema"
                )

            if not _output_schema and not self._allow_empty_outputs:
                raise Exception(
                    f"Invalid outputs implementation for '{self.alias}': empty outputs schema"
                )

            try:
                self._outputs_schema = create_schema_dict(schema_config=_output_schema)
            except Exception as e:
                raise Exception(
                    f"Can't create output schemas for module {self.alias}: {e}"
                )

            return self._outputs_schema
        except Exception as e:
            if is_debug():
                import traceback

                traceback.print_exc()
            raise KiaraException(
                f"Can't create output schemas for instance of module '{self.alias}': {e}",
                parent=e,
            )

    @property
    def input_names(self) -> Iterable[str]:
        """A list of input field names for this module."""
        return self.inputs_schema.keys()

    @property
    def output_names(self) -> Iterable[str]:
        """A list of output field names for this module."""
        return self.outputs_schema.keys()

    def augment_module_inputs(self, inputs: Mapping[str, Any]) -> Dict[str, Any]:

        augmented = augment_values(
            values=inputs, schemas=self.inputs_schema, constants=self.constants
        )

        return augmented

    # def augment_outputs(self, outputs: Mapping[str, Any]) -> Dict[str, Any]:
    #     return augment_values(values=outputs, schemas=self.outputs_schema)


class ModuleCharacteristics(BaseModel):
    class Config:
        allow_mutation = False

    is_idempotent: bool = Field(
        description="Whether this module is idempotent (aka always produces the same output with the same inputs.",
        default=True,
    )
    is_internal: bool = Field(
        description="Hint for frontends whether this module is used predominantly internally, and users won't need to know of its existence.",
        default=False,
    )
    unique_result_values: bool = Field(
        description="Don't re-use existing values for outputs that have matching hashes in the data store.",
        default=True,
    )


DEFAULT_IDEMPOTENT_MODULE_CHARACTERISTICS = ModuleCharacteristics()
DEFAULT_NO_IDEMPOTENT_MODULE_CHARACTERISTICS = ModuleCharacteristics(
    is_idempotent=False
)
DEFAULT_IDEMPOTENT_INTERNAL_MODULE_CHARACTERISTICS = ModuleCharacteristics(
    is_internal=True
)


class KiaraModule(InputOutputObject, Generic[KIARA_CONFIG]):
    """The base class that every custom module in *Kiara* needs to inherit from.

    The core of every ``KiaraModule`` is a ``process`` method, which should be a 'pure',
     idempotent function that creates one or several output values from the given input(s), and its purpose is to transfor
     a set of inputs into a set of outputs.

     Every module can be configured. The module configuration schema can differ, but every one such configuration needs to
     subclass the [KiaraModuleConfig][kiara.module_config.KiaraModuleConfig] class and set as the value to the
     ``_config_cls`` attribute of the module class. This is useful, because it allows for some modules to serve a much
     larger variety of use-cases than non-configurable modules would be, which would mean more code duplication because
     of very simlilar, but slightly different module data_types.

     Each module class (type) has a unique -- within a *kiara* context -- module type id which can be accessed via the
     ``_module_type_name`` class attribute.

    Examples:

        A simple example would be an 'addition' module, with ``a`` and ``b`` configured as inputs, and ``z`` as the output field name.

        An implementing class would look something like this:

        TODO

    Arguments:
        module_config: the configuation for this module
    """

    # TODO: not quite sure about this generic type here, mypy doesn't seem to like it
    _config_cls: Type[KIARA_CONFIG] = KiaraModuleConfig  # type: ignore

    @classmethod
    def is_pipeline(cls) -> bool:
        """Check whether this module type is a pipeline, or not."""
        return False

    @classmethod
    def _calculate_module_cid(
        cls, module_type_config: Union[Mapping[str, Any], KIARA_CONFIG]
    ) -> CID:

        if isinstance(module_type_config, Mapping):
            module_type_config = cls._resolve_module_config(**module_type_config)

        obj = {
            "module_type": cls._module_type_name,  # type: ignore
            "module_type_config": module_type_config.dict(),
        }
        _, cid = compute_cid(data=obj)
        return cid

    @classmethod
    def _resolve_module_config(cls, **config: Any) -> KIARA_CONFIG:

        _config = cls._config_cls(**config)  # type: ignore

        return _config

    def __init__(
        self,
        module_config: Union[None, KIARA_CONFIG, Mapping[str, Any]] = None,
    ):
        self._id: uuid.UUID = uuid.uuid4()

        if isinstance(module_config, KiaraModuleConfig):
            self._config: KIARA_CONFIG = module_config  # type: ignore
        elif module_config is None:
            self._config = self.__class__._config_cls()
        elif isinstance(module_config, Mapping):
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

        self._module_cid: Union[CID, None] = None
        self._characteristics: Union[ModuleCharacteristics, None] = None

        super().__init__(alias=self.__class__._module_type_name, config=self._config)  # type: ignore

        self._operation: Union[Operation, None] = None
        # self._merged_input_schemas: typing.Mapping[str, ValueSchema] = None  # type: ignore
        self._manifest_cache: Union[None, Manifest] = None

    @property
    def manifest(self) -> "Manifest":
        if self._manifest_cache is None:
            self._manifest_cache = Manifest(
                module_type=self.module_type_name,
                module_config=self.config.dict(),
                is_resolved=True,
            )
        return self._manifest_cache

    @property
    def module_id(self) -> uuid.UUID:
        """The id of this module."""
        return self._id

    @property
    def module_type_name(self) -> str:
        if not self._module_type_name:  # type: ignore
            raise Exception(
                f"Module class '{self.__class__.__name__}' does not have a '_module_type_name' attribute. This is a bug."
            )
        return self._module_type_name  # type: ignore

    @property
    def config(self) -> KIARA_CONFIG:
        """Retrieve the configuration object for this module.

        Returns:
            the module-class-specific config object
        """
        return self._config

    @property
    def module_instance_cid(self) -> CID:

        if self._module_cid is None:
            self._module_cid = self.__class__._calculate_module_cid(self._config)
        return self._module_cid

    @property
    def characteristics(self) -> ModuleCharacteristics:
        if self._characteristics is not None:
            return self._characteristics

        self._characteristics = self._retrieve_module_characteristics()
        return self._characteristics

    def _retrieve_module_characteristics(self) -> ModuleCharacteristics:

        return DEFAULT_IDEMPOTENT_MODULE_CHARACTERISTICS

    def get_config_value(self, key: str) -> Any:
        """Retrieve the value for a specific configuration option.

        Arguments:
            key: the config key

        Returns:
            the value for the provided key
        """

        try:
            return self.config.get(key)
        except Exception:
            raise Exception(
                f"Error accessing config value '{key}' in module {self.__class__._module_type_name}."  # type: ignore
            )

    def run(self, kiara: "Kiara", **inputs: Any) -> "ValueMap":
        """Run the module ad-hoc.

        This is mostly used in unit tests, you typically want to run a module via the KiaraAPI instance.

        Arguments:
            kiara: the kiara context
            inputs: the inputs for this module

        Returns:
            the outputs of this module run as a ValueMap instance
        """

        from kiara.models.values.value import ValueMap, ValueMapWritable, ValuePedigree

        _inputs: ValueMap = kiara.data_registry.create_valuemap(
            data=inputs, schema=self.inputs_schema
        )

        if _inputs.check_invalid():
            raise InvalidValuesException(
                msg=f"Invalid inputs for module '{self.module_type_name}'.",
                invalid_values=_inputs.check_invalid(),
            )
        environments = {
            env_name: env.instance_id
            for env_name, env in kiara.current_environments.items()
        }

        result_pedigree = ValuePedigree(
            kiara_id=kiara.id,
            module_type=self.module_type_name,
            module_config=self.config.dict(),
            inputs={k: v.value_id for k, v in _inputs.items()},
            environments=environments,
        )

        unique_result_values = self.characteristics.unique_result_values

        outputs = ValueMapWritable.create_from_schema(
            kiara=kiara,
            schema=self.outputs_schema,
            pedigree=result_pedigree,
            unique_value_ids=unique_result_values,
        )
        job_log = JobLog()
        self.process_step(inputs=_inputs, outputs=outputs, job_log=job_log)

        return outputs

    def process_step(
        self, inputs: "ValueMap", outputs: "ValueMap", job_log: JobLog
    ) -> None:
        """Kick off processing for a specific set of input/outputs.

        This method calls the implemented [process][kiara.module.KiaraModule.process] method of the inheriting class,
        as well as wrapping input/output-data related functionality.

        Arguments:
            inputs: the input value set
            outputs: the output value set
        """

        signature = inspect.signature(self.process)  # type: ignore

        process_inputs: Dict[str, Any] = {
            "inputs": inputs,
            "outputs": outputs,
        }

        if "job_log" in signature.parameters.keys():
            process_inputs["job_log"] = job_log

        try:
            self.process(**process_inputs)  # type: ignore
        except KiaraProcessingException as kpe:
            log_exception(kpe)
            raise kpe
        except Exception as e:
            log_exception(e)
            raise KiaraProcessingException(e, module=self, inputs=inputs)

    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False
        return self.module_instance_cid == other.module_instance_cid

    def __hash__(self):
        return int.from_bytes(self.module_instance_cid.digest, "big")

    def __repr__(self):
        return f"{self.__class__.__name__}(id={self.module_id} module_type={self.module_type_name} input_names={list(self.input_names)} output_names={list(self.output_names)})"

    @property
    def operation(self) -> "Operation":

        if self._operation is not None:
            return self._operation

        from kiara.models.module.operation import Operation

        self._operation = Operation.create_from_module(self)
        return self._operation

    def create_renderable(self, **config) -> RenderableType:

        return self.operation.create_renderable(**config)
