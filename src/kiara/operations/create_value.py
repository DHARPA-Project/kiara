# -*- coding: utf-8 -*-
import abc
import typing
from pydantic import Field

from kiara import Kiara, KiaraModule
from kiara.data import ValueSet
from kiara.data.values import Value, ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import ModuleTypeConfigSchema
from kiara.operations import Operation, OperationType


class CreateValueModuleConfig(ModuleTypeConfigSchema):

    source_profile: str = Field(description="The profile of the source value.")
    target_type: str = Field(description="The type of the value to convert to.")
    allow_none_input: bool = Field(
        description="Whether to allow 'none' source values, if one is encountered 'none' is returned.",
        default=False,
    )


class CreateValueModule(KiaraModule):
    """Base class for 'create' value type operations."""

    _config_cls = CreateValueModuleConfig

    @classmethod
    def retrieve_module_profiles(
        cls, kiara: "Kiara"
    ) -> typing.Mapping[str, typing.Union[typing.Mapping[str, typing.Any], Operation]]:

        all_metadata_profiles: typing.Dict[
            str, typing.Dict[str, typing.Dict[str, typing.Any]]
        ] = {}

        # value_types: typing.Iterable[str] = cls.get_supported_value_types()
        # if "*" in value_types:
        #     value_types = kiara.type_mgmt.value_type_names
        #
        # for value_type in value_types:

        target_type = cls.get_target_value_type()

        if target_type not in kiara.type_mgmt.value_type_names:
            raise Exception(
                f"Can't assemble type convert operations for target type '{target_type}': type not available"
            )

        for source_profile in cls.get_source_value_profiles():

            mod_conf = {
                "source_profile": source_profile,
                "target_type": target_type,
            }

            op_config = {
                "module_type": cls._module_type_id,  # type: ignore
                "module_config": mod_conf,
                "doc": f"Create a '{target_type}' value from a '{source_profile}'.",
            }
            # key = f"{value_type}.convert_from.{target_type}"   # type: ignore
            # if key in all_metadata_profiles.keys():
            #     raise Exception(f"Duplicate profile key: {key}")
            # all_metadata_profiles[key] = op_config
            key = f"create.{target_type}.from.{source_profile}"
            if key in all_metadata_profiles.keys():
                raise Exception(f"Duplicate profile key: {key}")
            all_metadata_profiles[key] = op_config

        return all_metadata_profiles

    # @classmethod
    # def get_supported_value_types(cls) -> typing.Set[str]:
    #
    #     _types = cls._get_supported_value_types()
    #     if isinstance(_types, str):
    #         _types = [_types]
    #
    #     return set(_types)
    #
    @classmethod
    @abc.abstractmethod
    def get_target_value_type(cls) -> str:
        pass

    @classmethod
    def get_source_value_profiles(cls) -> typing.Iterable[str]:

        # supported = cls.get_supported_value_types()

        types = []
        for attr_name, attr in cls.__dict__.items():

            if attr_name.startswith("from_") and callable(attr):
                v_type = attr_name[5:]
                # if v_type in supported:
                #     raise Exception(
                #         f"Invalid configuration for type conversion module class {cls.__name__}: conversion source type can't be '{v_type}' because this is already registered as a supported type of the class."
                #     )
                types.append(v_type)

        return types

    # @classmethod
    # def get_target_value_type(cls) -> str:
    #
    #     supported = cls.get_supported_value_types()
    #
    #     types = []
    #     for attr_name, attr in cls.__dict__.items():
    #
    #         if attr_name.startswith("to_") and callable(attr):
    #             v_type = attr_name[3:]
    #             if v_type in supported:
    #                 raise Exception(
    #                     f"Invalid configuration for type conversion module class {cls.__name__}: conversion target type can't be '{v_type}' because this is already registered as a supported type of the class."
    #                 )
    #             types.append(attr_name[3:])
    #
    #     return types

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        source_profile = self.get_config_value("source_profile")

        source_config = dict(
            self._kiara.type_mgmt.get_type_config_for_data_profile(source_profile)
        )
        source_config["doc"] = f"The '{source_profile}' value to be converted."

        schema: typing.Dict[str, typing.Dict[str, typing.Any]] = {
            source_profile: source_config
        }
        if self.get_config_value("allow_none_input"):
            schema[source_config["type"]]["optional"] = True

        return schema

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        source_profile = self.get_config_value("source_profile")

        target_type = self.get_config_value("target_type")

        schema: typing.Dict[str, typing.Dict[str, typing.Any]] = {
            target_type: {
                "type": target_type,
                "doc": f"The converted '{source_profile}' value as '{target_type}'.",
            }
        }
        if self.get_config_value("allow_none_input"):
            schema[target_type]["optional"] = True

        return schema

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        source_profile: str = self.get_config_value("source_profile")
        source_config: typing.Mapping[
            str, typing.Mapping[str, typing.Any]
        ] = self._kiara.type_mgmt.get_type_config_for_data_profile(source_profile)
        source_type = source_config["type"]

        target_type: str = self.get_config_value("target_type")

        allow_none: bool = self.get_config_value("allow_none_input")

        source: Value = inputs.get_value_obj(source_profile)
        if source_type != source.type_name:
            raise KiaraProcessingException(
                f"Invalid type ({source.type_name}) of source value: expected '{source_type}' (source profile name: {source_profile})."
            )

        if not source.is_set or source.is_none:
            if allow_none:
                outputs.set_value("value_item", None)
                return
            else:
                raise KiaraProcessingException("No source value set.")

        if not hasattr(self, f"from_{source_profile}"):
            raise Exception(
                f"Module '{self._module_type_id}' can't convert '{source_type}' into '{target_type}': missing method 'from_{source_type}'. This is a bug."  # type: ignore
            )

        func = getattr(self, f"from_{source_profile}")

        converted = func(source)
        outputs.set_value(target_type, converted)


class CreateValueOperationType(OperationType):
    """Operations that create values of specific types from values of certain value profiles.

    In most cases, source profiles will be 'file' or 'file_bundle' values of some sort, and the created values will exhibit more inherent structure and stricter specs than their sources.

    The 'create' operation type differs from 'import' in that it expects values that are already onboarded and are all information in the value is stored in a *kiara* data registry (in most cases the *kiara data store*).
    """

    def is_matching_operation(self, op_config: Operation) -> bool:

        return issubclass(op_config.module_cls, CreateValueModule)

    # def get_operations_for_source_type(
    #     self, value_type: str
    # ) -> typing.Dict[str, Operation]:
    #     """Find all operations that transform from the specified type.
    #
    #     The result dict uses the target type of the conversion as key, and the operation itself as value.
    #     """
    #
    #     result: typing.Dict[str, Operation] = {}
    #     for o_id, op in self.operations.items():
    #         source_type = op.module_config["source_type"]
    #         if source_type == value_type:
    #             target_type = op.module_config["target_type"]
    #             if target_type in result.keys():
    #                 raise Exception(
    #                     f"Multiple operations to transform from '{source_type}' to {target_type}"
    #                 )
    #             result[target_type] = op
    #
    #     return result

    def get_operations_for_target_type(
        self, value_type: str
    ) -> typing.Dict[str, Operation]:
        """Find all operations that transform to the specified type.

        The result dict uses the source type of the conversion as key, and the operation itself as value.
        """

        result: typing.Dict[str, Operation] = {}
        for o_id, op in self.operations.items():
            target_type = op.module_config["target_type"]
            if target_type == value_type:
                source_type = op.module_config["source_type"]
                if source_type in result.keys():
                    raise Exception(
                        f"Multiple operations to transform from '{source_type}' to {target_type}"
                    )
                result[source_type] = op
        return result
