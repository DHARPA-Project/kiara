# -*- coding: utf-8 -*-
import typing
from pydantic import BaseModel, Extra, Field, PrivateAttr

if typing.TYPE_CHECKING:
    from kiara import Kiara
    from kiara.module import KiaraModule


class ModuleProfileConfig(BaseModel):
    class Config:
        extra = Extra.forbid
        validate_all = True

    _module: typing.Optional["KiaraModule"] = PrivateAttr(default=None)
    module_type: str = Field(description="The module type.")
    module_config: typing.Dict[str, typing.Any] = Field(
        default_factory=dict, description="The configuration for the module."
    )

    def create_module(self, kiara: "Kiara"):

        if self._module is None:
            self._module = kiara.create_module(
                id=f"extract_metadata_{self.module_type}",
                module_type=self.module_type,
                module_config=self.module_config,
            )
        return self._module


class TypeConversionProfileConfig(ModuleProfileConfig):

    convert_source_type: str = Field(description="The source type.")
    convert_target_type: str = Field(description="The target type.")
    doc: str = Field(
        description="Documentation of what this profile does/is appropriate for.",
        default="-- n/a --",
    )


class ExtractMetadataProfileConfig(ModuleProfileConfig):

    value_type: str = Field(
        description="The type of the value to extract metadata from."
    )


class ModuleProfileMgmt(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: "Kiara" = kiara
        self._convert_profiles: typing.Dict[
            str, typing.Dict[str, TypeConversionProfileConfig]
        ] = None  # type: ignore
        self._metadata_profiles: typing.Dict[
            str, typing.Dict[str, ExtractMetadataProfileConfig]
        ] = None  # type: ignore

    @property
    def extract_metadata_profiles(
        self,
    ) -> typing.Mapping[str, typing.Mapping[str, ExtractMetadataProfileConfig]]:

        if self._metadata_profiles is not None:
            return self._metadata_profiles

        from kiara.modules.metadata import ExtractMetadataModule

        all_metadata_profiles = {}
        for module_type in self._kiara.available_module_types:

            cls = self._kiara.get_module_class(module_type=module_type)

            if issubclass(cls, ExtractMetadataModule):
                value_types = cls.get_supported_value_types()
                if "*" in value_types:
                    value_types = self._kiara._type_mgmt.value_type_names
                metadata_key = cls.get_metadata_key()

                for value_type in value_types:
                    if (
                        metadata_key
                        in all_metadata_profiles.setdefault(value_type, {}).keys()
                    ):
                        raise Exception(
                            f"Multiple profiles for type '{value_type}' and metadata key '{metadata_key}'. This is not allowed."
                        )  # yet, anyway

                    mc = {"type": value_type}
                    all_metadata_profiles[value_type][
                        metadata_key
                    ] = ExtractMetadataProfileConfig(
                        module_type=module_type, module_config=mc, value_type=value_type
                    )

            if hasattr(cls, "_extract_metadata_profiles"):
                profiles: typing.Mapping[str, typing.Mapping[str, typing.Any]] = cls._convert_profiles  # type: ignore
                for value_type, extract_details in profiles.items():
                    for metadata_key, module_config in extract_details.items():
                        if (
                            metadata_key
                            in all_metadata_profiles.setdefault(value_type, {}).keys()
                        ):
                            raise Exception(
                                f"Multiple profiles for type '{value_type}' and metadata key '{metadata_key}'. This is not allowed."
                            )  # yet, anyway
                        all_metadata_profiles[value_type][
                            metadata_key
                        ] = ExtractMetadataProfileConfig(
                            module_type=module_type,
                            module_config=module_config,
                            value_type=value_type,
                        )

        self._metadata_profiles = all_metadata_profiles
        return self._metadata_profiles

    @property
    def type_convert_profiles(
        self,
    ) -> typing.Mapping[str, typing.Mapping[str, TypeConversionProfileConfig]]:

        if self._convert_profiles is not None:
            return self._convert_profiles

        from kiara.modules.type_conversion import TypeConversionModule

        all_convert_profiles = {}
        for module_type in self._kiara.available_module_types:
            cls = self._kiara.get_module_class(module_type=module_type)

            if issubclass(cls, TypeConversionModule):
                source_types = cls.get_supported_source_types()
                if "*" in source_types:
                    source_types = self._kiara._type_mgmt.value_type_names
                target_types = cls.get_supported_target_types()

                for source_type in source_types:
                    for target_type in target_types:
                        if (
                            target_type
                            in all_convert_profiles.setdefault(source_type, {}).keys()
                        ):
                            raise Exception(
                                f"Multiple convert targets for '{source_type} -> {target_type}', this is not allowed."
                            )  # yet, anyway
                        mc = {"source_type": source_type, "target_type": target_type}
                        all_convert_profiles[source_type][
                            target_type
                        ] = TypeConversionProfileConfig(
                            module_type=module_type,
                            module_config=mc,
                            convert_source_type=source_type,
                            convert_target_type=target_type,
                        )

            if hasattr(cls, "_convert_profiles"):
                profiles: typing.Mapping[str, typing.Mapping[str, typing.Any]] = cls._convert_profiles  # type: ignore
                for source_type, module_config in profiles.items():
                    for target_type, details in module_config.items():
                        if (
                            target_type
                            in all_convert_profiles.setdefault(source_type, {}).keys()
                        ):
                            raise Exception(
                                f"Multiple convert targets for '{source_type} -> {target_type}', this is not allowed."
                            )  # yet, anyway
                        all_convert_profiles[source_type][
                            target_type
                        ] = TypeConversionProfileConfig(
                            module_type=module_type,
                            module_config=module_config,
                            convert_source_type=source_type,
                            convert_target_type=target_type,
                        )

        self._convert_profiles = all_convert_profiles
        return self._convert_profiles

    def get_type_conversion_module(
        self, source_type: str, target_type: str
    ) -> "KiaraModule":

        all_source_profiles = self.type_convert_profiles.get(source_type, None)
        if not all_source_profiles:
            raise Exception(
                f"No type conversion profiles for source type '{source_type}'."
            )

        convert_profile = all_source_profiles.get(target_type, None)
        if not convert_profile:
            raise Exception(
                f"No target conversion profile '{target_type}' for source type '{source_type}' available."
            )

        return convert_profile.create_module(self._kiara)
