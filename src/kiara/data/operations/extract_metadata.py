# -*- coding: utf-8 -*-
import typing

from kiara import Kiara
from kiara.data.operations import OperationType


class ExtractMetadataOperationType(OperationType):
    """Extract metadata from a dataset."""

    @classmethod
    def retrieve_operation_configs(
        cls, kiara: Kiara
    ) -> typing.Mapping[
        str, typing.Mapping[str, typing.Mapping[str, typing.Mapping[str, typing.Any]]]
    ]:

        all_metadata_profiles: typing.Dict[
            str, typing.Dict[str, typing.Dict[str, typing.Any]]
        ] = {}

        from kiara.modules.metadata import ExtractMetadataModule

        # find all KiaraModule subclasses that are relevant for this profile type
        for module_type in kiara.available_module_types:

            m_cls = kiara.get_module_class(module_type=module_type)

            if issubclass(m_cls, ExtractMetadataModule):
                value_types: typing.Iterable[str] = m_cls.get_supported_value_types()

                if "*" in value_types:
                    value_types = kiara.type_mgmt.value_type_names
                metadata_key = m_cls.get_metadata_key()

                for value_type in value_types:

                    mc = {"value_type": value_type}
                    profile_config = {
                        "module_type": module_type,
                        "module_config": mc,
                        # "value_type": value_type,
                    }
                    all_metadata_profiles.setdefault(value_type, {}).setdefault(
                        "extract_metadata", {}
                    )[metadata_key] = profile_config
                    # TODO: validate here?

        return all_metadata_profiles

    # value_type: str = Field(
    #     description="The type of the value to extract metadata from."
    # )
