# -*- coding: utf-8 -*-

#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


# class KiaraDataTypesRuntimeEnvironment(RuntimeEnvironment):
#
#     _kiara_model_id: ClassVar = "info.runtime.kiara_data_types"
#
#     environment_type: Literal["kiara_data_types"]
#     data_types: DataTypeClassesInfo = Field(
#         description="The available data types and their metadata."
#     )
#
#     @classmethod
#     def retrieve_environment_data(cls) -> Dict[str, Any]:
#
#         from kiara.api import KiaraAPI
#
#         kiara_api = KiaraAPI.instance()
#
#         data_types_infos: DataTypeClassesInfo = kiara_api.retrieve_data_types_info()
#         data_types = data_types_infos.model_dump()
#
#         return {"data_types": data_types}


# class KiaraTypesRuntimeEnvironment(RuntimeEnvironment):
#
#     _kiara_model_id: ClassVar = "info.runtime.kiara_types"
#
#     environment_type: Literal["kiara_types"]
#     archive_types: ArchiveTypeClassesInfo = Field(
#         description="The available implemented store types."
#     )
#     metadata_types: MetadataTypeClassesInfo = Field(
#         description="The available metadata types."
#     )
#
#     @classmethod
#     def retrieve_environment_data(cls) -> Dict[str, Any]:
#
#         from kiara.utils.archives import find_archive_types
#
#         result: Dict[str, Any] = {}
#         result["metadata_types"] = find_metadata_models()
#         result["archive_types"] = find_archive_types()
#
#         return result
