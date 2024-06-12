# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import builtins
import inspect
from pydoc import locate
from typing import Any, Type, Union

from pydantic import BaseModel

from kiara.context import KiaraContextInfo
from kiara.utils.yaml import StringYAML

yaml = StringYAML()
# kiara_obj = Kiara.instance()


def define_env(env):
    """
    This is the hook for defining variables, macros and filters.

    - variables: the dictionary that contains the environment variables
    - macro: a decorator function, to declare a macro.
    """
    # env.variables["baz"] = "John Doe"

    @env.macro
    def get_schema_for_model(model_class: Union[str, Type[BaseModel]]):

        if isinstance(model_class, str):
            _class: Type[BaseModel] = locate(model_class)  # type: ignore
        else:
            _class = model_class

        schema_json = _class.schema_json(indent=2)

        return schema_json

    @env.macro
    def get_src_of_object(obj: Union[str, Any]):

        try:
            if isinstance(obj, str):
                _obj: Type[BaseModel] = locate(obj)  # type: ignore
            else:
                _obj = obj

            src = inspect.getsource(_obj)
            return src
        except Exception as e:
            return f"Can't render object source: {e}"

    @env.macro
    def get_context_info() -> KiaraContextInfo:

        return builtins.plugin_package_context_info  # type: ignore

    # @env.macro
    # def get_module_info(module_type: str):
    #
    #     try:
    #
    #         m_cls = Kiara.instance().module_registry.get_module_class(module_type)
    #         info = KiaraModuleTypeInfo.from_module_class(m_cls)
    #
    #         from rich.console import Console
    #
    #         console = Console(record=True)
    #         console.print(info)
    #
    #         html = console.export_text()
    #         return html
    #     except Exception as e:
    #         return f"Can't render module info: {str(e)}"
    #
    # @env.macro
    # def get_info_item_list_for_category(
    #     category: str, limit_to_package: typing.Optional[str] = None
    # ) -> typing.Dict[str, KiaraInfoModel]:
    #     return _get_info_item_list_for_category(
    #         category=category, limit_to_package=limit_to_package
    #     )
    #
    # def _get_info_item_list_for_category(
    #     category: str, limit_to_package: typing.Optional[str] = None
    # ) -> typing.Dict[str, KiaraInfoModel]:
    #
    #     infos = kiara_context.find_subcomponents(category=category)
    #
    #     if limit_to_package:
    #         temp = {}
    #         for n_id, obj in infos.items():
    #             if obj.context.labels.get("package", None) == limit_to_package:
    #                 temp[n_id] = obj
    #         infos = temp
    #
    #     docs = {}
    #     for n_id, obj in infos.items():
    #         docs[obj.get_id()] = obj.documentation.description
    #
    #     return docs
    #
    # @env.macro
    # def get_info_for_categories(
    #     *categories: str, limit_to_package: typing.Optional[str] = None
    # ):
    #
    #     TITLE_MAP = {
    #         "metadata.module": "Modules",
    #         "metadata.pipeline": "Pipelines",
    #         "metadata.type": "Value data_types",
    #         "metadata.operation_type": "Operation data_types",
    #     }
    #     result = {}
    #     for cat in categories:
    #         infos = _get_info_item_list_for_category(
    #             cat, limit_to_package=limit_to_package
    #         )
    #         if infos:
    #             result[cat] = {"items": infos, "title": TITLE_MAP[cat]}
    #
    #     return result
    #
    # @env.macro
    # def get_module_list_for_package(
    #     package_name: str,
    #     include_core_modules: bool = True,
    #     include_pipelines: bool = True,
    # ):
    #
    #     modules = kiara_obj.module_registry.find_modules_for_package(
    #         package_name,
    #         include_core_modules=include_core_modules,
    #         include_pipelines=include_pipelines,
    #     )
    #
    #     result = []
    #     for name, info in modules.items():
    #         type_md = info.get_type_metadata()
    #         result.append(
    #             f"[``{name}``][kiara_info.modules.{name}]: {type_md.documentation.description}"
    #         )
    #
    #     return result
    #
    # @env.macro
    # def get_data_types_for_package(package_name: str):
    #
    #     data_types = kiara_obj.type_registry.find_data_type_classes_for_package(
    #         package_name
    #     )
    #     result = []
    #     for name, info in data_types.items():
    #         type_md = info.get_type_metadata()
    #         result.append(f"  - ``{name}``: {type_md.documentation.description}")
    #
    #     return "\n".join(result)
    #
    # @env.macro
    # def get_metadata_models_for_package(package_name: str):
    #
    #     metadata_schemas = kiara_obj.metadata_mgmt.find_all_models_for_package(
    #         package_name
    #     )
    #     result = []
    #     for name, info in metadata_schemas.items():
    #         type_md = info.get_type_metadata()
    #         result.append(f"  - ``{name}``: {type_md.documentation.description}")
    #
    #     return "\n".join(result)
    #
    # @env.macro
    # def get_kiara_context() -> KiaraContext:
    #     return kiara_context


# def on_post_build(env):
#     "Post-build actions"
#
#     site_dir = env.conf["site_dir"]
#
#     for category, classes in KIARA_MODEL_CLASSES.items():
#
#         for cls in classes:
#             schema_json = cls.schema_json(indent=2)
#
#             file_path = os.path.join(
#                 site_dir, "development", "entities", category, f"{cls.__name__}.json"
#             )
#             os.makedirs(os.path.dirname(file_path), exist_ok=True)
#             with open(file_path, "w") as f:
#                 f.write(schema_json)
