# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import inspect
import os
import typing
from pydantic import BaseModel
from pydoc import locate

from kiara import Kiara
from kiara.data.values import Value, ValueSchema
from kiara.info.kiara import KiaraContext
from kiara.info.pipelines import PipelineState, PipelineStructureDesc
from kiara.metadata.module_models import KiaraModuleTypeMetadata
from kiara.module_config import ModuleConfig, ModuleTypeConfigSchema
from kiara.module_mgmt.pipelines import PipelineModuleManager
from kiara.pipeline import PipelineValueInfo, PipelineValuesInfo, StepValueAddress
from kiara.pipeline.config import PipelineConfig
from kiara.pipeline.pipeline import (
    PipelineInputEvent,
    PipelineOutputEvent,
    PipelineStep,
    StepInputEvent,
    StepOutputEvent,
)
from kiara.pipeline.values import (
    PipelineInputRef,
    PipelineOutputRef,
    StepInputRef,
    StepOutputRef,
)
from kiara.utils import StringYAML

KIARA_MODEL_CLASSES: typing.Mapping[str, typing.List[typing.Type[BaseModel]]] = {
    "values": [
        ValueSchema,
        Value,
        PipelineValueInfo,
        PipelineValuesInfo,
        StepValueAddress,
        StepInputRef,
        StepOutputRef,
        PipelineInputRef,
        PipelineOutputRef,
    ],
    "modules": [
        ModuleTypeConfigSchema,
        PipelineConfig,
        PipelineStep,
        PipelineStructureDesc,
        PipelineState,
        ModuleConfig,
    ],
    "events": [
        StepInputEvent,
        StepOutputEvent,
        PipelineInputEvent,
        PipelineOutputEvent,
    ],
}


yaml = StringYAML()
kiara_obj = Kiara.instance()
kiara_context = KiaraContext.create(kiara=kiara_obj)


def define_env(env):
    """
    This is the hook for defining variables, macros and filters

    - variables: the dictionary that contains the environment variables
    - macro: a decorator function, to declare a macro.
    """

    # env.variables["baz"] = "John Doe"

    @env.macro
    def get_schema_for_model(model_class: typing.Union[str, typing.Type[BaseModel]]):

        if isinstance(model_class, str):
            _class: typing.Type[BaseModel] = locate(model_class)  # type: ignore
        else:
            _class = model_class

        schema_json = _class.schema_json(indent=2)

        return schema_json

    @env.macro
    def get_src_of_object(obj: typing.Union[str, typing.Any]):

        try:
            if isinstance(obj, str):
                _obj: typing.Type[BaseModel] = locate(obj)  # type: ignore
            else:
                _obj = obj

            src = inspect.getsource(_obj)
            return src
        except Exception as e:
            return f"Can't render object source: {str(e)}"

    @env.macro
    def get_pipeline_config(pipeline_name: str):

        pmm = PipelineModuleManager()
        desc = pmm.pipeline_descs[pipeline_name]["data"]

        desc_str = yaml.dump(desc)
        return desc_str

    @env.macro
    def get_module_info(module_type: str):

        try:

            m_cls = Kiara.instance().get_module_class(module_type)
            info = KiaraModuleTypeMetadata.from_module_class(m_cls)

            from rich.console import Console

            console = Console(record=True)
            console.print(info)

            html = console.export_text()
            return html
        except Exception as e:
            return f"Can't render module info: {str(e)}"

    @env.macro
    def get_module_list_for_package(
        package_name: str,
        include_core_modules: bool = True,
        include_pipelines: bool = True,
    ):

        modules = kiara_obj.module_mgmt.find_modules_for_package(
            package_name,
            include_core_modules=include_core_modules,
            include_pipelines=include_pipelines,
        )

        result = []
        for name, info in modules.items():
            type_md = info.get_type_metadata()
            result.append(
                f"  - [``{name}``]({type_md.context.get_url_for_reference('module_doc')}): {type_md.documentation.description}"
            )

        print(result)
        return "\n".join(result)

    @env.macro
    def get_value_types_for_package(package_name: str):

        value_types = kiara_obj.type_mgmt.find_value_types_for_package(package_name)
        result = []
        for name, info in value_types.items():
            type_md = info.get_type_metadata()
            result.append(f"  - ``{name}``: {type_md.documentation.description}")

        return "\n".join(result)

    @env.macro
    def get_metadata_schemas_for_package(package_name: str):

        metadata_schemas = kiara_obj.metadata_mgmt.find_all_schemas_for_package(
            package_name
        )
        result = []
        for name, info in metadata_schemas.items():
            type_md = info.get_type_metadata()
            result.append(f"  - ``{name}``: {type_md.documentation.description}")

        return "\n".join(result)

    @env.macro
    def get_kiara_context() -> KiaraContext:
        return kiara_context


def on_post_build(env):
    "Post-build actions"

    site_dir = env.conf["site_dir"]

    for category, classes in KIARA_MODEL_CLASSES.items():

        for cls in classes:
            schema_json = cls.schema_json(indent=2)

            file_path = os.path.join(
                site_dir, "development", "entities", category, f"{cls.__name__}.json"
            )
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w") as f:
                f.write(schema_json)
