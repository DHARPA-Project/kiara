# -*- coding: utf-8 -*-
import inspect
import os
from pydantic import BaseModel, typing
from pydoc import locate

from kiara import Kiara
from kiara.data.values import (
    PipelineInputField,
    PipelineOutputField,
    PipelineValue,
    PipelineValues,
    StepInputField,
    StepOutputField,
    StepValueAddress,
    Value,
    ValueSchema,
)
from kiara.module import ModuleInfo
from kiara.module_config import (
    KiaraModuleConfig,
    KiaraWorkflowConfig,
    PipelineModuleConfig,
)
from kiara.module_mgmt.pipelines import PipelineModuleManager
from kiara.pipeline.pipeline import (
    PipelineInputEvent,
    PipelineOutputEvent,
    PipelineState,
    PipelineStep,
    PipelineStructureDesc,
    StepInputEvent,
    StepOutputEvent,
)
from kiara.utils import StringYAML

KIARA_MODEL_CLASSES: typing.Mapping[str, typing.List[typing.Type[BaseModel]]] = {
    "values": [
        ValueSchema,
        Value,
        PipelineValue,
        PipelineValues,
        StepValueAddress,
        StepInputField,
        StepOutputField,
        PipelineInputField,
        PipelineOutputField,
    ],
    "modules": [
        KiaraModuleConfig,
        PipelineModuleConfig,
        PipelineStep,
        PipelineStructureDesc,
        PipelineState,
        KiaraWorkflowConfig,
    ],
    "events": [
        StepInputEvent,
        StepOutputEvent,
        PipelineInputEvent,
        PipelineOutputEvent,
    ],
}


yaml = StringYAML()


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
            if module_type == "pipeline" or not m_cls.is_pipeline():
                info = ModuleInfo(module_type=module_type)
            else:
                info = ModuleInfo(module_type=module_type)

            from rich.console import Console

            console = Console(record=True)
            console.print(info)

            html = console.export_text()
            return html
        except Exception as e:
            return f"Can't render module info: {str(e)}"


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
