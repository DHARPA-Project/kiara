# -*- coding: utf-8 -*-

"""Base module that holds [PipelineModule][kiara.pipeline.module.PipelineModule] classes that are auto-generated
from pipeline descriptions in the ``pipelines`` folder."""
import importlib
import typing

# TODO: add classloader for those classes to runtime


def create_pipeline_class(
    cls_name: str,
    type_name: str,
    pipeline_desc: typing.Mapping[str, typing.Any],
    base_module: str,
):

    from kiara.module_config import PipelineModuleConfig
    from kiara.pipeline.module import PipelineModule

    pmc = PipelineModuleConfig(**pipeline_desc)

    def init(self, id: str, **kwargs):
        # TODO: merge config
        if kwargs.get("module_config", None):
            raise Exception(
                f"Can't dynamically create PipelineModuleClass, 'module_config' provided externally: {pipeline_desc}"
            )
        kwargs["module_config"] = pipeline_desc
        super(self.__class__, self).__init__(id=id, **kwargs)

    attrs = {
        "__init__": init,
        "_config_cls": PipelineModuleConfig,
        "_base_pipeline_config": pmc,
    }
    # TODO: add pydoc

    if not base_module:
        base_module = "kiara.modules.pipelines"

    _module = importlib.import_module(base_module)
    if hasattr(_module, cls_name):
        raise Exception(
            f"Can't attach generated class '{cls_name}' to module '{base_module}': module already has an attribute with that name."
        )
    attrs["__module__"] = base_module

    cls = type(cls_name, (PipelineModule,), attrs)

    setattr(_module, cls_name, cls)

    return cls
