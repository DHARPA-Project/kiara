# -*- coding: utf-8 -*-

"""Base module that holds [PipelineModule][kiara.pipeline.module.PipelineModule] classes that are auto-generated
from pipeline descriptions in the ``pipelines`` folder."""
import importlib
import typing

# TODO: add classloader for those classes to runtime
from types import ModuleType

from kiara.defaults import KIARA_MODULE_METADATA_ATTRIBUTE


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
        "__doc__": pmc.documentation,
        "_config_cls": PipelineModuleConfig,
        "_base_pipeline_config": pmc,
    }

    if pmc.context:
        md = dict(pmc.context)
    else:
        md = {}

    md.setdefault("tags", []).append("pipeline")
    md.setdefault("labels", {})["pipeline"] = "yes"
    attrs[KIARA_MODULE_METADATA_ATTRIBUTE] = md

    if not base_module:
        base_module = "kiara.modules.pipelines"
    elif base_module.endswith("pipelines"):
        # TODO: this special case needs documentation
        base_module = base_module.rsplit(".", maxsplit=1)[0]

    # TODO: investiage whether just ignoring non-existing modules is good enough
    _m: typing.Optional[ModuleType] = None
    _base_module: typing.Optional[str] = None
    if "." in type_name:
        intermediate_namespace_tokens = type_name.split(".")[0:-1]
        for token in intermediate_namespace_tokens:
            try:
                _temp = f"{base_module}.{token}"
                _m = importlib.import_module(_temp)
                _base_module = _temp
            except Exception:
                if _m is None:
                    _m = importlib.import_module(base_module)
                if _base_module is None:
                    _base_module = base_module
                break
    else:
        _m = importlib.import_module(base_module)
        _base_module = base_module

    if hasattr(_m, cls_name):
        raise Exception(
            f"Can't attach generated class '{cls_name}' to module '{_base_module}': module already has an attribute with that name."
        )
    attrs["__module__"] = _base_module

    cls = type(cls_name, (PipelineModule,), attrs)

    setattr(_m, cls_name, cls)

    return cls
