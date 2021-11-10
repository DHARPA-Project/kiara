# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import typing
from rich import box
from rich.console import RenderableType
from rich.table import Table

from kiara.info import KiaraInfoModel
from kiara.metadata.module_models import KiaraModuleTypeMetadata

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara
    from kiara.module import KiaraModule


# class ModuleInfo(KiaraInfoModel):
#     """A simple model class to hold and display information about a module.
#
#     This is not used in processing at all, it is really only there to make it easier to communicate module characteristics..
#     """
#
#     @classmethod
#     def from_module_cls(cls, module_cls: typing.Type["KiaraModule"]):
#
#         return ModuleInfo(
#             metadata=KiaraModuleTypeMetadata.from_module_class(module_cls=module_cls)
#         )
#
#     class Config:
#         extra = Extra.forbid
#         allow_mutation = False
#
#     metadata: KiaraModuleTypeMetadata = Field(description="The metadata of the module.")
#
#     def create_renderable(self, **config: typing.Any) -> RenderableType:
#
#         table = self.metadata.create_renderable()
#         return table


class ModuleTypesGroupInfo(KiaraInfoModel):

    __root__: typing.Dict[str, KiaraModuleTypeMetadata]

    @classmethod
    def from_type_names(
        cls,
        kiara: "Kiara",
        type_names: typing.Optional[typing.Iterable[str]] = None,
        ignore_pipeline_modules: bool = False,
        ignore_non_pipeline_modules: bool = False,
        **config: typing.Any
    ):

        if ignore_pipeline_modules and ignore_non_pipeline_modules:
            raise Exception("Can't ignore both pipeline and non-pipeline modules.")

        if type_names is None:
            type_names = kiara.available_module_types

        classes = {}
        for tn in type_names:
            _cls = kiara.get_module_class(tn)
            if ignore_pipeline_modules and _cls.is_pipeline():
                continue
            if ignore_non_pipeline_modules and not _cls.is_pipeline():
                continue
            classes[tn] = KiaraModuleTypeMetadata.from_module_class(_cls)

        return ModuleTypesGroupInfo(__root__=classes)

    @classmethod
    def create_renderable_from_type_names(
        cls,
        kiara: "Kiara",
        type_names: typing.Iterable[str],
        ignore_pipeline_modules: bool = False,
        ignore_non_pipeline_modules: bool = False,
        **config: typing.Any
    ):

        classes = {}
        for tn in type_names:
            _cls = kiara.get_module_class(tn)
            classes[tn] = _cls

        return cls.create_renderable_from_module_type_map(
            module_types=classes,
            ignore_pipeline_modules=ignore_pipeline_modules,
            ignore_non_pipeline_modules=ignore_non_pipeline_modules,
            **config
        )

    @classmethod
    def create_renderable_from_module_type_map(
        cls,
        module_types: typing.Mapping[str, typing.Type["KiaraModule"]],
        ignore_pipeline_modules: bool = False,
        ignore_non_pipeline_modules: bool = False,
        **config: typing.Any
    ):
        """Create a renderable from a map of module classes.

        Render-configuration options:
          - include_full_doc (default: False): include the full documentation, instead of just a one line description
        """

        return cls.create_renderable_from_module_info_map(
            {
                k: KiaraModuleTypeMetadata.from_module_class(v)
                for k, v in module_types.items()
            },
            ignore_pipeline_modules=ignore_pipeline_modules,
            ignore_non_pipeline_modules=ignore_non_pipeline_modules,
            **config
        )

    @classmethod
    def create_renderable_from_module_info_map(
        cls,
        module_types: typing.Mapping[str, KiaraModuleTypeMetadata],
        ignore_pipeline_modules: bool = False,
        ignore_non_pipeline_modules: bool = False,
        **config: typing.Any
    ):
        """Create a renderable from a map of module info wrappers.

        Render-configuration options:
          - include_full_doc (default: False): include the full documentation, instead of just a one line description
        """

        if ignore_pipeline_modules and ignore_non_pipeline_modules:
            raise Exception("Can't ignore both pipeline and non-pipeline modules.")

        if ignore_pipeline_modules:
            module_types = {k: v for k, v in module_types.items() if not v.is_pipeline}
        elif ignore_non_pipeline_modules:
            module_types = {k: v for k, v in module_types.items() if v.is_pipeline}

        show_lines = False
        table = Table(show_header=False, box=box.SIMPLE, show_lines=show_lines)
        table.add_column("name", style="b")
        table.add_column("desc", style="i")

        for name, details in module_types.items():

            if config.get("include_full_doc", False):
                table.add_row(name, details.documentation.full_doc)
            else:
                table.add_row(name, details.documentation.description)

        return table

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        return ModuleTypesGroupInfo.create_renderable_from_module_info_map(
            self.__root__
        )
