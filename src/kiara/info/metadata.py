# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import typing
from rich import box
from rich.console import RenderableType
from rich.table import Table

from kiara import Kiara
from kiara.info import KiaraInfoModel
from kiara.metadata.core_models import MetadataModelMetadata


class MetadataModelsInfo(KiaraInfoModel):

    __root__: typing.Dict[str, MetadataModelMetadata]

    @classmethod
    def from_metadata_keys(cls, *metadata_keys: str, kiara: typing.Optional[Kiara]):

        if kiara is None:
            kiara = Kiara.instance()

        models = {}
        invalid = []
        for metadata_key in metadata_keys:
            model = kiara.metadata_mgmt.all_schemas.get(metadata_key, None)
            if model is None:
                invalid.append(metadata_key)
            else:
                models[metadata_key] = MetadataModelMetadata.from_model_class(
                    kiara.metadata_mgmt.all_schemas[metadata_key]
                )

        if invalid:
            raise Exception(
                f"Can't create metadata information, one or several metadata keys are not registered: {', '.join(invalid)}"
            )

        return MetadataModelsInfo(__root__=models)

    def create_renderable(self, **config: typing.Any) -> RenderableType:

        table = Table(show_header=True, box=box.SIMPLE)
        table.add_column("Metadata key", style="i")
        table.add_column("Description")

        for key, model in self.__root__.items():
            table.add_row(key, model.documentation.description)

        return table
