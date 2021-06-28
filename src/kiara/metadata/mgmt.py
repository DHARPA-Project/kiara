# -*- coding: utf-8 -*-
import typing

from kiara.metadata import MetadataModel
from kiara.utils.class_loading import find_all_metadata_schemas

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara


class MetadataMgmt(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: "Kiara" = kiara
        self._metadata_models: typing.Optional[
            typing.Dict[str, typing.Type[MetadataModel]]
        ] = None

    @property
    def all_schemas(self) -> typing.Mapping[str, typing.Type[MetadataModel]]:

        if self._metadata_models is None:
            self._metadata_models = find_all_metadata_schemas()
        return self._metadata_models

    def find_all_schemas_for_package(
        self, package_name: str
    ) -> typing.Dict[str, typing.Type[MetadataModel]]:

        result = {}
        for name, schema in self.all_schemas.items():
            schema_md = schema.get_model_cls_metadata()
            package = schema_md.context.labels.get("package")
            if package == package_name:
                result[name] = schema

        return result
