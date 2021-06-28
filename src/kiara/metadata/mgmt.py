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
