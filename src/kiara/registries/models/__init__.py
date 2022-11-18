# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Dict, Type, Union

from kiara.interfaces.python_api.models.info import KiaraModelClassesInfo
from kiara.models import KiaraModel

if TYPE_CHECKING:
    pass


class ModelRegistry(object):

    _instance = None

    @classmethod
    def instance(cls) -> "ModelRegistry":
        """The default ModelRegistry instance.

        Can be a simgleton because it only contains data that is determined by the current Python environment.
        """

        if cls._instance is None:
            cls._instance = ModelRegistry()
        return cls._instance

    def __init__(self) -> None:

        self._all_models: Union[KiaraModelClassesInfo, None] = None
        self._models_per_package: Dict[str, KiaraModelClassesInfo] = {}
        self._sub_models: Dict[Type[KiaraModel], KiaraModelClassesInfo] = {}

    @property
    def all_models(self) -> KiaraModelClassesInfo:

        if self._all_models is not None:
            return self._all_models

        self._all_models = KiaraModelClassesInfo.find_kiara_models()
        return self._all_models

    def get_model_cls(
        self,
        kiara_model_id: str,
        required_subclass: Union[Type[KiaraModel], None] = None,
    ) -> Type[KiaraModel]:

        model_info = self.all_models.item_infos.get(kiara_model_id, None)
        if model_info is None:
            raise Exception(
                f"Can't retrieve model class for id '{kiara_model_id}': id not registered."
            )

        cls = model_info.python_class.get_class()  # type: ignore
        if required_subclass:
            if not issubclass(cls, required_subclass):
                raise Exception(
                    f"Can't retrieve sub model of '{required_subclass.__name__}' with id '{kiara_model_id}': exists, but not the required subclass."
                )

        return cls  # type: ignore

    def get_models_for_package(self, package_name: str) -> KiaraModelClassesInfo:

        if package_name in self._models_per_package.keys():
            return self._models_per_package[package_name]

        temp = {}
        for key, info in self.all_models.item_infos.items():
            if info.context.labels.get("package") == package_name:
                temp[key] = info

        group = KiaraModelClassesInfo.construct(
            group_alias=f"kiara_models.{package_name}", item_infos=temp  # type: ignore
        )

        self._models_per_package[package_name] = group
        return group

    def get_models_of_type(self, model_type: Type[KiaraModel]) -> KiaraModelClassesInfo:

        if model_type in self._sub_models.keys():
            return self._sub_models[model_type]

        sub_classes = {}
        for model_id, type_info in self.all_models.item_infos.items():
            cls: Type[KiaraModel] = type_info.python_class.get_class()  # type: ignore

            if issubclass(cls, model_type):
                sub_classes[model_id] = type_info

        classes = KiaraModelClassesInfo(
            group_title=f"{model_type.__name__}-submodels", item_infos=sub_classes
        )
        self._sub_models[model_type] = classes
        return classes
