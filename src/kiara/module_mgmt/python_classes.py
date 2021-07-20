# -*- coding: utf-8 -*-
import typing
from pydantic import BaseModel, Field, validator

from kiara.module_mgmt import ModuleManager

if typing.TYPE_CHECKING:
    from kiara.module import KiaraModule


try:
    from typing import Literal
except Exception:
    from typing_extensions import Literal  # type: ignore


class PythonModuleManagerConfig(BaseModel):

    module_manager_type: Literal["python"]
    module_classes: typing.Dict[str, typing.Type["KiaraModule"]] = Field(
        description="The module classes this manager should hold."
    )

    @validator("module_classes", pre=True)
    def _ensure_module_class_types(cls, v):

        _classes = []
        if v:
            for _cls in v:
                if isinstance(_cls, str):
                    try:
                        module_name, cls_name = _cls.rsplit(".", maxsplit=1)
                        module = __import__(module_name)
                        _cls = getattr(module, cls_name)
                    except Exception:
                        raise ValueError(
                            f"Can't parse value '{_cls}' into KiaraModule class."
                        )

                from kiara import KiaraModule  # noqa

                if not issubclass(_cls, KiaraModule):
                    raise ValueError(f"Not a KiaraModule sub-class: {_cls}")
                _classes.append(_cls)

        return _classes


class PythonModuleManager(ModuleManager):
    def __init__(
        self,
        module_classes: typing.Optional[
            typing.Mapping[str, typing.Type["KiaraModule"]]
        ] = None,
    ):

        if not module_classes:
            from kiara.utils.class_loading import find_all_kiara_modules

            module_classes = find_all_kiara_modules()

        self._module_classes: typing.Mapping[str, typing.Type[KiaraModule]] = {}

        # TODO: investigate why streamlit reloads this several times
        for k, v in module_classes.items():
            if not v.is_pipeline():
                self._module_classes[k] = v

    def get_module_class(self, module_type: str) -> typing.Type["KiaraModule"]:

        cls = self._module_classes.get(module_type, None)
        if cls is None:
            raise ValueError(f"No module of type '{module_type}' available.")
        return cls

    def get_module_types(self) -> typing.Iterable[str]:
        return self._module_classes.keys()
