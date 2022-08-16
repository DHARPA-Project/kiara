# -*- coding: utf-8 -*-
# import abc
# from typing import Union, Iterable, Mapping, Dict, Any, TYPE_CHECKING
#
# from pydantic import Extra, Field, BaseModel
#
# from kiara.models import KiaraModel
#
# if TYPE_CHECKING:
#     from kiara.models.values.value import Value
#
#
# class RenderScene(KiaraModel):
#     class Config:
#         extra = Extra.ignore
#
#     parent_scene: Union[None, "RenderScene"] = Field(
#         description="The parent of this scene (if applicable).", default=None
#     )
#
#     @classmethod
#     @abc.abstractmethod
#     def retrieve_source_type(cls) -> str:
#         pass
#
#     @classmethod
#     def retrieve_supported_target_types(cls) -> Iterable[str]:
#
#         result = []
#         for attr in dir(cls):
#             if len(attr) <= 11 or not attr.startswith("render_as__"):
#                 continue
#
#             attr = attr[11:]
#             target_type = attr[0:]
#             result.append(target_type)
#
#         return result
#
#     scene_name: Union[None, str] = Field(
#         description="The name of the current scene to be rendered.", default=None
#     )
#     scenes: Union[None, Mapping[str, "RenderScene"]] = Field(
#         description="Child scenes, in case this itself is not going to be rendered.",
#         default=None,
#     )
#
#     def get_render_parameters(self) -> Dict[str, Any]:
#         if self.scene_name:
#             if not self.scenes or self.scene_name not in self.scenes.keys():
#                 if self.scenes:
#                     _msg = "Available names: " + ", ".join(self.scenes.keys())
#                 else:
#                     _msg = "No child scenes available"
#
#                 raise Exception(
#                     f"Can't render scene, no scene named '{self.scene_name}' available. {_msg}."
#                 )
#             else:
#                 return self.scenes[self.scene_name].get_render_parameters()
#         else:
#             return self.dict(exclude={"scene_name", "scenes"})
#
#
# class RenderMetadata(KiaraModel):
#
#     this_scene: RenderScene = Field(
#         description="The render instruction for the current scene."
#     )
#     related_scenes: Dict[str, Union[RenderScene, None]] = Field(
#         description="Related instructions, to be used by implementing frontends as hints.",
#         default_factory=dict,
#     )
#
#
# class RenderValueResult(BaseModel):
#
#     rendered: Any
#     metadata: RenderMetadata
#
#
# class RenderAnyValueScene(RenderScene):
#     @classmethod
#     def retrieve_source_type(cls) -> str:
#         return "any"
#
#     def render_as__terminal_renderable(self, value: "Value"):
#         render_config = {
#             "show_pedigree": False,
#             "show_serialized": False,
#             "show_data_preview": False,
#             "show_properties": True,
#             "show_destinies": True,
#             "show_destiny_backlinks": True,
#             "show_lineage": True,
#             "show_environment_hashes": False,
#             "show_environment_data": False,
#         }
#         value_info = value.create_info()
#         rend = value_info.create_renderable(**render_config)
#         return RenderValueResult(
#             rendered=rend, metadata=RenderMetadata(this_scene=self)
#         )
#
#     # def render_as__string(self, value: "Value"):
#     #     return RenderValueResult(rendered="xxx", metadata=RenderMetadata(this_scene=self))
