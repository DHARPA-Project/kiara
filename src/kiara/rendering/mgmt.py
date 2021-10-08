# -*- coding: utf-8 -*-
import typing

from kiara.rendering import KiaraRenderer, JinjaPipelineRenderer

if typing.TYPE_CHECKING:
    from kiara import Kiara

DEFAULT_TEMPLATES = {
    "pipeline": "pipeline"

}

class TemplateRenderingMgmt(object):

    @classmethod
    def create(cls, templates: typing.Optional[typing.Mapping[str, typing.Mapping[str, typing.Any]]]=None, kiara: typing.Optional["Kiara"]=None) -> "TemplateRenderingMgmt":

        if not templates:
            templates = DEFAULT_TEMPLATES

        _templates: typing.Dict[str, typing.Dict[typing.Any]] = {}
        for template_name, config in templates.items():
            if isinstance(config, typing.Type) and issubclass(config, KiaraRenderer):
                _templates[template_name] = {
                    "renderer_cls": config,
                    "renderer_config": {}
                }
            elif isinstance(config, str):
                if config == "pipeline":
                    _templates[template_name] = {
                        "renderer_cls": JinjaPipelineRenderer,
                        "renderer_config": {}
                    }
                else:
                    raise NotImplementedError()
            else:
                raise NotImplementedError()

        return TemplateRenderingMgmt(templates=_templates, kiara=kiara)

    def __init__(self, templates: typing.Mapping[str, KiaraRenderer], kiara: typing.Optional["Kiara"]=None):

        if kiara is None:
            from kiara import Kiara
            kiara = Kiara.instance()

        self._kiara: Kiara = kiara
        self._templates: typing.Mapping[str, KiaraRenderer] = templates

    def render(self, template_name: str, **inputs: typing.Any) -> typing.Any:

        if template_name not in self._templates.keys():
            raise Exception(f"No template '{template_name}' registered. Available templates: {', '.join(self._templates.keys())}")

        template_cls = self._templates[template_name]["renderer_cls"]
        template_config = self._templates[template_name]["renderer_config"]

        template_renderer: KiaraRenderer = template_cls(config=template_config, kiara=self._kiara)

        return template_renderer.render(**inputs)






