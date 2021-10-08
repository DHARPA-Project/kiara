# -*- coding: utf-8 -*-
import abc
import jupytext
import os
import typing
from jinja2 import Environment, FileSystemLoader, Template
from pathlib import Path

from kiara.defaults import KIARA_RESOURCES_FOLDER

if typing.TYPE_CHECKING:
    from kiara import Kiara, PipelineModule


class KiaraRenderer(abc.ABC):

    def __init__(self, config: typing.Optional[typing.Mapping[str, typing.Any]]=None, kiara: typing.Optional["Kiara"] = None):

        if kiara is None:
            from kiara import Kiara
            kiara = Kiara.instance()

        self._kiara: Kiara = kiara
        if config is None:
            config = {}
        self._config: typing.Mapping[str, typing.Any] = config

    def _augment_inputs(self, **inputs: typing.Any) -> typing.Mapping[str, typing.Any]:
        return inputs

    def _post_process(self, rendered: typing.Any, inputs: typing.Mapping[str, typing.Any]) -> typing.Any:
        return rendered

    def render(self, **inputs: typing.Mapping[str, typing.Any]):

        augmented_inputs = self._augment_inputs(**inputs)
        rendered = self._render_template(inputs=augmented_inputs)
        post_processed = self._post_process(rendered=rendered, inputs=augmented_inputs)
        return post_processed

    @abc.abstractmethod
    def _render_template(self, inputs: typing.Mapping[str, typing.Any]):
        pass


class JinjaRenderer(KiaraRenderer):


    def _render_template(self, inputs: typing.Mapping[str, typing.Any]):

        template = inputs.get("template", None)
        if not template:
            raise Exception("Can't render jinja template: no 'template' provided.")

        if isinstance(template, str):
            if not isinstance(template, Path):
                path = Path(template)

            loader = FileSystemLoader(path.parent)
            env: Environment = Environment(loader=loader)

            _template = env.get_template(path.name)
        elif isinstance(template, Template):
            _template = template
        else:
            raise TypeError(f"Invalid type for template: {type(template)}")

        rendered = _template.render(**inputs)

        return rendered


class JinjaPipelineRenderer(JinjaRenderer):

    def _augment_inputs(self, **inputs: typing.Any) -> typing.Mapping[str, typing.Any]:

        pipeline_input = inputs.get("inputs", None)
        module = inputs.get("module")
        module_config = inputs.get("module_config", None)

        module_obj: "PipelineModule" = self._kiara.create_module(
            module_type=module, module_config=module_config
        )

        if not module_obj.is_pipeline():
            raise Exception("Only pipeline modules supported (for now).")

        step_inputs: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
        # for k, v in pipeline_input.items():
        #     pi = module_obj.structure.pipeline_inputs.get(k)
        #     assert pi
        #     if len(pi.connected_inputs) != 1:
        #         raise NotImplementedError()
        #
        #     ci = pi.connected_inputs[0]
        #     if isinstance(v, str):
        #         v = f'"{v}"'
        #     step_inputs.setdefault(ci.step_id, {})[ci.value_name] = v

        result = {"structure": module_obj.structure, "input_values": step_inputs}
        if "template" in inputs.keys():
            template = inputs["template"]
        else:
            template = "notebook"

        if template in ["notebook", "python-script"]:
            template = os.path.join(KIARA_RESOURCES_FOLDER, "templates", f"{template}.j2")

        result["template"] = template
        return result

    def _post_process(self, rendered: typing.Any, inputs: typing.Mapping[str, typing.Any]):

        template: str = inputs["template"]
        if template.endswith("notebook.j2"):
            notebook = jupytext.reads(rendered, fmt="py:percent")
            converted = jupytext.writes(notebook, fmt="notebook")
            return converted
        elif template.endswith("python-script.j2"):
            import black
            from black import Mode
            cleaned = black.format_str(rendered, mode=Mode())
            return cleaned

        return rendered
