# -*- coding: utf-8 -*-
import typing
from jinja2 import Environment, FileSystemLoader, Template
from pathlib import Path

from kiara import PipelineStructure


class PipelineRenderer(object):
    def __init__(self, structure: PipelineStructure):

        self._structure: PipelineStructure = structure

    def render_from_path(
        self, path: typing.Union[str, Path], inputs: typing.Mapping[str, typing.Any]
    ):

        if not isinstance(path, Path):
            path = Path(path)

        loader = FileSystemLoader(path.parent)
        env = Environment(loader=loader)

        template = env.get_template(path.name)
        return self.render(template, inputs=inputs)

    def render(self, template: Template, inputs: typing.Mapping[str, typing.Any]):

        rendered = template.render(structure=self._structure, input_values=inputs)
        return rendered
