# -*- coding: utf-8 -*-
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Mapping,
    Type,
    Union,
)

from jinja2 import Template

from kiara.models.module.pipeline.pipeline import Pipeline
from kiara.renderers import (
    RenderInputsSchema,
    SourceTransformer,
)
from kiara.renderers.jinja import BaseJinjaRenderer, JinjaEnv

if TYPE_CHECKING:
    from kiara.api import JobDesc
    from kiara.context import Kiara


class JobDescTransformer(SourceTransformer):
    def __init__(self, kiara: "Kiara"):
        self._kiara: "Kiara" = kiara
        super().__init__()

    def retrieve_supported_python_classes(self) -> Iterable[Type]:
        from kiara.api import JobDesc

        return [JobDesc, str, Mapping, Path]

    def retrieve_supported_inputs_descs(self) -> Union[str, Iterable[str]]:
        return [
            "a job description instance",
            "a path to a a job description file",
            "the job description as a Python dict",
        ]

    def validate_and_transform(self, source: Any) -> Union["JobDesc", None]:
        from kiara.api import JobDesc

        if isinstance(source, JobDesc):
            return source
        elif isinstance(source, (Path, str)):
            return JobDesc.create_from_file(source)
        elif isinstance(source, Mapping):
            return JobDesc.create_from_data(data=source)
        else:
            return None


class JobDescPythonScriptRenderer(BaseJinjaRenderer["JobDesc", RenderInputsSchema]):
    """Renders a simple executable python script from a job description.

    ## Examples

    ### Terminal

    Example invoication from the command line (using [this](https://github.com/DHARPA-Project/kiara_plugin.tabular/blob/develop/examples/jobs/init.yaml) job description):

    ```
    kiara render --source-type job_desc --target-type python_script item init.yaml
    python tables_from_csv_files.py
    ```

    ### Python API

    Example usage from the Python API:

    ``` python
    from kiara.api import KiaraAPI

    kiara = KiaraAPI.instance()

    job_desc = "<path_to_job_desc>.yaml"

    rendered = kiara.render(job_desc, source_type="job_desc", target_type="python_script")
    print(f"# Rendered python script for job '{job_desc}':")
    print(rendered)
    ```

    """

    _renderer_name = "job_to_python_script"
    _inputs_schema = RenderInputsSchema

    def retrieve_supported_render_sources(self) -> str:
        return "job_desc"

    def retrieve_supported_render_targets(cls) -> Union[Iterable[str], str]:
        return "python_script"

    def retrieve_source_transformers(self) -> Iterable[SourceTransformer]:
        return [JobDescTransformer(kiara=self._kiara)]

    def retrieve_jinja_env(self) -> JinjaEnv:

        jinja_env = JinjaEnv(template_base="kiara")
        return jinja_env

    def get_template(self, render_config: RenderInputsSchema) -> Template:

        return self.get_jinja_env().get_template("pipeline/python_script.py.j2")

    def assemble_render_inputs(
        self, instance: Any, render_config: RenderInputsSchema
    ) -> Mapping[str, Any]:

        from kiara.utils.rendering import create_pipeline_render_inputs

        job_desc: JobDesc = instance

        pipeline = Pipeline.create_pipeline(
            kiara=self._kiara, pipeline=job_desc.operation
        )

        result = create_pipeline_render_inputs(pipeline, job_desc.inputs)
        return result
