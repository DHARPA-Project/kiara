# -*- coding: utf-8 -*-
import abc
import re
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Mapping,
    Union,
)

from pydantic.fields import Field

from kiara.interfaces.python_api.base_api import BaseAPI, find_base_api_endpoints
from kiara.interfaces.python_api.proxy import ApiEndpoints
from kiara.models.rendering import RenderValueResult
from kiara.renderers import (
    KiaraRenderer,
    KiaraRendererConfig,
    RenderInputsSchema,
)
from kiara.utils.cli import terminal_print
from kiara.utils.introspection import (
    create_signature_string,
    extract_arg_names,
    extract_proxy_arg_str,
)
from kiara.utils.output import (
    create_table_from_base_model_v1_cls,
)

if TYPE_CHECKING:
    from kiara.context import Kiara


class ApiRenderInputsSchema(RenderInputsSchema):

    filter: Union[str, Iterable[str]] = Field(
        description="One or a list of filter tokens -- if provided -- all of which must match for the api endpoing to be in the render result.",
        default_factory=list,
    )


class ApiRendererConfig(KiaraRendererConfig):

    pass
    # target_type: str = Field(description="The target type to render the api as.")


class ApiRenderer(
    KiaraRenderer[BaseAPI, ApiRenderInputsSchema, RenderValueResult, ApiRendererConfig]
):
    _inputs_schema = ApiRenderInputsSchema
    _renderer_config_cls = ApiRendererConfig

    def __init__(
        self,
        kiara: "Kiara",
        renderer_config: Union[None, Mapping[str, Any], KiaraRendererConfig] = None,
    ):

        self._api_endpoints: ApiEndpoints = ApiEndpoints(api_cls=BaseAPI)
        super().__init__(kiara=kiara, renderer_config=renderer_config)

    @property
    def api_endpoints(self) -> ApiEndpoints:
        return self._api_endpoints

    def get_renderer_alias(self) -> str:
        return f"api_to_{self.get_target_type()}"

    def retrieve_supported_render_sources(self) -> str:
        return "base_api"

    def retrieve_doc(self) -> Union[str, None]:

        return f"Render the kiara base API to: '{self.get_target_type()}'."

    @abc.abstractmethod
    def get_target_type(self) -> str:
        pass


class BaseApiDocRenderer(ApiRenderer):

    _renderer_name = "base_api_doc_renderer"

    def get_target_type(self) -> str:
        return "html"

    def retrieve_supported_render_targets(self) -> Union[Iterable[str], str]:
        return "html"

    def _render(self, instance: BaseAPI, render_config: ApiRenderInputsSchema) -> Any:

        # details = self.api_endpoints.get_api_endpoint("get_value")
        details = self.api_endpoints.get_api_endpoint("retrieve_aliases_info")

        # for k, v in details.arg_schema.items():
        #     print(k)
        #     print(type(v))

        terminal_print(create_table_from_base_model_v1_cls(details.arg_model))

        return "xxx"


class BaseApiDocTextRenderer(ApiRenderer):

    _renderer_name = "base_api_doc_markdown_renderer"

    def get_target_type(self) -> str:
        return "markdown"

    def retrieve_supported_render_targets(self) -> Union[Iterable[str], str]:
        return "markdown"

    def _render(self, instance: BaseAPI, render_config: ApiRenderInputsSchema) -> Any:

        template = self._kiara.render_registry.get_template(
            "kiara_api/api_doc.md.j2", "kiara"
        )

        result = ""
        for ep in self.api_endpoints.api_endpint_names:
            doc = self.api_endpoints.get_api_endpoint(ep).doc
            rendered = template.render(endpoint_name=ep, doc=doc)
            result += f"{rendered}\n"

        # details = self.api_endpoints.get_api_endpoint("get_value")
        # dbg(details.validated_func.__dict__)

        # for k, v in details.arg_schema.items():
        #     print(k)
        #     print(type(v))

        # inputs = {
        #     "value": "tm.date_array"
        # }
        # result = details.execute(instance, **inputs)
        # print(result)

        return result


class BaseApiRenderInputsSchema(ApiRenderInputsSchema):

    tag: str = Field(
        description="The tag to filter the api endpoints by.", default="kiara_api"
    )
    template_file: str = Field(
        description="The file that should contain the rendered code."
    )
    target_file: Union[str] = Field(
        description="The file to write the rendered code to.", default=None
    )


class BaseApiRenderer(ApiRenderer):

    _renderer_name = "base_api_kiara_api_renderer"
    _inputs_schema = BaseApiRenderInputsSchema
    _renderer_config_cls = ApiRendererConfig

    def __init__(
        self,
        kiara: "Kiara",
        renderer_config: Union[None, Mapping[str, Any], KiaraRendererConfig] = None,
    ):

        self._api_endpoints: ApiEndpoints = ApiEndpoints(api_cls=BaseAPI)
        super().__init__(kiara=kiara, renderer_config=renderer_config)

    def get_target_type(self) -> str:
        return "kiara_api"

    def retrieve_supported_render_targets(self) -> Union[Iterable[str], str]:
        return "kiara_api"

    def _render(
        self, instance: BaseAPI, render_config: BaseApiRenderInputsSchema
    ) -> Any:

        template_file = Path(render_config.template_file)

        if not template_file.is_file():
            raise ValueError(f"File '{template_file}' does not exist.")

        BEGIN_IMPORTS_MARKER = "# BEGIN AUTO-GENERATED-IMPORTS"
        END_IMPORTS_MARKER = "# END AUTO-GENERATED-IMPORTS"
        BEGIN_MARKER = "# BEGIN IMPORTED-ENDPOINTS"
        END_MARKER = "# END IMPORTED-ENDPOINTS"

        template_file_content = template_file.read_text()
        if BEGIN_IMPORTS_MARKER not in template_file_content:
            raise ValueError(
                f"File '{template_file}' does not contain BEGIN_IMPORTS_MARKER '{BEGIN_IMPORTS_MARKER}'."
            )
        if END_IMPORTS_MARKER not in template_file_content:
            raise ValueError(
                f"File '{template_file}' does not contain END_IMPORTS_MARKER '{END_IMPORTS_MARKER}'."
            )
        if BEGIN_MARKER not in template_file_content:
            raise ValueError(
                f"File '{template_file}' does not contain BEGIN_MARKER '{BEGIN_MARKER}'."
            )
        if END_MARKER not in template_file_content:
            raise ValueError(
                f"File '{template_file}' does not contain END_MARKER '{END_MARKER}'."
            )

        endpoint_code_template = self._kiara.render_registry.get_template(
            "kiara_api/kiara_api_endpoint.py.j2", "kiara"
        )

        tag = render_config.tag
        endpoints = find_base_api_endpoints(BaseAPI, label=tag)

        endpoint_data = []
        imports = {}
        for endpoint in endpoints:
            endpoint_name = endpoint.__name__
            doc = endpoint.__doc__
            sig_args = extract_arg_names(endpoint)
            sig_args.remove("self")

            arg_names_str = extract_proxy_arg_str(endpoint)

            sig_string, return_type = create_signature_string(endpoint, imports=imports)
            endpoint_data.append(
                {
                    "endpoint_name": endpoint_name,
                    "doc": doc.strip(),
                    "signature_str": sig_string,
                    "arg_names_str": arg_names_str,
                    "result_type": return_type,
                }
            )

        result = ""
        for endpoint_item in endpoint_data:

            rendered = endpoint_code_template.render(**endpoint_item)
            result += f"{rendered}\n"

        result = f"{BEGIN_MARKER}\n{result}    {END_MARKER}"

        pattern = rf"{BEGIN_MARKER}.*?{END_MARKER}"
        new_content = re.sub(pattern, result, template_file_content, flags=re.DOTALL)

        TYPE_CHECKING_FILTER = ["typing", "builtins", "collections", "uuid", "pathlib"]

        imports.setdefault("typing", set()).add("TYPE_CHECKING")

        imports_str = ""
        for module, items in imports.items():
            first_token = module.split(".")[0]
            if first_token in TYPE_CHECKING_FILTER:
                items_str = ", ".join(sorted(items))
                imports_str += f"from {module} import {items_str}\n"

        match = False
        imports_str += "if TYPE_CHECKING:\n"
        for module, items in imports.items():
            first_token = module.split(".")[0]
            if first_token not in TYPE_CHECKING_FILTER:
                match = True
                items_str = ", ".join(sorted(items))
                imports_str += f"    from {module} import {items_str}\n"

        if not match:
            imports_str += "    pass\n"

        imports_pattern = rf"{BEGIN_IMPORTS_MARKER}\n.*?{END_IMPORTS_MARKER}"
        new_content = re.sub(
            imports_pattern,
            f"{BEGIN_IMPORTS_MARKER}\n{imports_str}\n{END_IMPORTS_MARKER}",
            new_content,
            flags=re.DOTALL,
        )

        try_formatting = False
        try:
            import black

            try_formatting = True
        except ImportError:
            pass

        if try_formatting:
            try:
                new_content = black.format_str(new_content, mode=black.FileMode())
            except Exception as e:
                raise ValueError(f"Failed to format code: {e}")

        if render_config.target_file:
            target_file = Path(render_config.target_file)
            target_file.parent.mkdir(parents=True, exist_ok=True)
            target_file.write_text(new_content)
            print(f"Rendered api to file '{target_file}'.")
        else:
            return new_content
