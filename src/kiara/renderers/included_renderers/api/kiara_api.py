# -*- coding: utf-8 -*-
import abc
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Mapping,
    Union,
)

from pydantic.fields import Field

from kiara.api import KiaraAPI
from kiara.interfaces.python_api.proxy import ApiEndpoints
from kiara.models.rendering import RenderValueResult
from kiara.renderers import (
    KiaraRenderer,
    KiaraRendererConfig,
    RenderInputsSchema,
)
from kiara.utils.cli import terminal_print
from kiara.utils.output import (
    create_table_from_base_model_v1_cls,
)

if TYPE_CHECKING:
    from kiara.context import Kiara


class ApiRenderInputsSchema(RenderInputsSchema):

    pass


class ApiRendererConfig(KiaraRendererConfig):

    filter: Union[str, Iterable[str]] = Field(
        description="One or a list of filter tokens -- if provided -- all of which must match for the api endpoing to be in the render result.",
        default_factory=list,
    )
    # target_type: str = Field(description="The target type to render the api as.")


class ApiRenderer(
    KiaraRenderer[KiaraAPI, ApiRenderInputsSchema, RenderValueResult, ApiRendererConfig]
):
    _inputs_schema = ApiRenderInputsSchema
    _renderer_config_cls = ApiRendererConfig

    def __init__(
        self,
        kiara: "Kiara",
        renderer_config: Union[None, Mapping[str, Any], KiaraRendererConfig] = None,
    ):

        super().__init__(kiara=kiara, renderer_config=renderer_config)

        filters: Union[None, str, Iterable[str]] = self.renderer_config.filter
        if not filters:
            filters = None
        elif isinstance(filters, str):
            filters = [filters]

        self._api_endpoints: ApiEndpoints = ApiEndpoints(
            api_cls=KiaraAPI, filters=filters
        )

    @property
    def api_endpoints(self) -> ApiEndpoints:
        return self._api_endpoints

    def get_renderer_alias(self) -> str:
        return f"api_to_{self.get_target_type()}"

    def retrieve_supported_render_sources(self) -> str:
        return "kiara_api"

    def retrieve_doc(self) -> Union[str, None]:

        return f"Render the kiara API endpoints to: '{self.get_target_type()}'."

    @abc.abstractmethod
    def get_target_type(self) -> str:
        pass


class KiaraApiDocRenderer(ApiRenderer):

    _renderer_name = "kiara_api_doc_renderer"

    def get_target_type(self) -> str:
        return "html"

    def retrieve_supported_render_targets(self) -> Union[Iterable[str], str]:
        return "html"

    def _render(self, instance: KiaraAPI, render_config: ApiRenderInputsSchema) -> Any:

        # details = self.api_endpoints.get_api_endpoint("get_value")
        details = self.api_endpoints.get_api_endpoint("retrieve_aliases_info")

        # for k, v in details.arg_schema.items():
        #     print(k)
        #     print(type(v))

        terminal_print(create_table_from_base_model_v1_cls(details.arg_model))

        return "xxx"


class KiaraApiDocTextRenderer(ApiRenderer):

    _renderer_name = "kiara_api_doc_markdown_renderer"

    def get_target_type(self) -> str:
        return "markdown"

    def retrieve_supported_render_targets(self) -> Union[Iterable[str], str]:
        return "markdown"

    def _render(self, instance: KiaraAPI, render_config: ApiRenderInputsSchema) -> Any:

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
