# -*- coding: utf-8 -*-
import abc
import inspect
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Type,
    Union,
)

from docstring_parser import Docstring, parse
from pydantic import BaseModel
from pydantic.decorator import ValidatedFunction
from pydantic.fields import ModelField

from kiara.api import KiaraAPI
from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.rendering import RenderValueResult
from kiara.renderers import (
    KiaraRenderer,
    KiaraRendererConfig,
    RenderInputsSchema,
)
from kiara.utils.cli import terminal_print
from kiara.utils.output import create_table_from_base_model_cls

if TYPE_CHECKING:
    from kiara.context import Kiara


class ApiRenderInputsSchema(RenderInputsSchema):
    pass


class ApiRendererConfig(KiaraRendererConfig):

    pass
    # target_type: str = Field(description="The target type to render the api as.")


class ApiEndpoint(object):
    def __init__(self, func: Callable):

        self._func = func
        self._wrapped: Union[None, ValidatedFunction] = None
        self._arg_names: Union[None, List[str]] = None
        self._arg_details: Union[None, Dict[str, Any]] = None
        self._doc_string: Union[None, str] = None
        self._parsed_doc: Union[Docstring, None] = None
        self._doc: Union[DocumentationMetadataModel, None] = None

    @property
    def doc_string(self):

        if self._doc_string is not None:
            return self._doc_string

        _doc_string = self._func.__doc__
        if _doc_string is None:
            _doc_string = ""

        self._doc_string = inspect.cleandoc(_doc_string)
        return self._doc_string

    @property
    def doc(self) -> DocumentationMetadataModel:

        if self._doc is not None:
            return self._doc

        self._doc = DocumentationMetadataModel(
            description=self.parsed_doc.short_description,
            doc=self.parsed_doc.long_description,
        )
        return self._doc

    @property
    def parsed_doc(self) -> Docstring:

        if self._parsed_doc is not None:
            return self._parsed_doc

        parsed = parse(self.doc_string)
        self._parsed_doc = parsed
        return self._parsed_doc

    def get_arg_doc(self, arg_name: str) -> str:

        for p in self.parsed_doc.params:
            if p.arg_name == arg_name:
                return p.description

        return ""

    @property
    def validated_func(self) -> ValidatedFunction:

        if self._wrapped is not None:
            return self._wrapped

        self._wrapped = ValidatedFunction(self._func, None)
        return self._wrapped

    @property
    def arg_model(self) -> Type[BaseModel]:

        return self.validated_func.model

    @property
    def argument_names(self) -> List[str]:

        if self._arg_names is not None:
            return self._arg_names

        self._arg_names = [
            x
            for x in self.validated_func.model.__fields__
            if x not in ["self", "v__duplicate_kwargs", "args", "kwargs"]
        ]
        return self._arg_names

    @property
    def arg_schema(self) -> Dict[str, ModelField]:

        if self._arg_details is not None:
            return self._arg_details

        self._arg_details = {
            k: self.validated_func.model.__fields__[k] for k in self.argument_names
        }
        return self._arg_details


class ApiEndpoints(object):
    def __init__(self, api_cls: Type):

        self._api_cls = api_cls
        self._api_endpoint_names: Union[None, List[str]] = None
        self._endpoint_details: Dict[str, ApiEndpoint] = {}

    @property
    def api_endpint_names(self) -> List[str]:

        if self._api_endpoint_names is not None:
            return self._api_endpoint_names

        temp = []
        for func_name in dir(KiaraAPI):
            if func_name.startswith("_"):
                continue
            temp.append(func_name)

        self._api_endpoint_names = sorted(temp)
        return self._api_endpoint_names

    def get_api_endpoint(self, endpoint_name: str) -> ApiEndpoint:

        if endpoint_name in self._endpoint_details:
            return self._endpoint_details[endpoint_name]

        func = getattr(KiaraAPI, endpoint_name)
        result = ApiEndpoint(func)
        self._endpoint_details[endpoint_name] = result
        return result


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

        self._api_endpoints: ApiEndpoints = ApiEndpoints(api_cls=KiaraAPI)
        super().__init__(kiara=kiara, renderer_config=renderer_config)

    @property
    def api_endpoints(self) -> ApiEndpoints:
        return self._api_endpoints

    def get_renderer_alias(self) -> str:
        return f"api_to_{self.get_target_type()}"

    def retrieve_supported_render_sources(self) -> str:
        return "kiara_api"

    def retrieve_doc(self) -> Union[str, None]:

        return f"Render the kiara (of a supported type) to: '{self.get_target_type()}'."

    @abc.abstractmethod
    def get_target_type(self) -> str:
        pass


class ApiDocRenderer(ApiRenderer):

    _renderer_name = "api_doc_renderer"

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

        terminal_print(create_table_from_base_model_cls(details.arg_model))

        return "xxx"
