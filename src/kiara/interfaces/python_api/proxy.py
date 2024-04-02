# -*- coding: utf-8 -*-
import inspect
from typing import Any, Callable, Dict, Iterable, List, Mapping, Type, Union

from docstring_parser import Docstring, parse
from pydantic.v1.decorator import ValidatedFunction
from pydantic.v1.main import BaseModel as BaseModel1
from rich import box
from rich.console import Group, RenderableType
from rich.markdown import Markdown
from rich.markup import escape
from rich.table import Table

from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.exceptions import KiaraException
from kiara.models.documentation import DocumentationMetadataModel
from kiara.utils.reflection import extract_signature_metadata

EXCLUDED_KEYS = ["self", "v__duplicate_kwargs", "args", "kwargs"]


class ApiEndpoint(object):
    def __init__(self, func: Callable):

        self._func = func
        self._wrapped: Union[None, ValidatedFunction] = None
        self._arg_names: Union[None, List[str]] = None
        self._param_details: Union[None, Dict[str, Any]] = None
        self._raw_doc: Union[None, str] = None
        self._doc_string: Union[None, str] = None
        self._parsed_doc: Union[Docstring, None] = None
        self._doc: Union[DocumentationMetadataModel, None] = None
        self._result_type: Union[Type, None] = None
        self._signature_metadata: Union[None, Mapping[str, Any]] = None

    @property
    def doc_string(self):

        if self._doc_string is not None:
            return self._doc_string

        _doc_string = self.raw_doc
        self._doc_string = inspect.cleandoc(_doc_string)
        return self._doc_string

    @property
    def func(self) -> Callable:
        return self._func

    @property
    def raw_doc(self) -> str:

        if self._raw_doc is not None:
            return self._raw_doc

        _doc_string = self._func.__doc__
        if _doc_string is None:
            _doc_string = ""
        self._raw_doc = _doc_string
        return self._raw_doc

    @property
    def doc(self) -> DocumentationMetadataModel:

        if self._doc is not None:
            return self._doc

        desc = self.parsed_doc.short_description
        if desc is None:
            desc = DEFAULT_NO_DESC_VALUE
        self._doc = DocumentationMetadataModel(
            description=desc,
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
                desc: Union[str, None] = p.description
                return desc if desc else ""

        return ""

    @property
    def validated_func(self) -> ValidatedFunction:

        if self._wrapped is not None:
            return self._wrapped

        self._wrapped = ValidatedFunction(self._func, None)
        return self._wrapped

    @property
    def arg_model(self) -> Type[BaseModel1]:

        # TODO: pydantic refactoring, find a different way to do this in version 2
        result: Type[BaseModel1] = self.validated_func.model
        return result

    @property
    def argument_names(self) -> List[str]:

        if self._arg_names is not None:
            return self._arg_names

        self._arg_names = [
            x for x in self.validated_func.model.__fields__ if x not in EXCLUDED_KEYS
        ]
        return self._arg_names

    @property
    def arg_schema(self) -> Dict[str, Mapping[str, Any]]:

        if self._param_details is not None:
            return self._param_details

        param_details = {
            arg_name: self.signature_metadata["parameters"][arg_name]
            for arg_name in self.argument_names
        }
        for arg_name, details in param_details.items():
            details["doc"] = self.get_arg_doc(arg_name)

        self._param_details = param_details
        return self._param_details

    @property
    def signature_metadata(self) -> Mapping[str, Any]:

        if self._signature_metadata is not None:
            return self._signature_metadata

        self._signature_metadata = extract_signature_metadata(self._func)
        return self._signature_metadata

    @property
    def result_type(self) -> Type:

        result: Type = self.signature_metadata["return_type"]
        return result

    @property
    def result_doc(self) -> str:
        if self.parsed_doc.returns:
            desc: Union[None, str] = self.parsed_doc.returns.description
            return desc if desc else DEFAULT_NO_DESC_VALUE
        else:
            return DEFAULT_NO_DESC_VALUE

    def execute(self, instance: Any, **kwargs: Any) -> Any:

        result = self.validated_func.call(instance, **kwargs)
        return result

    def validate_and_assemble_args(self, **kwargs) -> BaseModel1:

        kwargs.pop("self", None)
        return self.validated_func.init_model_instance(None, **kwargs)

    def create_arg_schema_renderable(self, **config: Any) -> RenderableType:

        table = Table(box=box.SIMPLE, show_lines=False)
        table.add_column("Field name", style="i")
        table.add_column("Type", max_width=40)
        table.add_column("Description")
        table.add_column("Required")
        table.add_column("Default", justify="right", max_width=30)

        for arg_name in self.argument_names:
            row: List[RenderableType] = [f"[b]{arg_name}[/b]"]
            arg_type = self.arg_schema[arg_name]["type"]
            arg_str = str(arg_type)
            if arg_str.startswith("<class"):
                arg_str = arg_type.__name__
            arg_str = escape(str(arg_str))
            arg_str = arg_str.replace("typing.", "")
            row.append(arg_str)
            row.append(self.arg_schema[arg_name]["doc"])

            row.append(
                "[red]yes[/red]"
                if self.arg_schema[arg_name]["required"]
                else "[green]no[/green]"
            )

            default = self.arg_schema[arg_name]["default"]
            if default is not None:
                row.append(str(self.arg_schema[arg_name]["default"]))

            table.add_row(*row)

        return table

    def create_renderable(self, **config: Any) -> RenderableType:

        full_doc = config.get("full_doc", False)

        items: List[RenderableType] = []
        if full_doc:
            items.append(Markdown(self.doc.full_doc))
        else:
            items.append(Markdown(self.doc.description))

        items.append("")
        items.append("[b]Inputs[/b]")
        items.append(self.create_arg_schema_renderable(**config))

        if self.result_type is not None:
            items.append("")
            items.append("[b]Output[/b]")
            table = Table(box=box.SIMPLE, show_lines=False)
            table.add_column("Type", style="i")
            table.add_column("Description")
            result_type_name = str(self.result_type)
            if hasattr(self.result_type, "__name__"):
                result_type_name = self.result_type.__name__
            table.add_row(result_type_name, self.result_doc)
            items.append(table)

        return Group(*items)


class ApiEndpoints(object):
    def __init__(
        self,
        api_cls: Type,
        filters: Union[None, Iterable[str], str] = None,
        exclude: Union[None, Iterable[str], str] = None,
        include_tags: Union[None, Iterable[str], str] = None,
    ):

        if filters is None:
            filters = []
        elif isinstance(filters, str):
            filters = [filters]

        if exclude is None:
            exclude = []
        elif isinstance(exclude, str):
            exclude = [exclude]

        if include_tags is None:
            include_tags = []
        elif isinstance(include_tags, str):
            include_tags = [include_tags]

        self._api_cls = api_cls
        self._filters: Iterable[str] = filters
        self._exclude: Iterable[str] = exclude
        self._include_tags: Iterable[str] = include_tags

        self._api_endpoint_names: Union[None, List[str]] = None
        self._endpoint_details: Dict[str, ApiEndpoint] = {}

    @property
    def api_endpint_names(self) -> List[str]:

        if self._api_endpoint_names is not None:
            return self._api_endpoint_names

        temp = []

        avail_methods = list(
            inspect.getmembers(self._api_cls, predicate=inspect.isfunction)
        )

        avail_methods.sort(key=lambda x: inspect.getsourcelines(x[1])[1])

        method_names = [x[0] for x in avail_methods]
        for func_name in method_names:
            if func_name.startswith("_"):
                continue

            if func_name in self._exclude:
                continue

            func = getattr(self._api_cls, func_name)
            if not callable(func):
                continue

            if self._include_tags:
                if not hasattr(func, "_tags"):
                    continue
                tags = getattr(func, "_tags")
                match = False
                for t in tags:
                    if t in self._include_tags:
                        match = True
                        break
                if not match:
                    continue

            if self._filters:
                match = True
                for f in self._filters:
                    if f not in func_name:
                        match = False
                        break
                if match:
                    temp.append(func_name)

            else:
                temp.append(func_name)

        self._api_endpoint_names = temp
        return self._api_endpoint_names

    def get_api_endpoint(self, endpoint_name: str) -> ApiEndpoint:

        if endpoint_name in self._endpoint_details:
            return self._endpoint_details[endpoint_name]

        if not hasattr(self._api_cls, endpoint_name):
            details = "Available endpoints:\n"
            for n in self.api_endpint_names:
                details += f" - {n}"
            raise KiaraException(
                f"Endpoint '{endpoint_name}' not available.", details=details
            )

        func = getattr(self._api_cls, endpoint_name)
        result = ApiEndpoint(func)
        self._endpoint_details[endpoint_name] = result
        return result

    def create_renderable(self, **config: Any) -> RenderableType:

        from rich.table import Table

        if len(self.api_endpint_names) == 1:
            table = Table(box=box.SIMPLE, show_lines=False)
        else:
            table = Table(box=box.MINIMAL, show_lines=True)
        table.add_column("Endpoint", style="i b")
        table.add_column("Documentation")

        for endpoint_name in self.api_endpint_names:
            endpoint = self.get_api_endpoint(endpoint_name)
            table.add_row(endpoint_name, endpoint.create_renderable(**config))

        return table
