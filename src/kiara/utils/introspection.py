# -*- coding: utf-8 -*-
import inspect
import typing
from typing import Any, Callable, Dict


def extract_cls(arg: Any, imports: Dict[str, typing.Set[str]]) -> str:

    if arg in (type(None), None):
        return "None"
    elif isinstance(arg, type):
        name = arg.__name__
        module = arg.__module__
        if module == "typing":
            imports.setdefault(module, set()).add(name)
            return name
        elif module != "builtins":
            imports.setdefault(module, set()).add(name)
            return f'"{name}"'
        else:
            return name
    elif isinstance(arg, typing._UnionGenericAlias):  # type: ignore
        all_args = []
        for a in arg.__args__:
            cls = extract_cls(a, imports=imports)
            all_args.append(cls)

        imports.setdefault("typing", set()).add("Union")
        return f"Union[{', '.join(all_args)}]"
    elif isinstance(arg, typing._LiteralSpecialForm):  # type: ignore
        return "Literal"

    elif isinstance(arg, typing._GenericAlias):  # type: ignore

        origin_cls = extract_cls(arg.__origin__, imports=imports)
        if origin_cls == "Literal":
            all_args_str = ", ".join((f'"{x}"' for x in arg.__args__))
            imports.setdefault("typing", set()).add("Literal")
            return f"Literal[{all_args_str}]"

        all_args = []
        for a in arg.__args__:
            cls = extract_cls(a, imports=imports)
            all_args.append(cls)

        if origin_cls == '"Mapping"':
            imports.setdefault("typing", set()).add("Mapping")
            assert len(all_args) == 2
            return f"Mapping[{all_args[0]}, {all_args[1]}]"
        elif origin_cls == '"Iterable"':
            imports.setdefault("typing", set()).add("Iterable")
            return f"Iterable[{', '.join(all_args)}]"
        elif origin_cls in ('"List"', "list"):
            imports.setdefault("typing", set()).add("List")
            return f"List[{', '.join(all_args)}]"
        elif origin_cls == "dict":
            assert len(all_args) == 2
            imports.setdefault("typing", set()).add("Dict")
            return f"Dict[{all_args[0]}, {all_args[1]}]"
        elif origin_cls == "type":
            imports.setdefault("typing", set()).add("Type")
            result = f"Type[{', '.join(all_args)}]"
            return result
        else:
            raise Exception(f"Unexpected generic alias: {origin_cls}")
    elif isinstance(arg, typing.ForwardRef):
        return f'"{arg.__forward_arg__}"'
    else:
        raise Exception(f"Unexpected type '{type(arg)}' for arg: {arg}")


def create_default_string(default: Any) -> str:

    if default is None:
        return "None"
    elif isinstance(default, bool):
        return str(default)
    elif isinstance(default, str):
        if "\\" in default:
            default = f'r"{default}"'
            return default
        else:
            return f'"{default}"'
    else:
        raise Exception(f"Unexpected default value: {default}")


def parse_signature_args(func: Callable, imports: Dict[str, typing.Set[str]]) -> str:
    sig = inspect.signature(func)

    all_tokens = []
    param: inspect.Parameter
    for field_name, param in sig.parameters.items():
        if field_name == "self":
            all_tokens.append("self")
        else:
            arg_str = extract_cls(arg=param.annotation, imports=imports)

            if param.kind == inspect.Parameter.VAR_POSITIONAL:
                sig_token = f"*{field_name}: {arg_str}"
            elif param.kind == inspect.Parameter.VAR_KEYWORD:
                sig_token = f"**{field_name}: {arg_str}"
            else:
                sig_token = f"{field_name}: {arg_str}"

            if param.default != inspect.Parameter.empty:
                default_str = create_default_string(default=param.default)
                sig_token += f" = {default_str}"

            all_tokens.append(sig_token)

    return ", ".join(all_tokens)


def parse_signature_return(func: Callable, imports: Dict[str, typing.Set[str]]) -> str:

    sig = inspect.signature(func)
    sig_return_type = sig.return_annotation
    if isinstance(sig_return_type, str):
        return f'"{sig_return_type}"'
    elif sig_return_type == inspect.Parameter.empty:
        return "None"
    else:
        sig_return_type_str = extract_cls(arg=sig_return_type, imports=imports)
        return sig_return_type_str


def create_signature_string(
    func: Callable, imports: Dict[str, typing.Set[str]]
) -> typing.Tuple[str, typing.Union[str, None]]:

    params = parse_signature_args(func=func, imports=imports)
    return_type = parse_signature_return(func=func, imports=imports)
    if return_type == "None":
        sig_str = f"def {func.__name__}({params}):"
        _return_type = None
    else:
        sig_str = f"def {func.__name__}({params}) -> {return_type}:"
        _return_type = return_type

    return sig_str, _return_type


def extract_arg_names(func: Callable) -> typing.List[str]:
    sig = inspect.signature(func)
    return list(sig.parameters.keys())


def extract_proxy_arg_str(func: Callable) -> str:

    sig = inspect.signature(func)
    arg_str = ""
    for field_name, param in sig.parameters.items():

        if field_name == "self":
            continue

        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            arg_str += f"*{field_name}, "
        elif param.kind == inspect.Parameter.VAR_KEYWORD:
            arg_str += f"**{field_name}, "
        else:
            arg_str += f"{field_name}={field_name}, "

    if arg_str.endswith(", "):
        arg_str = arg_str[:-2]
    return arg_str
