# -*- coding: utf-8 -*-
import importlib
import inspect
import logging
import os
import sys
import typing
from pathlib import Path
from stevedore import ExtensionManager
from types import ModuleType

from kiara import KiaraModule
from kiara.defaults import RELATIVE_PIPELINES_PATH
from kiara.utils import (
    _get_all_subclasses,
    _import_modules_recursively,
    camel_case_to_snake_case,
    is_debug,
    log_message,
)

log = logging.getLogger("kiara")

KiaraEntryPointItem = typing.Union[typing.Type, typing.Tuple, typing.Callable]
KiaraEntryPointIterable = typing.Iterable[KiaraEntryPointItem]


def find_kiara_modules_under(
    module: typing.Union[str, ModuleType], prefix: typing.Optional[str] = ""
) -> typing.Mapping[str, typing.Type[KiaraModule]]:

    if hasattr(sys, "frozen"):
        raise NotImplementedError("Pyinstaller bundling not supported yet.")

    if isinstance(module, str):
        module = importlib.import_module(module)

    _import_modules_recursively(module)

    subclasses: typing.Iterable[typing.Type[KiaraModule]] = _get_all_subclasses(
        KiaraModule
    )

    result = {}
    for sc in subclasses:

        if not sc.__module__.startswith(module.__name__):
            continue

        if inspect.isabstract(sc):
            if is_debug():
                log.warning(f"Ignoring abstract subclass: {sc}")
            else:
                log.debug(f"Ignoring abstract subclass: {sc}")
            continue

        name = _get_module_name(sc)
        path = sc.__module__[len(module.__name__) + 1 :]  # noqa

        full_name = f"{path}.{name}"

        if prefix is None:
            prefix = module.__name__
            if prefix.startswith("kiara_modules."):
                prefix = prefix[0:-14]

        if prefix:
            full_name = f"{prefix}.{full_name}"

        result[full_name] = sc

    return result


def find_kiara_pipelines_under(
    module: typing.Union[str, ModuleType]
) -> typing.List[str]:

    if hasattr(sys, "frozen"):
        raise NotImplementedError("Pyinstaller bundling not supported yet.")

    if isinstance(module, str):
        module = importlib.import_module(module)

    # TODO: allow multiple pipeline folders
    path = os.path.join(os.path.dirname(module.__file__), RELATIVE_PIPELINES_PATH)

    if not os.path.exists:
        log_message(f"Pipelines folder '{path}' does not exist, ignoring...")
        return []

    return [path]


def find_all_kiara_pipeline_paths() -> typing.Dict[str, typing.List[str]]:

    log2 = logging.getLogger("stevedore")
    out_hdlr = logging.StreamHandler(sys.stdout)
    out_hdlr.setFormatter(
        logging.Formatter("kiara pipeline search plugin error -> %(message)s")
    )
    out_hdlr.setLevel(logging.INFO)
    log2.addHandler(out_hdlr)
    log2.setLevel(logging.INFO)

    log.debug("Loading kiara pipelines...")

    mgr = ExtensionManager(
        namespace="kiara.pipelines", invoke_on_load=False, propagate_map_exceptions=True
    )

    result_entrypoints: typing.Dict[str, typing.Iterable[typing.Union[str]]] = {}
    result_dynamic: typing.Dict[str, typing.Iterable[typing.Union[str]]] = {}
    # TODO: make sure we load 'core' first?
    for plugin in mgr:

        name = plugin.name

        if (
            isinstance(plugin.plugin, tuple)
            and len(plugin.plugin) >= 1
            and callable(plugin.plugin[0])
        ) or callable(plugin.plugin):
            pipeline_paths = _find_pipeline_folders_using_callable(plugin.plugin)

            if isinstance(pipeline_paths, (str, Path)):
                pipeline_paths = [pipeline_paths]

            result_dynamic[name] = pipeline_paths
        elif isinstance(plugin.plugin, str):
            result_entrypoints[name] = [plugin.plugin]
        elif isinstance(plugin.plugin, typing.Mapping):
            raise NotImplementedError()
        elif isinstance(plugin.plugin, typing.Iterable):
            result_entrypoints[name] = plugin.plugin
        elif isinstance(plugin.plugin, ModuleType):
            result_entrypoints[name] = plugin.plugin.__name__
        else:
            raise Exception(
                f"Can't load pipelines for entrypoint '{name}': invalid type '{type(plugin.plugin)}'"
            )

    result: typing.Dict[str, typing.List] = {}

    for k, v in result_entrypoints.items():
        for item in v:
            if item not in result.setdefault(k, []):
                result[k].append(item)

    for k, v in result_dynamic.items():
        for item in v:
            if item not in result.setdefault(k, []):
                result[k].append(item)

    return result


def find_all_kiara_modules() -> typing.Dict[str, typing.Type["KiaraModule"]]:
    """Find all [KiaraModule][kiara.module.KiaraModule] subclasses via package entry points.

    TODO
    """

    log2 = logging.getLogger("stevedore")
    out_hdlr = logging.StreamHandler(sys.stdout)
    out_hdlr.setFormatter(
        logging.Formatter("kiara module plugin search error -> %(message)s")
    )
    out_hdlr.setLevel(logging.INFO)
    log2.addHandler(out_hdlr)
    log2.setLevel(logging.INFO)

    log.debug("Finding kiara modules from search paths...")

    mgr = ExtensionManager(
        namespace="kiara.modules",
        invoke_on_load=False,
        propagate_map_exceptions=True,
    )

    result_entrypoints: typing.Dict[str, typing.Type] = {}
    result_dynamic: typing.Dict[str, typing.Type] = {}
    for plugin in mgr:

        name = plugin.name

        if isinstance(plugin.plugin, type) and issubclass(plugin.plugin, KiaraModule):
            ep = plugin.entry_point
            module_cls = ep.load()
            setattr(module_cls, "_module_type_name", name)
            result_entrypoints[name] = module_cls
        elif (
            isinstance(plugin.plugin, tuple)
            and len(plugin.plugin) >= 1
            and callable(plugin.plugin[0])
        ) or callable(plugin.plugin):
            modules = _find_kiara_modules_using_callable(plugin.plugin)

            for k, v in modules.items():
                _name = f"{name}.{k}"
                if _name in result_dynamic.keys():
                    raise Exception(f"Duplicate module name: {_name}")
                result_dynamic[_name] = v
        else:
            raise NotImplementedError()

    for k, v in result_dynamic.items():
        if k in result_entrypoints.keys():
            raise Exception(f"Duplicate module name: {k}")
        result_entrypoints[k] = v

    result: typing.Dict[str, typing.Type[KiaraModule]] = {}

    for k, v in result_entrypoints.items():
        if k.startswith("core."):
            k = k[5:]
        result[k] = v

    return result


def _get_module_name(module: typing.Type[KiaraModule]):

    if hasattr(module, "_module_type_name"):
        return module._module_type_name  # type: ignore
    else:
        name = camel_case_to_snake_case(module.__name__)
        if name.endswith("_module"):
            name = name[0:-7]
        if not inspect.isabstract(module):
            setattr(module, "_module_type_name", name)
        return name


def _find_pipeline_folders_using_callable(
    func: typing.Union[typing.Callable, typing.Tuple]
) -> typing.List[str]:

    # TODO: typecheck?
    return _callable_wrapper(func=func)  # type: ignore


def _find_kiara_modules_using_callable(
    func: typing.Union[typing.Callable, typing.Tuple]
) -> typing.Mapping[str, typing.Type[KiaraModule]]:

    # TODO: typecheck?
    return _callable_wrapper(func=func)  # type: ignore


def _callable_wrapper(func: typing.Union[typing.Callable, typing.Tuple]) -> typing.Any:

    _func = None
    _args = None
    _kwargs = None
    if isinstance(func, tuple):
        if len(func) >= 1:
            _func = func[0]
        if len(func) >= 2:
            _args = func[1]
        if len(func) >= 3:
            _kwargs = func[2]

        if len(func) > 3:
            raise ValueError(f"Can't parse entry: {func}")
    elif callable(func):
        _func = func
    else:
        raise TypeError(f"Invalid entry type: {type(func)}")

    if not _args:
        _args = []
    if not _kwargs:
        _kwargs = {}

    if isinstance(_args, str):
        _args = [_args]

    assert _func is not None
    result = _func(*_args, **_kwargs)
    return result
