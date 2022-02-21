# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import importlib
import inspect
import logging
import os
import sys
import typing
from stevedore import ExtensionManager
from types import ModuleType

from kiara.data.types import ValueType
from kiara.metadata import MetadataModel
from kiara.utils import (
    _get_all_subclasses,
    _import_modules_recursively,
    camel_case_to_snake_case,
    is_debug,
    log_message,
)

if typing.TYPE_CHECKING:
    from kiara.module import KiaraModule
    from kiara.operations import OperationType

log = logging.getLogger("kiara")

KiaraEntryPointItem = typing.Union[typing.Type, typing.Tuple, typing.Callable]
KiaraEntryPointIterable = typing.Iterable[KiaraEntryPointItem]

SUBCLASS_TYPE = typing.TypeVar("SUBCLASS_TYPE")


def _get_subclass_name(module: typing.Type) -> str:
    """Utility method to auto-generate a more or less nice looking id_or_alias for a class."""

    name = camel_case_to_snake_case(module.__name__)
    return name


def find_subclasses_under(
    base_class: typing.Type[SUBCLASS_TYPE],
    module: typing.Union[str, ModuleType],
    prefix: typing.Optional[str] = "",
    remove_namespace_tokens: typing.Optional[typing.Iterable[str]] = None,
    module_name_func: typing.Callable = None,
) -> typing.Mapping[str, typing.Type[SUBCLASS_TYPE]]:
    """Find all (non-abstract) subclasses of a base class that live under a module (recursively).

    Arguments:
        base_class: the parent class
        module: the module to search
        prefix: a string to use as a result items namespace prefix, defaults to an empty string, use 'None' to indicate the module path should be used
        remove_namespace_tokens: a list of strings to remove from module names when autogenerating subclass ids, and prefix is None

    Returns:
        a map containing the (fully namespaced) id of the subclass as key, and the actual class object as value
    """

    if hasattr(sys, "frozen"):
        raise NotImplementedError("Pyinstaller bundling not supported yet.")

    if isinstance(module, str):
        module = importlib.import_module(module)

    _import_modules_recursively(module)

    subclasses: typing.Iterable[typing.Type[SUBCLASS_TYPE]] = _get_all_subclasses(
        base_class
    )

    result = {}
    for sc in subclasses:

        if not sc.__module__.startswith(module.__name__):
            continue

        if inspect.isabstract(sc):
            if is_debug():
                # import traceback
                # traceback.print_stack()
                log.warning(f"Ignoring abstract subclass: {sc}")
            else:
                log.debug(f"Ignoring abstract subclass: {sc}")
            continue

        if module_name_func is None:
            module_name_func = _get_subclass_name
        name = module_name_func(sc)
        path = sc.__module__[len(module.__name__) + 1 :]  # noqa

        if path:
            full_name = f"{path}.{name}"
        else:
            full_name = name

        if prefix is None:
            prefix = module.__name__ + "."
            if remove_namespace_tokens:
                for rnt in remove_namespace_tokens:
                    if prefix.startswith(rnt):
                        prefix = prefix[0 : -len(rnt)]  # noqa

        if prefix:
            full_name = f"{prefix}.{full_name}"

        result[full_name] = sc

    return result


def load_all_subclasses_for_entry_point(
    entry_point_name: str,
    base_class: typing.Type[SUBCLASS_TYPE],
    set_id_attribute: typing.Union[None, str] = None,
    remove_namespace_tokens: typing.Union[typing.Iterable[str], bool, None] = None,
) -> typing.Dict[str, typing.Type[SUBCLASS_TYPE]]:
    """Find all subclasses of a base class via package entry points.

    Arguments:
        entry_point_name: the entry point name to query entries for
        base_class: the base class to look for
        set_id_attribute: whether to set the entry point id as attribute to the class, if None, no id attribute will be set, if a string, the attribute with that name will be set
        remove_namespace_tokens: a list of strings to remove from module names when autogenerating subclass ids, and prefix is None, or a boolean in which case all or none namespaces will be removed

    TODO
    """

    log2 = logging.getLogger("stevedore")
    out_hdlr = logging.StreamHandler(sys.stdout)
    out_hdlr.setFormatter(
        logging.Formatter(f"{entry_point_name} plugin search error -> %(message)s")
    )
    out_hdlr.setLevel(logging.INFO)
    log2.addHandler(out_hdlr)
    log2.setLevel(logging.INFO)

    log.debug(f"Finding {entry_point_name} items from search paths...")

    mgr = ExtensionManager(
        namespace=entry_point_name,
        invoke_on_load=False,
        propagate_map_exceptions=True,
    )

    result_entrypoints: typing.Dict[str, typing.Type] = {}
    result_dynamic: typing.Dict[str, typing.Type] = {}
    for plugin in mgr:
        name = plugin.name

        if isinstance(plugin.plugin, type) and issubclass(plugin.plugin, base_class):
            ep = plugin.entry_point
            module_cls = ep.load()

            if set_id_attribute:
                if hasattr(module_cls, set_id_attribute):
                    if not getattr(module_cls, set_id_attribute) == name:
                        log.warning(
                            f"Item id mismatch for type {entry_point_name}: {getattr(module_cls, set_id_attribute)} != {name}, entry point key takes precedence: {name})"
                        )
                        setattr(module_cls, set_id_attribute, name)

                else:
                    setattr(module_cls, set_id_attribute, name)
            result_entrypoints[name] = module_cls
        elif (
            isinstance(plugin.plugin, tuple)
            and len(plugin.plugin) >= 1
            and callable(plugin.plugin[0])
        ) or callable(plugin.plugin):
            modules = _callable_wrapper(plugin.plugin)

            for k, v in modules.items():
                _name = f"{name}.{k}"
                if _name in result_dynamic.keys():
                    raise Exception(
                        f"Duplicate item name for type {entry_point_name}: {_name}"
                    )
                result_dynamic[_name] = v

        else:
            raise Exception(
                f"Can't load subclasses for entry point {entry_point_name} and base class {base_class}: invalid plugin type {type(plugin.plugin)}"
            )

    for k, v in result_dynamic.items():
        if k in result_entrypoints.keys():
            raise Exception(f"Duplicate item name for type {entry_point_name}: {k}")
        result_entrypoints[k] = v

    result: typing.Dict[str, typing.Type[SUBCLASS_TYPE]] = {}
    for k, v in result_entrypoints.items():

        if remove_namespace_tokens:
            if remove_namespace_tokens is True:
                k = k.split(".")[-1]
            elif isinstance(remove_namespace_tokens, typing.Iterable):
                for rnt in remove_namespace_tokens:
                    if k.startswith(rnt):
                        k = k[len(rnt) :]  # noqa

        if k in result.keys():
            msg = ""
            if set_id_attribute:
                msg = f" Check whether '{v.__name__}' is missing the '{set_id_attribute}' class attribute (in case this is a sub-class), or it's '{k}' value is also set in another class?"
            raise Exception(
                f"Duplicate item name for base class {base_class}: {k}.{msg}"
            )
        result[k] = v

    return result


def find_all_kiara_modules() -> typing.Dict[str, typing.Type["KiaraModule"]]:
    """Find all [KiaraModule][kiara.module.KiaraModule] subclasses via package entry points.

    TODO
    """

    from kiara.module import KiaraModule

    modules = load_all_subclasses_for_entry_point(
        entry_point_name="kiara.modules",
        base_class=KiaraModule,  # type: ignore
        set_id_attribute="_module_type_name",
        remove_namespace_tokens=["core."],
    )
    result = {}
    # need to test this, since I couldn't add an abstract method to the KiaraModule class itself (mypy complained because it is potentially overloaded)
    for k, cls in modules.items():

        if not hasattr(cls, "process"):
            msg = f"Ignoring module class '{cls}': no 'process' method."
            if is_debug():
                log.warning(msg)
            else:
                log.debug(msg)
            continue

        # TODO: check signature of process method

        if k.startswith("_"):
            tokens = k.split(".")
            if len(tokens) == 1:
                k = k[1:]
            else:
                k = ".".join(tokens[1:])

        result[k] = cls
    return result


def find_all_metadata_models() -> typing.Dict[str, typing.Type["MetadataModel"]]:
    """Find all [KiaraModule][kiara.module.KiaraModule] subclasses via package entry points.

    TODO
    """

    return load_all_subclasses_for_entry_point(
        entry_point_name="kiara.metadata_models",
        base_class=MetadataModel,
        set_id_attribute="_metadata_key",
        remove_namespace_tokens=["core."],
    )


def find_all_value_types() -> typing.Dict[str, typing.Type["ValueType"]]:
    """Find all [KiaraModule][kiara.module.KiaraModule] subclasses via package entry points.

    TODO
    """

    all_value_types = load_all_subclasses_for_entry_point(
        entry_point_name="kiara.value_types",
        base_class=ValueType,  # type: ignore
        set_id_attribute="_value_type_name",
        remove_namespace_tokens=True,
    )

    invalid = [x for x in all_value_types.keys() if "." in x]
    if invalid:
        raise Exception(
            f"Invalid value type name(s), type names can't contain '.': {', '.join(invalid)}"
        )

    return all_value_types


def find_all_operation_types() -> typing.Dict[str, typing.Type["OperationType"]]:

    from kiara.operations import OperationType

    return load_all_subclasses_for_entry_point(
        entry_point_name="kiara.operation_types",
        base_class=OperationType,
        set_id_attribute="_operation_type_name",
        remove_namespace_tokens=["core."],
    )


def _get_and_set_module_name(module: typing.Type["KiaraModule"]):

    if hasattr(module, "_module_type_name"):
        return module._module_type_name  # type: ignore
    else:
        name = camel_case_to_snake_case(module.__name__)
        if name.endswith("_module"):
            name = name[0:-7]
        if not inspect.isabstract(module):
            setattr(module, "_module_type_name", name)
        return name


def _get_and_set_metadata_model_name(module: typing.Type["KiaraModule"]):

    if hasattr(module, "_metadata_key"):
        return module._metadata_key  # type: ignore
    else:
        name = camel_case_to_snake_case(module.__name__)
        if name.endswith("_metadata"):
            name = name[0:-9]
        if not inspect.isabstract(module):
            setattr(module, "_metadata_key", name)
        return name


def _get_and_set_value_type_name(module: typing.Type["KiaraModule"]):

    if hasattr(module, "_value_type_name"):
        return module._value_type_name  # type: ignore
    else:
        name = camel_case_to_snake_case(module.__name__)
        if name.endswith("_type"):
            name = name[0:-5]
        if not inspect.isabstract(module):
            setattr(module, "_value_type_name", name)
        return name


def _get_and_set_operation_type_name(module: typing.Type["KiaraModule"]):

    if hasattr(module, "_operation_type_name"):
        return module._operation_type_name  # type: ignore
    else:
        name = camel_case_to_snake_case(module.__name__)
        if name.endswith("_type"):
            name = name[0:-5]
        if not inspect.isabstract(module):
            setattr(module, "_operation_type_name", name)
        return name


def find_kiara_modules_under(
    module: typing.Union[str, ModuleType],
    prefix: typing.Optional[str] = "",
    remove_namespace_tokens: typing.Optional[typing.Iterable[str]] = None,
) -> typing.Mapping[str, typing.Type["KiaraModule"]]:

    from kiara.module import KiaraModule

    if remove_namespace_tokens is None:
        remove_namespace_tokens = ["kiara_modules."]

    return find_subclasses_under(
        base_class=KiaraModule,  # type: ignore
        module=module,
        prefix=prefix,
        remove_namespace_tokens=remove_namespace_tokens,
        module_name_func=_get_and_set_module_name,
    )


def find_metadata_models_under(
    module: typing.Union[str, ModuleType], prefix: typing.Optional[str] = ""
) -> typing.Mapping[str, typing.Type[MetadataModel]]:

    return find_subclasses_under(
        base_class=MetadataModel,
        module=module,
        prefix=prefix,
        remove_namespace_tokens=[],
        module_name_func=_get_and_set_metadata_model_name,
    )


def find_value_types_under(
    module: typing.Union[str, ModuleType], prefix: typing.Optional[str] = ""
) -> typing.Mapping[str, typing.Type[ValueType]]:

    return find_subclasses_under(
        base_class=ValueType,  # type: ignore
        module=module,
        prefix=prefix,
        remove_namespace_tokens=[],
        module_name_func=_get_and_set_value_type_name,
    )


def find_operations_under(
    module: typing.Union[str, ModuleType], prefix: typing.Optional[str] = ""
) -> typing.Mapping[str, typing.Type["OperationType"]]:

    from kiara.operations import OperationType

    return find_subclasses_under(
        base_class=OperationType,
        module=module,
        prefix=prefix,
        remove_namespace_tokens=[],
        module_name_func=_get_and_set_operation_type_name,
    )


def find_pipeline_base_path_for_module(
    module: typing.Union[str, ModuleType]
) -> typing.Optional[str]:

    if hasattr(sys, "frozen"):
        raise NotImplementedError("Pyinstaller bundling not supported yet.")

    if isinstance(module, str):
        module = importlib.import_module(module)

    module_file = module.__file__
    assert module_file is not None
    path = os.path.dirname(module_file)

    if not os.path.exists:
        log_message(f"Pipelines folder '{path}' does not exist, ignoring...")
        return None

    return path


def find_all_kiara_pipeline_paths(
    skip_errors: bool = False,
) -> typing.Dict[str, typing.List[typing.Tuple[typing.Optional[str], str]]]:

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

    result_entrypoints: typing.Dict[str, typing.Tuple[typing.Optional[str], str]] = {}
    result_dynamic: typing.Dict[str, typing.Tuple[typing.Optional[str], str]] = {}
    # TODO: make sure we load 'core' first?
    for plugin in mgr:

        name = plugin.name

        if (
            isinstance(plugin.plugin, tuple)
            and len(plugin.plugin) >= 1
            and callable(plugin.plugin[0])
        ) or callable(plugin.plugin):
            pipeline_path_tuple = _find_pipeline_folders_using_callable(plugin.plugin)
            result_dynamic[name] = pipeline_path_tuple

        # elif isinstance(plugin.plugin, str):
        #     if skip_errors:
        #         continue
        #
        #     raise NotImplementedError(
        #         f"Finding pipeline paths using item '{plugin.plugin}' not supported."
        #     )
        #     # module_name = plugin.plugin
        #     # try:
        #     #     m = importlib.import_module(module_name)
        #     #     pipeline_path_tuple = _find_pipeline_folders_using_callable(m)
        #     # except Exception:
        #     #     raise Exception(
        #     #         f"Can't load pipelines for module '{module_name}': module does not exist"
        #     #     )
        #     # result_entrypoints[name] = pipeline_path_tuple
        # elif isinstance(plugin.plugin, typing.Mapping):
        #     if skip_errors:
        #         continue
        #     raise NotImplementedError(
        #         f"Finding pipeline paths for mapping '{plugin.plugin}' not supported."
        #     )
        # elif isinstance(plugin.plugin, typing.Iterable):
        #     if skip_errors:
        #         continue
        #     raise NotImplementedError(
        #         f"Finding pipeline paths for iterable '{plugin.plugin}' not supported."
        #     )
        #     result_entrypoints[name] = plugin.plugin
        # elif isinstance(plugin.plugin, ModuleType):
        #     if skip_errors:
        #         continue
        #     # print(f"Entrypoint type not supported yet: {plugin.plugin}")
        #     raise NotImplementedError(
        #         f"Pipeline entrypoint lookup ModuleType not supported yet: {plugin.plugin}"
        #     )
        #     # result_entrypoints[name] = _find_pipeline_folders_using_callable(
        #     #     plugin.plugin
        #     # )
        else:
            msg = f"Can't load pipelines for entrypoint '{name}': invalid plugin type '{type(plugin.plugin)}'"
            if skip_errors:
                log_message(msg)
                continue
            raise Exception(msg)

    result: typing.Dict[str, typing.List[typing.Tuple[typing.Optional[str], str]]] = {}

    for k, v in result_entrypoints.items():
        result.setdefault(k, []).append(v)

    for k, v in result_dynamic.items():
        result.setdefault(k, []).append(v)

    return result


def _find_pipeline_folders_using_callable(
    func: typing.Union[typing.Callable, typing.Tuple]
) -> typing.Tuple[typing.Optional[str], str]:

    if not callable(func):
        assert len(func) >= 2
        args = func[1]
        assert len(args) == 1
        module_path: typing.Optional[str] = args[0]
    else:
        module_path = None
    path = _callable_wrapper(func=func)  # type: ignore
    assert isinstance(path, str)
    return (module_path, path)


# def _find_kiara_modules_using_callable(
#     func: typing.Union[typing.Callable, typing.Tuple]
# ) -> typing.Mapping[str, typing.Type[KiaraModule]]:
#
#     # TODO: typecheck?
#     return _callable_wrapper(func=func)  # type: ignore


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
