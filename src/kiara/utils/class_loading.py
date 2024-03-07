# -*- coding: utf-8 -*-
import functools

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import importlib
import inspect
import logging
import os
import sys
from pkgutil import iter_modules
from types import ModuleType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import structlog

from kiara.utils import (
    _get_all_subclasses,
    camel_case_to_snake_case,
    is_debug,
    is_develop,
    log_exception,
    log_message,
)

if TYPE_CHECKING:
    from click import Command

    from kiara.data_types import DataType
    from kiara.models import KiaraModel
    from kiara.modules import KiaraModule
    from kiara.operations import OperationType
    from kiara.registries import KiaraArchive
    from kiara.renderers import KiaraRenderer


logger = structlog.getLogger()

KiaraEntryPointItem = Union[Type, Tuple, Callable]
KiaraEntryPointIterable = Iterable[KiaraEntryPointItem]

SUBCLASS_TYPE = TypeVar("SUBCLASS_TYPE")


def _default_id_func(cls: Type) -> str:
    """Utility method to auto-generate a more or less nice looking id_or_alias for a class."""
    name = camel_case_to_snake_case(cls.__name__)
    path = cls.__module__

    if path.startswith("kiara_modules."):
        tokens = path.split(".")
        if len(tokens) == 2:
            path = tokens[1]
        else:
            path = ".".join(tokens[2:])

    if path:
        full_name = f"{path}.{name}"
    else:
        full_name = name
    return full_name


def _cls_name_id_func(cls: Type) -> str:
    """Utility method to auto-generate a more or less nice looking id_or_alias for a class."""
    name: str = camel_case_to_snake_case(cls.__name__)
    return name


def find_subclasses_under(
    base_class: Type[SUBCLASS_TYPE],
    python_module: Union[str, ModuleType],
) -> List[Type[SUBCLASS_TYPE]]:
    """
    Find all (non-abstract) subclasses of a base class that live under a module (recursively).

    Arguments:
    ---------
        base_class: the parent class
        python_module: the Python module to search

    Returns:
    -------
        a list of all subclasses
    """
    # if hasattr(sys, "frozen"):
    #     raise NotImplementedError("Pyinstaller bundling not supported yet.")

    try:
        if isinstance(python_module, str):
            python_module = importlib.import_module(python_module)

        _import_modules_recursively(python_module)
    except Exception as e:
        log_exception(e)
        log_message("ignore.python_module", module=str(python_module), reason=str(e))
        return []

    subclasses: Iterable[Type[SUBCLASS_TYPE]] = _get_all_subclasses(base_class)

    result = []
    for sc in subclasses:

        if not sc.__module__.startswith(python_module.__name__):
            continue

        result.append(sc)

    return result


def _process_subclass(
    sub_class: Type,
    base_class: Type,
    type_id_key: Union[str, None],
    type_id_func: Union[Callable, None],
    type_id_no_attach: bool,
    attach_python_metadata: Union[bool, str] = False,
    ignore_abstract_classes: bool = True,
    ignore_modules_with_null_module_name: bool = True,
) -> Union[str, None]:
    """
    Process subclasses of a base class that live under a module (recursively).

    Arguments:
    ---------
        base_class: the parent class
        python_module: the Python module to search
        ignore_abstract_classes: whether to include abstract classes in the result
        type_id_key: if provided, the found classes will have their id attached as an attribute, using the value of this as the name. if an attribute of this name already exists, it will be used as id without further processing
        type_id_func: a function to take the found class as input, and returns a string representing the id of the class. By default, the module path + "." + class name (snake-case) is used (minus the string 'kiara_modules.<project_name>'', if it exists at the beginning
        type_id_no_attach: in case you want to use the type_id_key to set the id, but don't want it attached to classes that don't have it, set this to true. In most cases, you won't need this option
        attach_python_metadata: whether to attach a [PythonClass][kiara.models.python_class.PythonClass] metadata model to the class. By default, '_python_class' is used as attribute name if this argument is 'True', If this argument is a string, that will be used as name instead.
        ignore_modules_with_null_module_name: ignore modules that have their '_module_type_name' attribute set to 'None', this is mostly useful to filter out base classes

    Returns:
    -------
        the type id
    """
    is_abstract = inspect.isabstract(sub_class)
    if ignore_abstract_classes and is_abstract:

        if sub_class.__dict__.get("_is_abstract", False):
            return None

        if is_develop():
            from kiara.modules import KiaraModule

            if base_class == KiaraModule and is_develop():
                missing = []
                abs_meth = sub_class.__abstractmethods__
                if "create_inputs_schema" in abs_meth:
                    missing.append("create_inputs_schema")
                if "create_outputs_schema" in abs_meth:
                    missing.append("create_outputs_schema")
                if not hasattr(sub_class, "process"):
                    missing.append("process")

                if missing:
                    name = f"{sub_class.__module__}.{sub_class.__name__}"
                    title = "Invalid kiara module"
                    if hasattr(sub_class, "_module_type_name"):
                        name = f"**{name}** ( *{sub_class._module_type_name}* )"  # type: ignore
                        title = f"{title} '[i]{sub_class._module_type_name}[/i]'"  # type: ignore
                    msg = f"Invalid kiara module: {name}\n\nMissing method(s):"
                    for m in missing:
                        msg = f"{msg}\n- *{m}*"

                    from rich.markdown import Markdown

                    from kiara.utils.develop import log_dev_message

                    log_dev_message(msg=Markdown(msg), title=title)

        log_message(
            "ignore.subclass",
            sub_class=f"{sub_class.__module__}.{sub_class.__name__}",
            base_class=f"{base_class.__module__}.{base_class.__name__}",
            reason="subclass is abstract",
        )
        return None

    if type_id_func is None:
        type_id_func = _default_id_func

    if type_id_key:

        if hasattr(sub_class, type_id_key):
            type_id: Union[str, None] = getattr(sub_class, type_id_key)
            if type_id is None and ignore_modules_with_null_module_name:
                log_message(
                    "ignore.subclass",
                    sub_class=f"{sub_class.__module__}.{sub_class.__name__}",
                    base_class=f"{base_class.__module__}.{base_class.__name__}",
                    reason=f"'{ type_id_key }' subclass is set to 'None'",
                )
                return None
            if not type_id and not is_abstract:
                raise Exception(
                    f"Class attribute '{type_id_key}' is 'None' for class '{sub_class.__name__}', this is not allowed."
                )
            elif not type_id:
                type_id = type_id_func(sub_class)
        else:
            type_id = type_id_func(sub_class)
            if not type_id_no_attach:
                setattr(sub_class, type_id_key, type_id)
    else:
        type_id = type_id_func(sub_class)

    if attach_python_metadata:
        from kiara.models.python_class import PythonClass

        pm_key = "_python_class"
        if isinstance(attach_python_metadata, str):
            pm_key = attach_python_metadata
        pc = PythonClass.from_class(sub_class)
        setattr(sub_class, pm_key, pc)

    return type_id


def load_all_subclasses_for_entry_point(
    entry_point_name: str,
    base_class: Type[SUBCLASS_TYPE],
    ignore_abstract_classes: bool = True,
    type_id_key: Union[str, None] = None,
    type_id_func: Union[Callable, None] = None,
    type_id_no_attach: bool = False,
    attach_python_metadata: Union[bool, str] = False,
) -> Dict[str, Type[SUBCLASS_TYPE]]:
    """
    Find all subclasses of a base class via package entry points.

    Arguments:
    ---------
        entry_point_name: the entry point name to query entries for
        base_class: the base class to look for
        ignore_abstract_classes: whether to include abstract classes in the result
        type_id_key: if provided, the found classes will have their id attached as an attribute, using the value of this as the name. if an attribute of this name already exists, it will be used as id without further processing
        type_id_func: a function to take the found class as input, and returns a string representing the id of the class. By default, the module path + "." + class name (snake-case) is used (minus the string 'kiara_modules.<project_name>'', if it exists at the beginning
        type_id_no_attach: in case you want to use the type_id_key to set the id, but don't want it attached to classes that don't have it, set this to true. In most cases, you won't need this option
        attach_python_metadata: whether to attach a [PythonClass][kiara.models.python_class.PythonClass] metadata model to the class. By default, '_python_class' is used as attribute name if this argument is 'True', If this argument is a string, that will be used as name instead.
    """
    log2 = logging.getLogger("stevedore")
    out_hdlr = logging.StreamHandler(sys.stdout)
    out_hdlr.setFormatter(
        logging.Formatter(
            f"{entry_point_name} plugin search message/error -> %(message)s"
        )
    )
    out_hdlr.setLevel(logging.INFO)
    log2.addHandler(out_hdlr)
    if is_debug():
        log2.setLevel(logging.DEBUG)
    else:
        out_hdlr.setLevel(logging.INFO)
        log2.setLevel(logging.INFO)

    log_message("events.loading.entry_points", entry_point_name=entry_point_name)

    from stevedore import ExtensionManager

    mgr = ExtensionManager(
        namespace=entry_point_name,
        invoke_on_load=False,
        propagate_map_exceptions=True,
    )

    result_entrypoints: Dict[str, Type[SUBCLASS_TYPE]] = {}
    result_dynamic: Dict[str, Type[SUBCLASS_TYPE]] = {}

    for plugin in mgr:
        name = plugin.name

        if isinstance(plugin.plugin, type):
            # this means an actual (sub-)class was provided in the entrypoint

            cls = plugin.plugin
            if not issubclass(cls, base_class):
                log_message(
                    "ignore.entrypoint",
                    entry_point=name,
                    base_class=base_class,
                    sub_class=plugin.plugin,
                    reason=f"Entry point reference not a subclass of '{base_class}'.",
                )
                continue

            _process_subclass(
                sub_class=cls,
                base_class=base_class,
                type_id_key=type_id_key,
                type_id_func=type_id_func,
                type_id_no_attach=type_id_no_attach,
                attach_python_metadata=attach_python_metadata,
                ignore_abstract_classes=ignore_abstract_classes,
            )

            result_entrypoints[name] = cls
        elif (
            isinstance(plugin.plugin, tuple)
            and len(plugin.plugin) >= 1
            and callable(plugin.plugin[0])
        ) or callable(plugin.plugin):
            try:
                if callable(plugin.plugin):
                    func = plugin.plugin
                    args = []
                else:
                    func = plugin.plugin[0]
                    args = plugin.plugin[1:]
                classes = func(*args)
            except Exception as e:
                log_exception(e)
                raise Exception(f"Error trying to load plugin '{plugin.plugin}': {e}")

            for sub_class in classes:
                type_id = _process_subclass(
                    sub_class=sub_class,
                    base_class=base_class,
                    type_id_key=type_id_key,
                    type_id_func=type_id_func,
                    type_id_no_attach=type_id_no_attach,
                    attach_python_metadata=attach_python_metadata,
                    ignore_abstract_classes=ignore_abstract_classes,
                )

                if type_id is None:
                    continue

                if type_id in result_dynamic.keys():
                    raise Exception(
                        f"Duplicate type id '{type_id}' for type {entry_point_name}: {result_dynamic[type_id]} -- {sub_class}"
                    )
                result_dynamic[type_id] = sub_class

        else:
            raise Exception(
                f"Can't load subclasses for entry point {entry_point_name} and base class {base_class}: invalid plugin type {type(plugin.plugin)}"
            )

    for k, v in result_dynamic.items():
        if k in result_entrypoints.keys():
            msg = f"Duplicate item name '{k}' for type {entry_point_name}: {v} -- {result_entrypoints[k]}."
            try:
                if type_id_key not in v.__dict__.keys():
                    msg = f"{msg} Most likely the name is picked up from a subclass, try to add a '{type_id_key}' class attribute to your implementing class, with the name you want to give your type as value."
            except Exception:
                pass

            raise Exception(msg)
        result_entrypoints[k] = v

    return result_entrypoints


def find_all_kiara_modules() -> Dict[str, Type["KiaraModule"]]:
    """
    Find all [KiaraModule][kiara.module.KiaraModule] subclasses via package entry points.

    Todo:
    ----
    """
    from kiara.modules import KiaraModule

    modules = load_all_subclasses_for_entry_point(
        entry_point_name="kiara.modules",
        base_class=KiaraModule,  # type: ignore
        type_id_key="_module_type_name",
        attach_python_metadata=True,
    )

    result = {}
    # need to test this, since I couldn't add an abstract method to the KiaraModule class itself (mypy complained because it is potentially overloaded)
    for k, cls in modules.items():

        if not hasattr(cls, "process"):
            if is_develop():
                from rich.markdown import Markdown

                msg = f"Invalid kiara module: **{cls.__module__}.{cls.__name__}**\n\nMissing method(s):\n- *process*"
                from kiara.utils.develop import log_dev_message

                log_dev_message(msg=Markdown(msg))

            # TODO: check signature of process method
            log_message(
                "ignore.subclass",
                sub_class=cls,
                base_class=KiaraModule,
                reason="'process' method is missing",
            )
            continue

        result[k] = cls
    return result


def find_all_kiara_model_classes() -> Dict[str, Type["KiaraModel"]]:
    """
    Find all [KiaraModule][kiara.module.KiaraModule] subclasses via package entry points.

    Todo:
    ----
    """
    from kiara.models import KiaraModel

    return load_all_subclasses_for_entry_point(
        entry_point_name="kiara.model_classes",
        base_class=KiaraModel,  # type: ignore
        type_id_key="_kiara_model_id",
        type_id_func=_cls_name_id_func,
        attach_python_metadata=False,
    )


# def find_all_value_metadata_models() -> Dict[str, Type["ValueMetadata"]]:
#     """Find all [KiaraModule][kiara.module.KiaraModule] subclasses via package entry points.
#
#     TODO
#     """
#
#     from kiara.models.values.value_metadata import ValueMetadata
#
#     return load_all_subclasses_for_entry_point(
#         entry_point_name="kiara.metadata_models",
#         base_class=ValueMetadata,  # type: ignore
#         type_id_key="_metadata_key",
#         type_id_func=_cls_name_id_func,
#         attach_python_metadata=False,
#     )


@functools.lru_cache(maxsize=1)
def find_all_archive_types() -> Dict[str, Type["KiaraArchive"]]:
    """Find all [KiaraArchive][kiara.registries.KiaraArchive] subclasses via package entry points."""
    from kiara.registries import KiaraArchive

    return load_all_subclasses_for_entry_point(
        entry_point_name="kiara.archive_type",
        base_class=KiaraArchive,  # type: ignore
        type_id_key="_archive_type_name",
        type_id_func=_cls_name_id_func,
        attach_python_metadata=False,
    )


def find_all_data_types() -> Dict[str, Type["DataType"]]:
    """
    Find all [KiaraModule][kiara.module.KiaraModule] subclasses via package entry points.

    Todo:
    ----
    """
    from kiara.data_types import DataType

    all_data_types = load_all_subclasses_for_entry_point(
        entry_point_name="kiara.data_types",
        base_class=DataType,  # type: ignore
        type_id_key="_data_type_name",
        type_id_func=_cls_name_id_func,
    )

    invalid = [x for x in all_data_types.keys() if "." in x]
    if invalid:
        raise Exception(
            f"Invalid value type name(s), type names can't contain '.': {', '.join(invalid)}"
        )

    return all_data_types


def find_all_operation_types() -> Dict[str, Type["OperationType"]]:

    from kiara.operations import OperationType

    result = load_all_subclasses_for_entry_point(
        entry_point_name="kiara.operation_types",
        base_class=OperationType,  # type: ignore
        type_id_key="_operation_type_name",
    )
    return result


def find_kiara_modules_under(
    module: Union[str, ModuleType],
) -> List[Type["KiaraModule"]]:

    from kiara.modules import KiaraModule

    return find_subclasses_under(
        base_class=KiaraModule,  # type: ignore
        python_module=module,
    )


def find_kiara_model_classes_under(
    module: Union[str, ModuleType]
) -> List[Type["KiaraModel"]]:

    from kiara.models import KiaraModel

    result = find_subclasses_under(
        base_class=KiaraModel,  # type: ignore
        python_module=module,
    )

    return result


# def find_value_metadata_models_under(
#     module: Union[str, ModuleType]
# ) -> List[Type["ValueMetadata"]]:
#
#     from kiara.models.values.value_metadata import ValueMetadata
#
#     result = find_subclasses_under(
#         base_class=ValueMetadata,  # type: ignore
#         python_module=module,
#     )
#
#     return result


def find_data_types_under(module: Union[str, ModuleType]) -> List[Type["DataType"]]:

    from kiara.data_types import DataType

    return find_subclasses_under(
        base_class=DataType,  # type: ignore
        python_module=module,
    )


def find_operations_under(
    module: Union[str, ModuleType]
) -> List[Type["OperationType"]]:

    from kiara.operations import OperationType

    return find_subclasses_under(
        base_class=OperationType,  # type: ignore
        python_module=module,
    )


def find_pipeline_base_path_for_module(
    module: Union[str, ModuleType]
) -> Union[str, None]:

    # if hasattr(sys, "frozen"):
    #     raise NotImplementedError("Pyinstaller bundling not supported yet.")

    if isinstance(module, str):
        module = importlib.import_module(module)

    module_file = module.__file__
    assert module_file is not None
    path = os.path.dirname(module_file)

    if not os.path.exists(path):
        log_message("ignore.pipeline_folder", path=path, reason="folder does not exist")
        return None

    return path


def find_all_kiara_pipeline_paths(
    skip_errors: bool = False,
) -> Dict[str, Union[Dict[str, Any], None]]:

    import logging

    log2 = logging.getLogger("stevedore")
    out_hdlr = logging.StreamHandler(sys.stdout)
    out_hdlr.setFormatter(
        logging.Formatter("kiara pipeline search plugin error -> %(message)s")
    )
    out_hdlr.setLevel(logging.INFO)
    log2.addHandler(out_hdlr)
    log2.setLevel(logging.INFO)

    log_message("events.loading.pipelines")

    from stevedore import ExtensionManager

    mgr = ExtensionManager(
        namespace="kiara.pipelines", invoke_on_load=False, propagate_map_exceptions=True
    )

    paths: Dict[str, Union[Dict[str, Any], None]] = {}
    # TODO: make sure we load 'core' first?
    for plugin in mgr:

        name = plugin.name
        if (
            isinstance(plugin.plugin, tuple)
            and len(plugin.plugin) >= 1
            and callable(plugin.plugin[0])
        ) or callable(plugin.plugin):
            try:
                if callable(plugin.plugin):
                    func = plugin.plugin
                    args = []
                else:
                    func = plugin.plugin[0]
                    args = plugin.plugin[1:]

                f_args = []
                metadata: Union[Dict[str, Any], None] = None
                if len(args) >= 1:
                    f_args.append(args[0])
                if len(args) >= 2:
                    metadata = args[1]
                    assert isinstance(metadata, Mapping)
                if len(args) > 3:
                    logger.debug(
                        "ignore.pipeline_lookup_arguments",
                        reason="more than 2 arguments provided",
                        surplus_args=args[2:],
                        path=f_args[0],
                    )

                result = func(f_args[0])
                if not result:
                    continue
                if isinstance(result, str):
                    paths[result] = metadata
                else:
                    for path in paths:
                        assert path not in paths.keys()
                        paths[path] = metadata

            except Exception as e:
                log_exception(e)
                if skip_errors:
                    log_message(
                        "ignore.pipline_entrypoint", entrypoint_name=name, reason=str(e)
                    )
                    continue
                raise Exception(f"Error trying to load plugin '{plugin.plugin}': {e}")
        else:
            if skip_errors:
                log_message(
                    "ignore.pipline_entrypoint",
                    entrypoint_name=name,
                    reason=f"invalid plugin type '{type(plugin.plugin)}'",
                )
                continue
            msg = f"Can't load pipelines for entrypoint '{name}': invalid plugin type '{type(plugin.plugin)}'"
            raise Exception(msg)

    return paths


def find_all_cli_subcommands() -> Iterable["Command"]:

    entry_point_name = "kiara.cli_subcommands"
    log2 = logging.getLogger("stevedore")
    out_hdlr = logging.StreamHandler(sys.stdout)
    out_hdlr.setFormatter(
        logging.Formatter(
            f"{entry_point_name} plugin search message/error -> %(message)s"
        )
    )
    out_hdlr.setLevel(logging.INFO)
    log2.addHandler(out_hdlr)
    if is_debug():
        log2.setLevel(logging.DEBUG)
    else:
        out_hdlr.setLevel(logging.INFO)
        log2.setLevel(logging.INFO)

    log_message("events.loading.entry_points", entry_point_name=entry_point_name)
    from stevedore import ExtensionManager

    mgr = ExtensionManager(
        namespace=entry_point_name,
        invoke_on_load=False,
        propagate_map_exceptions=True,
    )

    return [plugin.plugin for plugin in mgr]


def find_all_kiara_renderers() -> Dict[str, Type["KiaraRenderer"]]:
    """
    Find all [KiaraComponent][kiara_plugin.streamilt.components.KiaraComponent] subclasses via package entry points.

    Todo:
    ----
    """
    from kiara.renderers import KiaraRenderer

    components = load_all_subclasses_for_entry_point(
        entry_point_name="kiara.renderers",
        base_class=KiaraRenderer,  # type: ignore
        type_id_key="_renderer_name",
        type_id_func=_cls_name_id_func,
        attach_python_metadata=True,
    )

    return components


def find_kiara_renderers_under(
    module: Union[str, ModuleType],
) -> List[Type["KiaraRenderer"]]:

    from kiara.renderers import KiaraRenderer

    return find_subclasses_under(
        base_class=KiaraRenderer,  # type: ignore
        python_module=module,
    )


# def _find_pipeline_folders_using_callable(
#     func: Union[Callable, Tuple]
# ) -> Tuple[Optional[str], str]:
#
#     if not callable(func):
#         assert len(func) >= 2
#         args = func[1]
#         assert len(args) == 1
#         module_path: Optional[str] = args[0]
#     else:
#         module_path = None
#     path = _callable_wrapper(func=func)  # type: ignore
#     assert isinstance(path, str)
#     return (module_path, path)


# def _find_kiara_modules_using_callable(
#     func: typing.Union[typing.Callable, typing.Tuple]
# ) -> typing.Mapping[str, typing.Type[KiaraModule]]:
#
#     # TODO: typecheck?
#     return _callable_wrapper(func=func)  # type: ignore
def _import_modules_recursively(module: "ModuleType"):

    if not hasattr(module, "__path__"):
        return

    for submodule in iter_modules(module.__path__):  # type: ignore

        try:
            submodule_mod = importlib.import_module(
                f"{module.__name__}.{submodule.name}"
            )
            if hasattr(submodule_mod, "__path__"):
                _import_modules_recursively(submodule_mod)
        except Exception as e:
            logger.error(
                "ignore.python_module",
                module=f"{module.__name__}.{submodule.name}",
                reason=str(e),
                base_module=str(module),
            )
