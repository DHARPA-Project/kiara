# -*- coding: utf-8 -*-

#  Copyright (c) 2020-2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


import importlib
import os
import typing
from pathlib import Path
from types import ModuleType


def gen_pages_for_module(
    module: typing.Union[str, ModuleType], prefix: str = "api_reference"
):
    """Generate modules for a set of modules (using the [mkdocstring](https://github.com/mkdocstrings/mkdocstrings) package."""
    result = {}
    modules_info = get_source_tree(module)
    for module_name, path in modules_info.items():
        page_name = module_name

        if page_name.endswith("__init__"):
            page_name = page_name[0:-9]
        if page_name.endswith("._frkl"):
            continue

        doc_path = f"{prefix}{os.path.sep}{page_name}.md"
        p = Path("..", path["abs_path"])
        if not p.read_text().strip():
            continue

        main_module = path["main_module"]
        if page_name == main_module:
            title = page_name
        else:
            title = page_name.replace(f"{main_module}.", "➜ ")  # noqa

        result[doc_path] = {
            "python_src": path,
            "content": f"---\ntitle: {title}\n---\n# {page_name}\n\n::: {module_name}",
        }

    return result


def get_source_tree(module: typing.Union[str, ModuleType]):
    """Find all python source files for a module."""
    if isinstance(module, str):
        module = importlib.import_module(module)

    if not isinstance(module, ModuleType):
        raise TypeError(
            f"Invalid type '{type(module)}', input needs to be a string or module."
        )

    module_file = module.__file__
    assert module_file is not None
    module_root = os.path.dirname(module_file)
    module_name = module.__name__

    src = {}

    for path in Path(module_root).glob("**/*.py"):
        rel = os.path.relpath(path, module_root)
        mod_name = f"{module_name}.{rel[0:-3].replace(os.path.sep, '.')}"
        rel_path = f"{module_name}{os.path.sep}{rel}"
        src[mod_name] = {
            "rel_path": rel_path,
            "abs_path": path,
            "main_module": module_name,
        }

    return src


def gen_api_doc_pages(base_path: typing.Union[str, Path]):
    """Generate the mkdocs code reference pages and navigation."""

    import mkdocs_gen_files

    if isinstance(base_path, str):
        base_path = Path(base_path)

    nav = mkdocs_gen_files.Nav()

    for path in sorted(base_path.rglob("*.py")):
        if f"{os.path.sep}resources{os.path.sep}" in path.as_posix():
            continue
        module_path = path.relative_to(base_path).with_suffix("")
        doc_path = path.relative_to(base_path).with_suffix(".md")
        full_doc_path = Path("reference", doc_path)

        parts = list(module_path.parts)

        if parts[-1] == "__init__":
            parts = parts[:-1]
        elif parts[-1] == "__main__":
            continue

        nav[parts] = doc_path

        with mkdocs_gen_files.open(full_doc_path, "w") as fd:
            ident = ".".join(parts)
            print("::: " + ident, file=fd)

        mkdocs_gen_files.set_edit_path(full_doc_path, path)

    with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
        nav_file.writelines(nav.build_literate_nav())


def create_info_pages(pkg_name: str):
    import builtins

    from kiara.context import KiaraContextInfo
    from kiara.doc.gen_info_pages import generate_detail_pages
    from kiara.interfaces.python_api.kiara_api import KiaraAPI

    kiara: KiaraAPI = KiaraAPI.instance()
    context_info = KiaraContextInfo.create_from_kiara_instance(
        kiara=kiara._api.context, package_filter=pkg_name
    )

    generate_detail_pages(context_info=context_info)

    builtins.plugin_package_context_info = context_info  # type: ignore
