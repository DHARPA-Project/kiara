# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import mkdocs_gen_files
import os
import typing

from kiara import Kiara
from kiara.defaults import KIARA_RESOURCES_FOLDER
from kiara.info import KiaraDynamicInfoModel, KiaraInfoModel
from kiara.info.kiara import KiaraContext
from kiara.metadata.core_models import ContextMetadataModel
from kiara.metadata.module_models import KiaraModuleTypeMetadata
from kiara.metadata.operation_models import OperationsMetadata
from kiara.metadata.type_models import ValueTypeMetadata
from kiara.operations import Operation

_jinja_env = None


def get_jina_env():

    global _jinja_env
    if _jinja_env is None:
        from jinja2 import Environment, FileSystemLoader

        _jinja_env = Environment(
            loader=FileSystemLoader(
                os.path.join(KIARA_RESOURCES_FOLDER, "templates", "listings"),
                encoding="utf8",
            )
        )
    return _jinja_env


def render_item_listing(
    kiara: Kiara, item_type: str, limit_to_package: typing.Optional[str] = None
) -> typing.Optional[str]:
    """Render an item listing summary page, for: https://oprypin.github.io/mkdocs-literate-nav/

    This code is a terrible, terrible mess, but I just don't care enough. If the output it produces is wrong, it'll be
    obvious in the documentation (hopefully).
    I'd have to spend considerable time cleaning this up, and at the moment it does not seem worth it.
    """

    info: KiaraInfoModel = KiaraContext.get_info(kiara=kiara)
    tree = info.get_subcomponent_tree()
    if tree is None:
        raise Exception("Can't render item listing, no subcomponent tree available.")

    def extract_cls_from_kiara_module_type_metadata(obj):
        return obj.python_class.get_class()

    def extract_cls_from_operation(obj):
        return obj.module.__class__

    def extract_cls_from_op_type(obj):
        return obj.python_class.get_class()

    def extract_cls_from_value_type(obj):
        return obj.python_class.get_class()

    item_type_map: typing.Dict[str, typing.Dict[str, typing.Any]] = {
        "module": {
            "cls": KiaraModuleTypeMetadata,
            "extract": extract_cls_from_kiara_module_type_metadata,
        },
        "pipeline": {
            "cls": KiaraModuleTypeMetadata,
            "extract": extract_cls_from_kiara_module_type_metadata,
        },
        "type": {
            "cls": ValueTypeMetadata,
            "extract": extract_cls_from_value_type,
        },
        "operation": {"cls": Operation, "extract": extract_cls_from_operation},
        "operation_type": {
            "cls": OperationsMetadata,
            "extract": extract_cls_from_op_type,
        },
    }
    item_cls = item_type_map[item_type]["cls"]
    plural = f"{item_type}s"

    item_summaries: typing.Dict[typing.Tuple, typing.List] = {}
    list_template = get_jina_env().get_template(f"{item_type}_list.md.j2")

    for node in tree.nodes():
        tokens = node.split(".")
        if len(tokens) < 3:
            continue
        category = tokens[1]
        obj = tree.nodes[node]["obj"]

        path_tokens = tokens[2:]

        if category == plural:

            if isinstance(obj, item_cls):  # type: ignore
                if limit_to_package:
                    cls = item_type_map[item_type]["extract"](obj)
                    md = ContextMetadataModel.from_class(cls)
                    if md.labels.get("package", None) != limit_to_package:
                        continue
                item_summaries.setdefault(tuple(path_tokens), []).append(obj)
            elif isinstance(obj, KiaraDynamicInfoModel):
                item_summaries.setdefault(tuple(path_tokens), [])

    new_summary: typing.Dict[typing.Tuple, typing.List] = {}
    no_childs = []
    for path_tokens, items in item_summaries.items():
        # if len(path_tokens) == 1:
        #     new_summary[path_tokens] = item_summaries[path_tokens]

        # full_path = [path_tokens[0]]
        full_path: typing.List[str] = []
        collect: typing.Optional[typing.List[str]] = None
        for token in path_tokens:

            if collect is not None:
                collect.append(token)
                t = tuple(full_path + collect)
                if not item_summaries[t]:
                    continue
                else:
                    new_summary.setdefault(tuple(full_path), []).extend(
                        item_summaries[t]
                    )
            else:
                full_path.append(token)
                t = tuple(full_path)
                if item_summaries[t]:
                    new_summary[t] = item_summaries[t]
                else:
                    match = False
                    any_childs = False
                    for k in item_summaries.keys():
                        if len(full_path) == 1 and full_path[0] == k[0] and len(k) > 2:
                            match = True

                        if (
                            len(k) > 1
                            and len(t) <= len(k)
                            and k[0 : len(t)] == t  # noqa
                            and item_summaries[k]
                        ):
                            any_childs = True

                    if not any_childs:
                        no_childs.append(t)

                    if not match:
                        new_summary.setdefault(t, [])
                        collect = []
                    else:
                        new_summary.setdefault(t, []).extend(item_summaries[t])

    main_summary = []

    for summary_path, items in new_summary.items():

        if not summary_path:
            continue

        match = False
        for nc in no_childs:
            if len(summary_path) >= len(nc):
                if summary_path[0 : len(nc)] == nc:  # noqa
                    match = True
                    break
        if match:
            continue

        padding = "  " * len(summary_path)
        path = os.path.join(*summary_path)

        rendered = list_template.render(**{"path": path, plural: items})
        p_write = os.path.join(plural, path, "index.md")
        p_index = os.path.join(path, "index.md")

        with mkdocs_gen_files.open(p_write, "w") as f:
            f.write(rendered)

        main_summary.append(f"{padding}* [{summary_path[-1]}]({p_index})")

    modules_content = "xxxxxxx"
    with mkdocs_gen_files.open(f"{plural}/index.md", "w") as f:
        f.write(modules_content)

    summary_content = "\n".join(main_summary)

    summary_page = f"{plural}/SUMMARY.md"
    with mkdocs_gen_files.open(summary_page, "w") as f:
        f.write(summary_content)

    return f"{plural}/"


TYPE_ALIAS_MAP = {
    "type": {"name": "ValueOrm value_types"},
    "module": {"name": "Modules"},
    "pipeline": {"name": "Pipelines"},
    "operation_type": {"name": "Operation value_types"},
}


def generate_pages_and_summary_for_types(
    kiara: Kiara,
    types: typing.Optional[typing.Iterable[str]] = None,
    limit_to_package: typing.Optional[str] = None,
) -> typing.Dict[str, typing.Dict[str, typing.Any]]:

    if types is None:
        types = ["type", "module", "operation_type"]

    type_details: typing.Dict[str, typing.Dict[str, typing.Any]] = {
        t: dict(TYPE_ALIAS_MAP[t]) for t in types
    }

    for t, details in type_details.items():
        result = render_item_listing(
            kiara=kiara, item_type=t, limit_to_package=limit_to_package
        )
        type_details[t]["path"] = result

    return type_details
