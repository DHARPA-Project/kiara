# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import mkdocs_gen_files
import os

from kiara.defaults import KIARA_RESOURCES_FOLDER
from kiara.kiara import KiaraContextInfo
from kiara.models.info import InfoModelGroup

_jinja_env = None


def get_jina_env():

    global _jinja_env
    if _jinja_env is None:
        from jinja2 import Environment, FileSystemLoader

        _jinja_env = Environment(
            loader=FileSystemLoader(
                os.path.join(KIARA_RESOURCES_FOLDER, "templates", "doc_gen"),
                encoding="utf8",
            )
        )
    return _jinja_env


def render_item_listing(item_type: str, items: InfoModelGroup):

    list_template = get_jina_env().get_template("info_listing.j2")

    render_args = {"items": items.get_type_infos(), "item_type": item_type}

    rendered = list_template.render(**render_args)
    path = f"info/{item_type}.md"
    with mkdocs_gen_files.open(path, "w") as f:
        f.write(rendered)

    return path


def generate_detail_pages(context_info: KiaraContextInfo):

    pages = {}
    summary = []

    all_info = context_info.get_all_info(skip_empty_types=True)

    for item_type, items_info in all_info.items():
        summary.append(f"- [{item_type}]({item_type}.md)")
        path = render_item_listing(item_type=item_type, items=items_info)
        pages[item_type] = path

    if summary:
        with mkdocs_gen_files.open("info/SUMMARY.md", "w") as f:
            f.write("\n".join(summary))

    return pages
