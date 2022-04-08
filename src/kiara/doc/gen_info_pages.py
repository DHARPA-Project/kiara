# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import mkdocs_gen_files
import os
from typing import Any, Dict

from kiara.defaults import KIARA_RESOURCES_FOLDER

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


def render_item_listing(item_type: str, items: Dict[str, Any]):

    list_template = get_jina_env().get_template("info_listing.j2")

    render_args = {"items": items, "item_type": item_type}

    rendered = list_template.render(**render_args)
    path = f"info/{item_type}.md"
    with mkdocs_gen_files.open(path, "w") as f:
        f.write(rendered)

    return path


def generate_detail_pages(
    type_item_details: Dict[str, Dict[str, Any]],
):

    pages = {}
    summary = []
    for t, details in type_item_details.items():
        summary.append(f"- [{t}]({t}.md)")
        path = render_item_listing(item_type=t, items=details)
        pages[t] = path

    dbg(summary)
    with mkdocs_gen_files.open("info/SUMMARY.md", "w") as f:
        f.write("\n".join(summary))

    return pages
