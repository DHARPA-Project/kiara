# -*- coding: utf-8 -*-

#  Copyright (c) 2020-2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License Version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

"""Main module for code that helps with documentation auto-generation in supported projects."""

import os
import tempfile
import urllib.parse

import mkdocs.utils
from mkdocs.config import Config, config_options  # noqa
from mkdocs.plugins import BasePlugin
from mkdocs.structure.files import File, Files
from mkdocs.structure.nav import (
    Navigation,
    Section,
    _add_parent_links,
    _add_previous_and_next_links,
)
from mkdocs.structure.pages import Page

from kiara.doc.generate_api_doc import gen_pages_for_module


class FrklDocumentationPlugin(BasePlugin):
    """
    [mkdocs](https://www.mkdocs.org/) plugin to render API documentation for a project.

    To add to a project, add this to the 'plugins' section of a mkdocs config file:

    ```yaml
    - frkl-docgen:
        main_module: "module_name"
    ```

    This will add an ``API reference`` navigation item to your page navigation, with auto-generated entries for every
    Python module in your package.
    """

    config_scheme = (("main_module", mkdocs.config.config_options.Type(str)),)

    def __init__(self):
        self._doc_paths = None
        self._dir = tempfile.TemporaryDirectory(prefix="frkl_doc_gen_")
        self._doc_files = None
        super().__init__()

    def on_files(self, files: Files, config: Config) -> Files:

        self._doc_paths = gen_pages_for_module(self.config["main_module"])
        self._doc_files = {}

        for k in sorted(self._doc_paths, key=lambda x: os.path.splitext(x)[0]):
            content = self._doc_paths[k]["content"]
            _file = File(
                k,
                src_dir=self._dir.name,
                dest_dir=config["site_dir"],
                use_directory_urls=config["use_directory_urls"],
            )

            os.makedirs(os.path.dirname(_file.abs_src_path), exist_ok=True)  # type: ignore

            with open(_file.abs_src_path, "w") as f:  # type: ignore
                f.write(content)

            self._doc_files[k] = _file
            files.append(_file)

        return files

    def on_page_content(self, html, page: Page, config: Config, files: Files):

        repo_url = config.get("repo_url", None)
        python_src = config.get("edit_uri", None)

        if page.file.src_path in self._doc_paths.keys():
            src_path = self._doc_paths.get(page.file.src_path)["python_src"]["rel_path"]
            rel_base = urllib.parse.urljoin(repo_url, f"{python_src}/../src/{src_path}")
            page.edit_url = rel_base

        return html

    def on_nav(self, nav: Navigation, config: Config, files: Files):

        for item in nav.items:
            if item.title and "Api reference" in item.title:
                return nav

        pages = []
        for _file in self._doc_files.values():
            pages.append(_file.page)

        section = Section(title="API reference", children=pages)
        nav.items.append(section)
        nav.pages.extend(pages)

        _add_previous_and_next_links(nav.pages)
        _add_parent_links(nav.items)

        return nav

    def on_post_build(self, config: Config):

        self._dir.cleanup()
