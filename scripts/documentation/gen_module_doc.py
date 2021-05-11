# -*- coding: utf-8 -*-
import mkdocs_gen_files
import os
import typing

from kiara import Kiara

kiara = Kiara.instance()
kiara.available_module_types


def class_namespace(cls: typing.Type):

    module = cls.__module__
    if module is None or module == str.__class__.__module__:
        return cls.__name__
    else:
        return module + "." + cls.__name__


page_file_path = os.path.join("development", "modules_list.md")
page_content = """# Available module types

This page contains a list of all available *Kiara* module types, and their details.

!!! note
The formatting here will be improved later on, for now this should be enough to get the important details of each module type.

"""


for module_type in kiara.available_module_types:

    if module_type == "pipeline":
        continue

    page_content = page_content + f"## ``{module_type}``\n\n"
    page_content = (
        page_content + "```\n{{ get_module_info('" + module_type + "') }}\n```\n\n"
    )

with mkdocs_gen_files.open(page_file_path, "w") as f:
    f.write(page_content)
