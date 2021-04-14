# -*- coding: utf-8 -*-
import inspect
import mkdocs_gen_files
import os
import typing

from kiara.doc.mkdocs_macros_kiara import KIARA_MODEL_CLASSES


def class_namespace(cls: typing.Type):

    module = cls.__module__
    if module is None or module == str.__class__.__module__:
        return cls.__name__
    else:
        return module + "." + cls.__name__


overview_file_path = os.path.join("development", "entities", "index.md")
overview = """# Schemas overviews

This page contains an overview of the available models and their associated schemas used in *kiara*.

"""

for category, classes in KIARA_MODEL_CLASSES.items():

    overview = overview + f"## {category.capitalize()}\n\n"

    file_path = os.path.join("development", "entities", f"{category}.md")

    content = f"# {category.capitalize()}\n\n"

    for cls in classes:

        doc = cls.__doc__

        if doc is None:
            doc = ""

        doc = inspect.cleandoc(doc)

        doc_short = doc.split("\n")[0]
        if doc_short:
            doc_str = f": {doc_short}"
        else:
            doc_str = ""

        overview = (
            overview
            + f"  - [``{cls.__name__}``]({category}{os.path.sep}#{cls.__name__.lower()}){doc_str}\n"
        )

        namescace = class_namespace(cls)
        download_link = f'<a href="{cls.__name__}.json">{cls.__name__}.json</a>'

        # content = content + f"## {cls.__name__}\n\n" + "{{ get_schema_for_model('" + class_namespace(cls) + ") }}\n\n"
        content = content + f"## {cls.__name__}\n\n"
        content = content + doc + "\n\n"
        content = content + "#### References\n\n"
        content = (
            content + f"  - model class reference: [{cls.__name__}][{namescace}]\n"
        )
        content = content + f"  - JSON schema file: {download_link}\n\n"
        content = content + "#### JSON schema\n\n"
        content = (
            content
            + "``` json\n{{ get_schema_for_model('"
            + namescace
            + "') }}\n```\n\n"
        )

    with mkdocs_gen_files.open(file_path, "w") as f:
        f.write(content)

with mkdocs_gen_files.open(overview_file_path, "w") as f:
    f.write(overview)
