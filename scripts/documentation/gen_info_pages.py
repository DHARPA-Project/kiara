# -*- coding: utf-8 -*-
import mkdocs_gen_files

from kiara import Kiara
from kiara.doc.gen_info_pages import generate_pages_and_summary_for_types

kiara = Kiara.instance()

types = ["value_type", "module", "operation_type"]

type_details = generate_pages_and_summary_for_types(kiara=kiara, types=types)

summary_content = []
for name, details in type_details.items():
    line = f"* [{details['name']}]({details['path']})"
    summary_content.append(line)


nav = [
    "* [Home](index.md)",
    "* [Install](install.md)",
    "* [Architecture](architecture/)",
]
nav.extend(summary_content)

nav.append("* [API docs](reference/)")

with mkdocs_gen_files.open("SUMMARY.md", "w") as f:
    f.write("\n".join(nav))
