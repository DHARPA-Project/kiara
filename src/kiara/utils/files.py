# -*- coding: utf-8 -*-
import json
import os
from pathlib import Path
from ruamel.yaml import YAML
from typing import Any, Union

yaml = YAML(typ="safe")


def get_data_from_file(
    path: Union[str, Path], content_type: Union[str, None] = None
) -> Any:

    if isinstance(path, str):
        path = Path(os.path.expanduser(path))

    content = path.read_text()

    if content_type:
        assert content_type in ["json", "yaml"]
    else:
        if path.name.endswith(".json"):
            content_type = "json"
        elif path.name.endswith(".yaml") or path.name.endswith(".yml"):
            content_type = "yaml"
        else:
            raise ValueError(
                "Invalid data format, only 'json' or 'yaml' are supported currently."
            )

    if content_type == "json":
        data = json.loads(content)
    else:
        data = yaml.load(content)

    return data
