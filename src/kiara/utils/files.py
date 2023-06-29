# -*- coding: utf-8 -*-
import json
import os
from pathlib import Path
from typing import Any, Union

from ruamel.yaml import YAML

from kiara.exceptions import KiaraException

yaml = YAML(typ="safe")


def get_data_from_file(
    path: Union[str, Path], content_type: Union[str, None] = None
) -> Any:

    if isinstance(path, str):
        path = Path(os.path.expanduser(path))

    if not path.exists():
        raise KiaraException(f"File not found: {path}")

    if not path.is_file():
        raise KiaraException(f"Path is not a file: {path}")

    content = path.read_text()

    if not content_type:
        if path.name.endswith(".json"):
            content_type = "json"
        elif path.name.endswith(".yaml") or path.name.endswith(".yml"):
            content_type = "yaml"

    if content_type:

        if content_type not in ["json", "yaml"]:
            raise KiaraException(
                "Invalid content type, only 'json' or 'yaml' are supported currently."
            )

        if content_type == "json":
            data = json.loads(content)
        else:
            data = yaml.load(content)
    else:
        try:
            data = json.loads(content)
        except Exception:
            try:
                data = yaml.load(content)
            except Exception:
                raise ValueError("Could not determine data format from file extension.")

    return data
