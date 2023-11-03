# -*- coding: utf-8 -*-
from typing import Any, Mapping, Union

import httpx
from ruamel.yaml import YAML

yaml = YAML(typ="safe")


def get_data_from_url(
    url: str, content_type: Union[str, None] = None
) -> Mapping[str, Any]:

    if content_type:
        assert content_type in ["json", "yaml"]

    r = httpx.get(url, follow_redirects=True)

    if not content_type:
        if url.endswith(".json"):
            content_type = "json"
        elif url.endswith(".yaml") or url.endswith(".yml"):
            content_type = "yaml"

    if content_type == "json":
        result = r.json()
    elif content_type == "yaml":
        result = yaml.load(r.text)
    else:
        try:
            result = r.json()
        except Exception:
            try:
                result = yaml.load(r.text)
            except Exception:
                raise ValueError(f"Can't parse data from url '{url}'")

    if not isinstance(result, Mapping):
        raise ValueError(f"Data from url '{url}' is not a Mapping")
    return result
