# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import os
from pathlib import Path
from typing import Any, Iterable, List, Mapping, Optional, Union

from kiara.defaults import MODULE_TYPE_NAME_KEY
from kiara.models.module.pipeline.value_refs import StepValueAddress
from kiara.utils import get_data_from_file


def create_step_value_address(
    value_address_config: Union[str, Mapping[str, Any]],
    default_field_name: str,
) -> "StepValueAddress":

    if isinstance(value_address_config, StepValueAddress):
        return value_address_config

    sub_value: Optional[Mapping[str, Any]] = None

    if isinstance(value_address_config, str):

        tokens = value_address_config.split(".")
        if len(tokens) == 1:
            step_id = value_address_config
            output_name = default_field_name
        elif len(tokens) == 2:
            step_id = tokens[0]
            output_name = tokens[1]
        elif len(tokens) == 3:
            step_id = tokens[0]
            output_name = tokens[1]
            sub_value = {"config": tokens[2]}
        else:
            raise NotImplementedError()

    elif isinstance(value_address_config, Mapping):

        step_id = value_address_config["step_id"]
        output_name = value_address_config["value_name"]
        sub_value = value_address_config.get("sub_value", None)
    else:
        raise TypeError(
            f"Invalid type for creating step value address: {type(value_address_config)}"
        )

    if sub_value is not None and not isinstance(sub_value, Mapping):
        raise ValueError(
            f"Invalid type '{type(sub_value)}' for sub_value (step_id: {step_id}, value name: {output_name}): {sub_value}"
        )

    input_link = StepValueAddress(
        step_id=step_id, value_name=output_name, sub_value=sub_value
    )
    return input_link


def ensure_step_value_addresses(
    link: Union[str, Mapping, Iterable], default_field_name: str
) -> List["StepValueAddress"]:

    if isinstance(link, (str, Mapping)):
        input_links: List[StepValueAddress] = [
            create_step_value_address(
                value_address_config=link, default_field_name=default_field_name
            )
        ]

    elif isinstance(link, Iterable):
        input_links = []
        for o in link:
            il = create_step_value_address(
                value_address_config=o, default_field_name=default_field_name
            )
            input_links.append(il)
    else:
        raise TypeError(f"Can't parse input map, invalid type for output: {link}")

    return input_links


def get_pipeline_details_from_path(
    path: Union[str, Path],
    module_type_name: Optional[str] = None,
    base_module: Optional[str] = None,
) -> Mapping[str, Any]:
    """Load a pipeline description, save it's content, and determine it the pipeline base name.

    Arguments:
        path: the path to the pipeline file
        module_type_name: if specifies, overwrites any auto-detected or assigned pipeline name
        base_module: overrides the base module the assembled pipeline module will be located in the python hierarchy

    """

    if isinstance(path, str):
        path = Path(os.path.expanduser(path))

    if not path.is_file():
        raise Exception(
            f"Can't add pipeline description '{path.as_posix()}': not a file"
        )

    data = get_data_from_file(path)

    if not data:
        raise Exception(
            f"Can't register pipeline file '{path.as_posix()}': no content."
        )

    if module_type_name:
        data[MODULE_TYPE_NAME_KEY] = module_type_name

    if not isinstance(data, Mapping):
        raise Exception("Not a dictionary type.")

    # filename = path.name
    # name = data.get(MODULE_TYPE_NAME_KEY, None)
    # if name is None:
    #     name = filename.split(".", maxsplit=1)[0]

    result = {"data": data, "source": path.as_posix(), "source_type": "file"}
    if base_module:
        result["base_module"] = base_module
    return result


def check_doc_sidecar(
    path: Union[Path, str], data: Mapping[str, Any]
) -> Mapping[str, Any]:

    if isinstance(path, str):
        path = Path(os.path.expanduser(path))

    _doc = data["data"].get("documentation", None)
    if _doc is None:
        _doc_path = Path(path.as_posix() + ".md")
        if _doc_path.is_file():
            doc = _doc_path.read_text()
            if doc:
                data["data"]["documentation"] = doc

    return data
