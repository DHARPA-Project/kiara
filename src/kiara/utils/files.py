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


def unpack_archive(
    archive_file: str, out_dir: str, autodetect_file_type: bool = False
) -> None:

    if autodetect_file_type:
        raise NotImplementedError("Autodetecting file type is not implemented yet.")
        # import puremagic
        # type_matches = puremagic.magic_file(archive_file)
        #
        # for type_match in type_matches:
        #     print("----")
        #     dbg(type_match._asdict())
        #     if type_match.confidence >= 0.6:

    error = None
    try:
        import shutil

        shutil.unpack_archive(archive_file, out_dir)
    except Exception:
        # try patool, maybe we're lucky
        try:
            import patoolib

            patoolib.extract_archive(archive_file, outdir=out_dir)
        except Exception as e:
            error = e

    if error is not None:
        if not autodetect_file_type:
            unpack_archive(archive_file, out_dir, autodetect_file_type=True)
        else:
            raise KiaraException(msg=f"Could not extract archive: {error}.")
