# -*- coding: utf-8 -*-
from pathlib import Path
from typing import TYPE_CHECKING, Union

from kiara.defaults import KIARA_CONFIG_FILE_NAME, KIARA_MAIN_CONFIG_FILE
from kiara.exceptions import KiaraException

if TYPE_CHECKING:
    from kiara.context import KiaraConfig


def assemble_kiara_config(
    config_file: Union[str, None] = None, create_config_file: bool = False
) -> "KiaraConfig":
    """Assemble a KiaraConfig object from a config file path or create a new one.



    Arguments:
        config_file: The path to a Kiara config file or a folder containing one named 'kiara.config'.
        create_config_file: If True, create a new config file if it does not exist.

    """

    exists = False
    if config_file:
        config_path = Path(config_file)
        if config_path.exists():
            if config_path.is_file():
                config_file_path = config_path
                exists = True
            else:
                config_file_path = config_path / KIARA_CONFIG_FILE_NAME
                if config_file_path.exists():
                    exists = True
        else:
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_file_path = config_path

    else:
        config_file_path = Path(KIARA_MAIN_CONFIG_FILE)
        if not config_file_path.exists():
            exists = False
        else:
            exists = True

    from kiara.context import KiaraConfig

    if not exists:
        kiara_config = KiaraConfig()

        if config_file:
            if not create_config_file:
                raise KiaraException(
                    f"specified config file does not exist: {config_file}."
                )
        else:
            if create_config_file:
                kiara_config.save(config_file_path)
    else:
        kiara_config = KiaraConfig.load_from_file(config_file_path)

    return kiara_config
