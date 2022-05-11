# -*- coding: utf-8 -*-

#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import os
import platform
import typing
from pydantic import Field

from kiara.models.runtime_environment import RuntimeEnvironment


class OSRuntimeEnvironment(RuntimeEnvironment):
    """Manages information about the OS this kiara instance is running in.

    # TODO: details for other OS's (mainly BSDs)
    """

    _kiara_model_id = "info.runtime.os"

    environment_type: typing.Literal["operating_system"]
    operation_system: str = Field(description="The operation system name.")
    platform: str = Field(description="The platform name.")
    release: str = Field(description="The platform release name.")
    version: str = Field(description="The platform version name.")
    machine: str = Field(description="The architecture.")
    os_specific: typing.Dict[str, typing.Any] = Field(
        description="OS specific platform metadata.", default_factory=dict
    )

    @classmethod
    def retrieve_environment_data(self) -> typing.Dict[str, typing.Any]:

        os_specific: typing.Dict[str, typing.Any] = {}
        platform_system = platform.system()
        if platform_system == "Linux":
            import distro

            os_specific["distribution"] = {
                "name": distro.name(),
                "version": distro.version(),
                "codename": distro.codename(),
            }
        elif platform_system == "Darwin":
            mac_version = platform.mac_ver()
            os_specific["mac_ver_release"] = mac_version[0]
            os_specific["mac_ver_machine"] = mac_version[2]

        result = {
            "operation_system": os.name,
            "platform": platform_system,
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "os_specific": os_specific,
        }

        # if config.include_all_info:
        #     result["uname"] = platform.uname()._asdict()

        return result
