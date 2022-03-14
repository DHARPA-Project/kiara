# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import sys
import typing
from importlib_metadata import distribution, packages_distributions
from pydantic import BaseModel, Field

from kiara.models.runtime_environment import RuntimeEnvironment


class PythonPackage(BaseModel):

    name: str = Field(description="The name of the Python package.")
    version: str = Field(description="The version of the package.")


class PythonRuntimeEnvironment(RuntimeEnvironment):

    environment_type: typing.Literal["python"]
    python_version: str = Field(description="The version of Python.")
    packages: typing.List[PythonPackage] = Field(
        description="The packages installed in the Python (virtual) environment."
    )
    # python_config: typing.Dict[str, str] = Field(
    #     description="Configuration details about the Python installation."
    # )

    @classmethod
    def retrieve_environment_data(cls) -> typing.Dict[str, typing.Any]:

        packages = []
        all_packages = packages_distributions()
        for name, pkgs in all_packages.items():
            for pkg in pkgs:
                dist = distribution(pkg)
                packages.append({"name": name, "version": dist.version})

        result: typing.Dict[str, typing.Any] = {
            "python_version": sys.version,
            "packages": sorted(packages, key=lambda x: x["name"]),
        }

        # if config.include_all_info:
        #     import sysconfig
        #     result["python_config"] = sysconfig.get_config_vars()

        return result
