# -*- coding: utf-8 -*-

#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import sys
from importlib_metadata import distribution, packages_distributions
from pydantic import BaseModel, Field
from rich import box
from rich.console import RenderableType
from rich.table import Table
from typing import Any, Dict, List, Literal, Optional

from kiara.models.runtime_environment import RuntimeEnvironment
from kiara.utils.output import extract_renderable


class PythonPackage(BaseModel):

    name: str = Field(description="The name of the Python package.")
    version: str = Field(description="The version of the package.")


class PythonRuntimeEnvironment(RuntimeEnvironment):

    environment_type: Literal["python"]
    python_version: str = Field(description="The version of Python.")
    packages: List[PythonPackage] = Field(
        description="The packages installed in the Python (virtual) environment."
    )
    # python_config: typing.Dict[str, str] = Field(
    #     description="Configuration details about the Python installation."
    # )

    def _create_renderable_for_field(
        self, field_name: str, for_summary: bool = False
    ) -> Optional[RenderableType]:

        if field_name != "packages":
            return extract_renderable(getattr(self, field_name))

        if for_summary:
            return ", ".join(p.name for p in self.packages)

        table = Table(show_header=True, box=box.SIMPLE)
        table.add_column("package name")
        table.add_column("version")

        for package in self.packages:
            table.add_row(package.name, package.version)

        return table

    @classmethod
    def retrieve_environment_data(cls) -> Dict[str, Any]:

        packages = []
        all_packages = packages_distributions()
        for name, pkgs in all_packages.items():
            for pkg in pkgs:
                dist = distribution(pkg)
                packages.append({"name": name, "version": dist.version})

        result: Dict[str, Any] = {
            "python_version": sys.version,
            "packages": sorted(packages, key=lambda x: x["name"]),
        }

        # if config.include_all_info:
        #     import sysconfig
        #     result["python_config"] = sysconfig.get_config_vars()

        return result
