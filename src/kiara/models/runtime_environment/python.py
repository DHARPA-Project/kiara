# -*- coding: utf-8 -*-

#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import sys
from functools import lru_cache
from typing import Any, ClassVar, Dict, List, Literal, Mapping, Union

from pydantic import BaseModel, Field
from rich import box
from rich.console import RenderableType
from rich.table import Table

from kiara.models.runtime_environment import RuntimeEnvironment
from kiara.utils.output import extract_renderable

try:
    from importlib.metadata import distribution, packages_distributions  # type: ignore
except Exception:
    from importlib_metadata import distribution, packages_distributions  # type:ignore


class PythonPackage(BaseModel):

    name: str = Field(description="The name of the Python package.")
    version: str = Field(description="The version of the package.")


@lru_cache()
def find_all_distributions():
    all_packages = packages_distributions()
    return all_packages


class PythonRuntimeEnvironment(RuntimeEnvironment):

    _kiara_model_id: ClassVar = "info.runtime.python"

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
    ) -> Union[RenderableType, None]:

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

    def _retrieve_sub_profile_env_data(self) -> Mapping[str, Any]:

        only_packages = [p.name for p in self.packages]
        full = {k.name: k.version for k in self.packages}

        return {
            "package_names": only_packages,
            "packages": full,
            "package_names_incl_python_version": {
                "python_version": self.python_version,
                "packages": only_packages,
            },
            "packages_incl_python_version": {
                "python_version": self.python_version,
                "packages": full,
            },
        }

    @classmethod
    def retrieve_environment_data(cls) -> Dict[str, Any]:

        packages: Dict[str, str] = {}
        all_packages = find_all_distributions()

        for name, pkgs in all_packages.items():
            for pkg in pkgs:
                dist = distribution(pkg)
                if pkg in packages.keys() and packages[pkg] != dist.version:
                    raise Exception(
                        f"Multiple versions of package '{pkg}' available: {packages[pkg]} and {dist.version}."
                    )
                packages[pkg] = dist.version

        result: Dict[str, Any] = {
            "python_version": sys.version,
            "packages": [
                {"name": p, "version": packages[p]}
                for p in sorted(packages.keys(), key=lambda x: x.lower())
            ],
        }

        # if config.include_all_info:
        #     import sysconfig
        #     result["python_config"] = sysconfig.get_config_vars()

        return result


class KiaraPluginsRuntimeEnvironment(RuntimeEnvironment):

    _kiara_model_id: ClassVar = "info.runtime.kiara_plugins"

    environment_type: Literal["kiara_plugins"]
    kiara_plugins: List[PythonPackage] = Field(
        description="The kiara plugin packages installed in the Python (virtual) environment."
    )

    def _create_renderable_for_field(
        self, field_name: str, for_summary: bool = False
    ) -> Union[RenderableType, None]:

        if field_name != "packages":
            return extract_renderable(getattr(self, field_name))

        if for_summary:
            return ", ".join(p.name for p in self.kiara_plugins)

        table = Table(show_header=True, box=box.SIMPLE)
        table.add_column("package name")
        table.add_column("version")

        for package in self.kiara_plugins:
            table.add_row(package.name, package.version)

        return table

    def _retrieve_sub_profile_env_data(self) -> Mapping[str, Any]:

        only_packages = [p.name for p in self.kiara_plugins]
        full = {k.name: k.version for k in self.kiara_plugins}

        return {"package_names": only_packages, "packages": full}

    @classmethod
    def retrieve_environment_data(cls) -> Dict[str, Any]:

        packages = []
        all_packages = find_all_distributions()
        all_pkg_names = set()
        for name, pkgs in all_packages.items():

            for pkg in pkgs:
                if not pkg.startswith("kiara_plugin.") and not pkg.startswith(
                    "kiara-plugin."
                ):
                    continue
                else:
                    all_pkg_names.add(pkg)

        for pkg in sorted(all_pkg_names):
            dist = distribution(pkg)
            packages.append({"name": pkg, "version": dist.version})

        result: Dict[str, Any] = {
            "kiara_plugins": packages,
        }

        # if config.include_all_info:
        #     import sysconfig
        #     result["python_config"] = sysconfig.get_config_vars()

        return result
