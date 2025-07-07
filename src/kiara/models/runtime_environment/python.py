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
    from importlib.metadata import (  # type: ignore
        distribution,
        distributions,
        packages_distributions,
    )
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
        # Method 1: Use packages_distributions (your current approach)
        all_packages = packages_distributions()
        for name, pkgs in all_packages.items():
            for pkg in pkgs:
                try:
                    dist = distribution(pkg)
                    packages[pkg] = dist.version
                except Exception:  # noqa
                    continue

        # Method 2: Use distributions() to catch packages that might be missed
        for dist in distributions():
            name = dist.metadata["Name"]
            version = dist.version
            packages[name] = version

        # # Method 3: Check for local/editable packages by scanning sys.path
        # # This is particularly useful for uv workspace packages
        # for path_str in sys.path:
        #     path = Path(path_str)
        #     if path.exists() and path.is_dir():
        #         # Look for .egg-info or .dist-info directories
        #         for info_dir in path.glob('*.egg-info'):
        #             try:
        #                 pkg_name = info_dir.stem.split('-')[0]
        #                 if pkg_name not in packages:
        #                     # Try to get version from PKG-INFO or METADATA
        #                     pkg_info_file = info_dir / 'PKG-INFO'
        #                     metadata_file = info_dir / 'METADATA'
        #
        #                     version = 'unknown'
        #                     for info_file in [metadata_file, pkg_info_file]:
        #                         if info_file.exists():
        #                             content = info_file.read_text()
        #                             for line in content.split('\n'):
        #                                 if line.startswith('Version:'):
        #                                     version = line.split(':', 1)[1].strip()
        #                                     break
        #                             if version != 'unknown':
        #                                 break
        #
        #                     packages[pkg_name] = version
        #             except Exception:
        #                 continue
        #
        #         # Also check for .dist-info directories
        #         for info_dir in path.glob('*.dist-info'):
        #             try:
        #                 pkg_name = info_dir.stem.split('-')[0]
        #                 if pkg_name not in packages:
        #                     metadata_file = info_dir / 'METADATA'
        #                     version = 'unknown'
        #
        #                     if metadata_file.exists():
        #                         content = metadata_file.read_text()
        #                         for line in content.split('\n'):
        #                             if line.startswith('Version:'):
        #                                 version = line.split(':', 1)[1].strip()
        #                                 break
        #
        #                     packages[pkg_name] = version
        #             except Exception:
        #                 continue

        return {
            "python_version": sys.version,
            "packages": [
                {"name": p, "version": packages[p]}
                for p in sorted(packages.keys(), key=lambda x: x.lower())
            ],
        }


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
