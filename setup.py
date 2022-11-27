#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Setup file for kiara.
    Use setup.cfg to configure your project.

    This file was generated with PyScaffold 3.1.
    PyScaffold helps you to put up the scaffold of your new Python project.
    Learn more under: https://pyscaffold.org/
"""
from setuptools import setup

import sys

try:
    from pkg_resources import VersionConflict, require

    require("setuptools>=38.3")
except VersionConflict:
    print("Error: version of setuptools is too old (<38.3)!")
    sys.exit(1)


def get_extra_requires(add_all=True, add_all_dev=True, add_all_modules=True):

    from distutils.dist import Distribution

    dist = Distribution()
    dist.parse_config_files()
    dist.parse_command_line()

    extras = {}
    extra_deps = dist.get_option_dict("options.extras_require")

    for extra_name, data in extra_deps.items():

        _, dep_string = data
        deps = []
        d = dep_string.split("\n")
        for line in d:
            if not line:
                continue
            deps.append(line)
        extras[extra_name] = deps

    if add_all:
        all = set()
        for e_n, deps in extras.items():
            if not e_n.startswith("dev_") and not e_n.startswith("modules_"):
                all.update(deps)
        # all.add("kiara_modules.core")
        extras["all"] = all

    if add_all_modules:
        all_modules = set()
        for e_n, deps in extras.items():
            if e_n.startswith("modules_"):
                all_modules.update(deps)
        extras["modules_all"] = all_modules

    if add_all_dev:
        all_modules_dev = set()
        for e_n, deps in extras.items():
            if not e_n.startswith("modules_"):
                all_modules_dev.update(deps)

        extras["dev_all"] = all_modules_dev

    return extras


if __name__ in ["__main__", "builtins", "__builtin__"]:
    setup(
        extras_require=get_extra_requires(),
    )
