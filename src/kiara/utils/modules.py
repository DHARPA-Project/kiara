# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import os
import typing

if typing.TYPE_CHECKING:
    from kiara import Kiara


def find_file_for_module(
    module_name: str, kiara: typing.Optional["Kiara"] = None
) -> str:
    """Find the python file a module belongs to."""

    if kiara is None:
        from kiara.kiara import Kiara

        kiara = Kiara.instance()

    m_cls = kiara.get_module_class(module_type=module_name)
    python_module = m_cls.get_type_metadata().python_class.get_module()

    # TODO: some sanity checks
    module_file = python_module.__file__
    assert module_file is not None
    if module_file.endswith("__init__.py"):
        extra_bit = (
            python_module.__name__.replace(".", os.path.sep)
            + os.path.sep
            + "__init__.py"
        )
    else:
        extra_bit = python_module.__name__.replace(".", os.path.sep) + ".py"
    python_file_path = module_file[0 : -len(extra_bit)]  # noqa

    return python_file_path


def find_all_module_python_files(
    kiara: typing.Optional["Kiara"] = None,
) -> typing.Set[str]:

    if kiara is None:
        from kiara.kiara import Kiara

        kiara = Kiara.instance()

    all_paths = set()
    for module_name in kiara.available_non_pipeline_module_types:
        path = find_file_for_module(module_name=module_name, kiara=kiara)
        all_paths.add(path)
    return all_paths
