# -*- coding: utf-8 -*-
from kiara.interfaces.python_api.base_api import BaseAPI

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


def test_render_python_script(api: BaseAPI):

    render_config = {"inputs": {"a": True, "b": False}}

    rendered = api.render(
        "logic.xor",
        source_type="pipeline",
        target_type="python_script",
        render_config=render_config,
    )

    compile(rendered, "<string>", "exec")

    local_vars = {}
    exec(rendered, {}, local_vars)  # noqa
    assert local_vars["pipeline_result_y"].data is True
