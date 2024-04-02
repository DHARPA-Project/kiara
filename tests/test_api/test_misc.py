# -*- coding: utf-8 -*-
from kiara.interfaces.python_api.base_api import BaseAPI

#  Copyright (c) 2023, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


def test_api_instance(api: BaseAPI):

    assert api.context.id


def test_api_doc(api: BaseAPI):

    assert "doc" in api.doc.keys()
    assert "Get the documentation" in api.doc["doc"]


def test_runtime_config(api: BaseAPI):

    rtc = api.get_runtime_config()
    assert "job_cache" in rtc.model_dump().keys()


def test_context_names(api: BaseAPI):

    # this is specific to the test setup api context, usually there are names in there, at least 'default'
    assert not api.list_context_names()
