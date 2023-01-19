# -*- coding: utf-8 -*-
from kiara import KiaraAPI

#  Copyright (c) 2023, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


def test_api_instance(api: KiaraAPI):

    assert api.context.id


def test_api_doc(api: KiaraAPI):

    assert "doc" in api.doc.keys()
    assert "Get the documentation" in api.doc["doc"]


def test_runtime_config(api: KiaraAPI):

    rtc = api.get_runtime_config()
    assert "job_cache" in rtc.dict().keys()


def test_context_names(api: KiaraAPI):

    # this is specific to the test setup api context, usually there are names in there, at least 'default'
    assert not api.list_context_names()
