# -*- coding: utf-8 -*-
import pytest

from kiara.api import KiaraAPI


def test_pure_string(api: KiaraAPI):

    value = api.register_data("test_string", "string")
    assert value.data == "test_string"


def test_string_with_config(api: KiaraAPI):
    config = {
        "type": "string",
        "type_config": {"allowed_strings": ["x", "y", "z", "test_string"]},
    }
    value = api.register_data("test_string", data_type=config)
    assert value.data == "test_string"


def test_invalid_string(api: KiaraAPI):

    with pytest.raises(ValueError):
        config = {"type": "string", "type_config": {"allowed_strings": ["x", "y", "z"]}}
        api.register_data("test_string", data_type=config)
