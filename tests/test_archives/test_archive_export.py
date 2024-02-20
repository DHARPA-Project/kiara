# -*- coding: utf-8 -*-

from kiara.api import KiaraAPI


def test_archive_export_no_alias(api: KiaraAPI):

    api.run_job(operation="logic.and", inputs={"a": True, "b": True})["y"]

    # api.export_archive("test_archive_export_no_alias.zip")
    # api.store_value(result_bool)
    # print(result_bool)
