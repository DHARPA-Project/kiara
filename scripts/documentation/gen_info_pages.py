# -*- coding: utf-8 -*-

import builtins

from kiara.context import Kiara, KiaraContextInfo
from kiara.doc.gen_info_pages import generate_detail_pages

pkg_name = "kiara"

kiara: Kiara = Kiara.instance()
context_info = KiaraContextInfo.create_from_kiara_instance(
    kiara=kiara, package_filter=pkg_name
)

generate_detail_pages(
    context_info=context_info, sub_path="included_components", add_summary_page=True
)

builtins.plugin_package_context_info = context_info
