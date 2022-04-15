# -*- coding: utf-8 -*-

import builtins

from kiara.doc.gen_info_pages import generate_detail_pages
from kiara.kiara import Kiara, KiaraContextInfo

pkg_name = "kiara"

kiara: Kiara = Kiara.instance()
context_info = KiaraContextInfo.create_from_kiara_instance(
    kiara=kiara, package_filter=pkg_name
)

generate_detail_pages(context_info=context_info)

builtins.plugin_package_context_info = context_info
