# -*- coding: utf-8 -*-
from mkdocstrings.handlers.base import BaseCollector, BaseHandler, BaseRenderer


def get_handler():
    return KiaraModuleHandler()


class KiaraModuleCollector(BaseCollector):
    def __init__(self):
        pass


class KiaraModuleRenderer(BaseRenderer):
    def __init__(self):
        pass


class KiaraModuleHandler(BaseHandler):
    def __init__(self):

        collector = KiaraModuleCollector()
        renderer = KiaraModuleRenderer()
        super().__init__(collector=collector, renderer=renderer)
