# -*- coding: utf-8 -*-
import asyncio
import os
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from hypercorn import Config
from hypercorn.asyncio import serve
from typing import Optional

from kiara import Kiara
from kiara.context import KiaraContextInfo
from kiara.models.module.operation import OperationGroupInfo


class KiaraAPI(object):
    def __init__(self, kiara: Kiara):

        self._kiara: Kiara = kiara
        self._config: Optional[Config] = None

    @property
    def app(self):

        app = FastAPI()

        static_dir = os.path.join(os.path.dirname(__file__), "static")

        app.mount(
            "/static", StaticFiles(directory=static_dir, html=True), name="static"
        )

        @app.get("/context/", response_model=KiaraContextInfo)
        async def context():
            return self._kiara.context_info

        @app.get("/operations/", response_model=OperationGroupInfo)
        async def operations(test: Optional[bool] = False):
            print(test)
            return self._kiara.context_info.operations

        @app.get("/operation/{operation_id}", response_class=HTMLResponse)
        async def get_operation(operation_id):

            print(operation_id)

            return self._kiara.context_info.operations[operation_id].create_html()

        self._app = app
        return self._app

    @property
    def config(self) -> Config:
        if self._config is not None:
            return self._config

        self._config = Config()
        self._config.bind = ["localhost:8888"]
        return self._config

    def start(self):

        asyncio.run(serve(self.app, self.config))
