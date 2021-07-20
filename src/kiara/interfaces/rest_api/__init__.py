# -*- coding: utf-8 -*-
import threading
import typing
import uvicorn
from fastapi import APIRouter, FastAPI
from pydantic.fields import Field
from pydantic.main import BaseModel

from kiara import Kiara
from kiara.data.operations import DataOperationMgmt
from kiara.data.values import ValueSchema, ValueSetImpl
from kiara.metadata.module_models import KiaraModuleInstanceMetadata


class ModuleRunResponse(BaseModel):

    outputs: typing.Dict[str, typing.Any] = Field(description="The module output.")
    output_schema: typing.Dict[str, ValueSchema] = Field(
        description="The output schema."
    )


class KiaraRestService(object):
    def __init__(self, kiara: typing.Optional[Kiara] = None):

        if kiara is None:
            kiara = Kiara.instance()

        self._kiara: Kiara = kiara
        self._app = FastAPI()
        self._modules_router: APIRouter = self.create_modules_router()
        self._app.include_router(self._modules_router, prefix="/module")

    def start(self):

        thread = threading.Thread(target=uvicorn.run, args=(self._app,))
        thread.start()
        thread.join()

    def create_modules_router(self):

        modules_router = APIRouter()

        @modules_router.post("/explain-instance")
        def explain(
            module_type: str,
            module_config: typing.Optional[typing.Dict[str, typing.Any]] = None,
        ) -> KiaraModuleInstanceMetadata:

            module = self._kiara.create_module(
                module_type=module_type, module_config=module_config
            )
            return module.info

        @modules_router.post("/run")
        def run(
            module_type: str,
            module_config: typing.Optional[typing.Dict[str, typing.Any]] = None,
            inputs: typing.Optional[typing.Dict[str, typing.Any]] = None,
        ) -> ModuleRunResponse:

            module = self._kiara.create_module(
                module_type=module_type, module_config=module_config
            )

            result_values = self._kiara.run_module(
                module=module,
                inputs=inputs,
                resolve_result=False,
            )

            op_mgmt: DataOperationMgmt = self._kiara.data_operations

            result = {}
            for field_name, value in result_values.items():
                v = op_mgmt.run("serialize", "msgpack", value)
                result[field_name] = v.get_value_obj("bytes")

            r = {}
            schemas = {}

            for field_name, value in result.items():
                assert value.type_name == "bytes"
                v = op_mgmt.run("deserialize", "msgpack", value)
                r[field_name] = v.get_value_obj("value_data")
                schemas[field_name] = ValueSchema(
                    type=v.get_value_data("value_type"),
                    doc=result_values.get_value_obj(field_name).value_schema.doc,
                )

            value_set = ValueSetImpl.from_schemas(
                kiara=self._kiara, schemas=schemas, initial_values=r
            )
            outputs = value_set.get_all_value_data()

            _result = ModuleRunResponse(outputs=outputs, output_schema=schemas)
            return _result

        return modules_router
