# -*- coding: utf-8 -*-
import click

from kiara import Kiara
from kiara.interfaces.rest_api import KiaraRestService


@click.group()
@click.pass_context
def service(ctx):
    """Metadata-related sub-commands."""


@service.command(name="start")
@click.pass_context
async def start(ctx):

    kiara_obj: Kiara = ctx.obj["kiara"]

    service = KiaraRestService(kiara=kiara_obj)
    service.start()

    # app = FastAPI()
    #
    # @app.post("/run")
    # def run():
    #
    #     module_type = "logic.and"
    #     module_config = {"delay": 0.1}
    #     inputs = {"a": True, "b": True}
    #     result_values = kiara_obj.run(
    #         module_type=module_type,
    #         module_config=module_config,
    #         inputs=inputs,
    #         resolve_result=False,
    #     )
    #
    #     op_mgmt: DataOperationMgmt = kiara_obj.data_operations
    #
    #     result = {}
    #     for field_name, value in result_values.items():
    #         v = op_mgmt.run("serialize", "msgpack", value)
    #         result[field_name] = v.get_value_obj("bytes")
    #
    #     r = {}
    #     schemas = {}
    #
    #     for field_name, value in result.items():
    #         assert value.type_name == "bytes"
    #         v = op_mgmt.run("deserialize", "msgpack", value)
    #         r[field_name] = v.get_value_obj("value_data")
    #         schemas[field_name] = {"type": v.get_value_data("value_type")}
    #
    #     value_set = ValueSetImpl.from_schemas(
    #         kiara=kiara_obj, schemas=schemas, initial_values=r
    #     )
    #     import pp
    #
    #     pp(value_set.get_all_value_data())
    #
    # thread = threading.Thread(target=uvicorn.run, args=(app,))
    # thread.start()
    # thread.join()


# def create_module_router_from_modules(kiara: Kiara) -> APIRouter:
#
#     module_router = APIRouter()
#     workflow_router = APIRouter()

# for name in sorted(all_classes):
#
#     cls = all_classes[name]
#
#     if name == "dharpa_workflow":
#         continue
#
#     func, resp_model = create_processing_function(cls)
#
#     is_workflow = True if issubclass(cls, DharpaWorkflowOld) else False
#     if is_workflow:
#         workflow_router.post(f"/{name}", response_model=resp_model)(func)
#     else:
#         module_router.post(f"/{name}", response_model=resp_model)(func)
#
# return module_router, workflow_router
