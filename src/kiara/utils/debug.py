# -*- coding: utf-8 -*-
import logging
import uuid
from typing import TYPE_CHECKING, Any, List, Mapping

from rich import box
from rich.console import RenderableType
from rich.table import Table

from kiara.models.module.jobs import ActiveJob, JobConfig
from kiara.models.module.manifest import Manifest
from kiara.models.values.value import Value
from kiara.utils import get_dev_config
from kiara.utils.cli import terminal_print
from kiara.utils.develop import DetailLevel
from kiara.utils.modules import module_config_is_empty

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.modules import KiaraModule

DEFAULT_VALUE_MAP_RENDER_CONFIG = {
    "ignore_fields": [
        "kiara_id",
        "data_type_class",
        "destiny_backlinks",
        "environments",
        "property_links",
    ],
}


def create_module_preparation_table(
    kiara: "Kiara",
    job_config: JobConfig,
    job_id: uuid.UUID,
    module: "KiaraModule",
    **render_config: Any,
) -> Table:

    dev_config = get_dev_config()
    table = Table(show_header=False, box=box.SIMPLE)
    table.add_column("key", style="i")
    table.add_column("value")

    table.add_row("job_id", str(job_id))

    module_details = dev_config.log.pre_run.module_info
    if module_details not in [DetailLevel.NONE.value, DetailLevel.NONE]:
        pipeline_name = job_config.module_config.get("pipeline_name", None)
        if module_details in [DetailLevel.MINIMAL.value, DetailLevel.MINIMAL]:
            table.add_row("module", job_config.module_type)
            if pipeline_name:
                table.add_row("pipeline name", pipeline_name)
            doc = module.operation.doc
            table.add_row(
                "module desc",
                doc.description,
                # kiara.context_info.module_types.item_infos[
                #     job_config.module_type
                # ].documentation.description,
            )
        elif module_details in [DetailLevel.FULL.value, DetailLevel.FULL]:
            table.add_row("module", job_config.module_type)
            if pipeline_name:
                table.add_row("pipeline name", pipeline_name)
            doc = module.operation.doc
            table.add_row(
                "module doc",
                doc.full_doc,
                # kiara.context_info.module_types.item_infos[
                #     job_config.module_type
                # ].documentation.full_doc,
            )
            if module_config_is_empty(job_config.module_config):
                table.add_row("module_config", "-- no config --")
            else:
                module = kiara.module_registry.create_module(manifest=job_config)
                table.add_row("module_config", module.config)

    inputs_details = dev_config.log.pre_run.inputs_info
    if inputs_details not in [DetailLevel.NONE.value, DetailLevel.NONE]:
        if inputs_details in [DetailLevel.MINIMAL, DetailLevel.MINIMAL.value]:
            render_config["show_type"] = False
            value_map_rend = create_value_map_renderable(
                value_map=job_config.inputs, **render_config
            )
            table.add_row("inputs", value_map_rend)
        elif inputs_details in [DetailLevel.FULL, DetailLevel.FULL.value]:
            value_map = kiara.data_registry.load_values(values=job_config.inputs)
            table.add_row("inputs", value_map.create_renderable(**render_config))

    return table


def create_post_run_table(
    kiara: "Kiara",
    job: ActiveJob,
    module: "KiaraModule",
    job_config: JobConfig,
    **render_config: Any,
) -> Table:

    dev_config = get_dev_config()
    table = Table(show_header=False, box=box.SIMPLE)
    table.add_column("key", style="i")
    table.add_column("value")

    table.add_row("job_id", str(job.job_id))
    table.add_row("status", job.status.value)
    if job.error:
        table.add_row("error", job.error)
    if job.job_log.log:
        start_time = job.job_log.log[0].timestamp
        last_time = start_time
        log_table = Table(show_header=False, box=box.SIMPLE_HEAD)
        log_table.add_column("log", style="i")
        log_table.add_column("level")
        log_table.add_column("timestamp")
        for log in job.job_log.log:
            log_time = log.timestamp
            if log_time == start_time:
                time_str = str(log_time)
            else:
                time_str = f"+ {log_time - last_time}"
            log_level = logging.getLevelName(log.log_level).lower()
            log_table.add_row(log.msg, log_level, time_str)
            last_time = log_time
        table.add_row("duration", f"{last_time - start_time}")
        table.add_row("logs", log_table)
    module_details = dev_config.log.post_run.module_info
    if module_details not in [DetailLevel.NONE.value, DetailLevel.NONE]:
        if module_details in [DetailLevel.MINIMAL.value, DetailLevel.MINIMAL]:
            table.add_row("module", module.module_type_name)
            table.add_row(
                "module desc",
                kiara.context_info.module_types.item_infos[
                    module.module_type_name
                ].documentation.description,
            )
        elif module_details in [DetailLevel.FULL.value, DetailLevel.FULL]:
            table.add_row("module", module.module_type_name)
            table.add_row(
                "module doc",
                kiara.context_info.module_types.item_infos[
                    module.module_type_name
                ].documentation.full_doc,
            )
            if module_config_is_empty(module.config.model_dump()):
                table.add_row("module_config", "-- no config --")
            else:
                table.add_row("module_config", module.config)

    if job_config.pipeline_metadata is not None:
        pm_table = Table(show_header=False, box=box.SIMPLE)
        pm_table.add_column("key")
        pm_table.add_column("value")
        pm_table.add_row("pipeline_id", str(job_config.pipeline_metadata.pipeline_id))
        pm_table.add_row("step_id", job_config.pipeline_metadata.step_id)
        table.add_row("pipeline_step_metadata", pm_table)
    else:
        table.add_row("pipeline_step_metadata", "-- not a pipeline step --")

    inputs_details = dev_config.log.post_run.inputs_info
    if inputs_details not in [DetailLevel.NONE.value, DetailLevel.NONE]:
        if inputs_details in [DetailLevel.MINIMAL, DetailLevel.MINIMAL.value]:
            render_config["show_type"] = False
            value_map_rend: RenderableType = create_value_map_renderable(
                value_map=job_config.inputs, **render_config
            )
            table.add_row("inputs", value_map_rend)
        elif inputs_details in [DetailLevel.FULL, DetailLevel.FULL.value]:
            value_map = kiara.data_registry.load_values(values=job_config.inputs)
            table.add_row("inputs", value_map.create_renderable(**render_config))

    outputs_details = dev_config.log.post_run.outputs_info
    if outputs_details not in [DetailLevel.NONE.value, DetailLevel.NONE]:
        if outputs_details in [DetailLevel.MINIMAL, DetailLevel.MINIMAL.value]:
            render_config["show_type"] = False
            if job.results is None:
                value_map_rend = "-- no results --"
            else:
                value_map_rend = create_value_map_renderable(
                    value_map=job.results, **render_config
                )
            table.add_row("outputs", value_map_rend)
        elif outputs_details in [DetailLevel.FULL, DetailLevel.FULL.value]:
            if job.results is None:
                value_map_rend = "-- no results --"
            else:
                value_map = kiara.data_registry.load_values(values=job.results)
                value_map_rend = value_map.create_renderable(**render_config)
            table.add_row("outputs", value_map_rend)

    return table


def terminal_print_manifest(manifest: Manifest):

    terminal_print(manifest.create_renderable())


def create_value_map_renderable(value_map: Mapping[str, Any], **render_config: Any):

    show_type = render_config.get("show_type", True)

    rc = dict(DEFAULT_VALUE_MAP_RENDER_CONFIG)
    rc.update(render_config)

    table = Table(show_header=True, box=box.SIMPLE)
    table.add_column("field name", style="i")
    if show_type:
        table.add_column("type")
    table.add_column("value")

    for k, v in value_map.items():
        row: List[Any] = [k]
        if isinstance(v, Value):
            if show_type:
                row.append("value object")
            row.append(v.create_renderable(**rc))
        elif isinstance(v, uuid.UUID):
            if show_type:
                row.append("value id")
            row.append(str(v))
        else:
            if show_type:
                row.append("raw data")
            row.append(str(v))

        table.add_row(*row)

    return table
