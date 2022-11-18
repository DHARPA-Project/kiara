# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import os
import structlog
from pathlib import Path
from pydantic import Field, PrivateAttr
from typing import TYPE_CHECKING, Any, Dict, Iterable, Mapping, Union

from kiara.defaults import DEFAULT_EXCLUDE_DIRS, VALID_PIPELINE_FILE_EXTENSIONS
from kiara.models.module.operation import (
    OperationConfig,
    OperationDetails,
    OperationSchema,
    PipelineOperationConfig,
)
from kiara.models.module.pipeline import PipelineConfig
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import KiaraModule
from kiara.modules.included_core_modules.pipeline import PipelineModule
from kiara.operations import OperationType
from kiara.utils import log_exception
from kiara.utils.class_loading import find_all_kiara_pipeline_paths
from kiara.utils.pipelines import check_doc_sidecar, get_pipeline_details_from_path

if TYPE_CHECKING:
    from kiara.context import Kiara

logger = structlog.getLogger()


class PipelineOperationDetails(OperationDetails):

    pipeline_inputs_schema: Mapping[str, ValueSchema] = Field(
        description="The input schema for the pipeline."
    )
    pipeline_outputs_schema: Mapping[str, ValueSchema] = Field(
        description="The output schema for the pipeline."
    )
    pipeline_config: PipelineConfig = Field(description="The pipeline config.")
    _op_schema: OperationSchema = PrivateAttr(default=None)

    def get_operation_schema(self) -> OperationSchema:

        if self._op_schema is not None:
            return self._op_schema

        self._op_schema = OperationSchema(
            alias=self.operation_id,
            inputs_schema=self.pipeline_inputs_schema,
            outputs_schema=self.pipeline_outputs_schema,
        )
        return self._op_schema

    # def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
    #     return inputs
    #
    # def create_operation_outputs(self, outputs: ValueMap) -> Mapping[str, Value]:
    #     return outputs


class PipelineOperationType(OperationType[PipelineOperationDetails]):

    _operation_type_name = "pipeline"

    def __init__(self, kiara: "Kiara", op_type_name: str) -> None:

        super().__init__(kiara=kiara, op_type_name=op_type_name)
        self._pipelines: Union[None, Mapping[str, Mapping[str, Any]]] = None

    @property
    def pipeline_data(self) -> Mapping[str, Mapping[str, Any]]:

        if self._pipelines is not None:
            return self._pipelines

        ignore_errors = False
        pipeline_paths: Dict[
            str, Union[Dict[str, Any], None]
        ] = find_all_kiara_pipeline_paths(skip_errors=ignore_errors)

        for ep in self._kiara.context_config.extra_pipelines:
            ep = os.path.realpath(ep)
            if ep not in pipeline_paths.keys():
                pipeline_paths[ep] = None

        all_pipelines = []

        for _path in pipeline_paths.keys():
            path = Path(_path)
            if not path.exists():
                logger.warning(
                    "ignore.pipeline_path", path=path, reason="path does not exist"
                )
                continue

            elif path.is_dir():

                for root, dirnames, filenames in os.walk(path, topdown=True):

                    dirnames[:] = [d for d in dirnames if d not in DEFAULT_EXCLUDE_DIRS]

                    for filename in [
                        f
                        for f in filenames
                        if os.path.isfile(os.path.join(root, f))
                        and any(
                            f.endswith(ext) for ext in VALID_PIPELINE_FILE_EXTENSIONS
                        )
                    ]:

                        full_path = os.path.join(root, filename)
                        try:

                            data = get_pipeline_details_from_path(path=full_path)
                            data = check_doc_sidecar(full_path, data)
                            existing_metadata = data.pop("metadata", {})
                            _md = pipeline_paths[_path]
                            if _md is None:
                                md = {}
                            else:
                                md = dict(_md)
                            md.update(existing_metadata)
                            data["metadata"] = md

                            all_pipelines.append(data)

                        except Exception as e:
                            log_exception(e)
                            logger.warning(
                                "ignore.pipeline_file", path=full_path, reason=str(e)
                            )

            elif path.is_file():
                data = get_pipeline_details_from_path(path=path)
                data = check_doc_sidecar(path, data)
                existing_metadata = data.pop("metadata", {})
                _md = pipeline_paths[_path]
                if _md is None:
                    md = {}
                else:
                    md = dict(_md)
                md.update(existing_metadata)
                data["metadata"] = md
                all_pipelines.append(data)

        pipelines = {}
        for pipeline in all_pipelines:
            name = pipeline["data"].get("pipeline_name", None)
            if name is None:
                source = pipeline["source"]
                name = os.path.basename(source)
                if "." in name:
                    name, _ = name.rsplit(".", maxsplit=1)
                pipeline["data"]["pipeline_name"] = name
            pipelines[name] = pipeline

        return pipelines

    def retrieve_included_operation_configs(
        self,
    ) -> Iterable[Union[Mapping, OperationConfig]]:

        op_configs = []
        for pipeline_name, pipeline_data in self.pipeline_data.items():
            pipeline_config: Dict[str, Any] = dict(pipeline_data["data"])
            pipeline_id = pipeline_config.pop("pipeline_name", None)
            doc = pipeline_config.get("doc", None)
            pipeline_metadata = pipeline_data["metadata"]

            op_details = PipelineOperationConfig(
                pipeline_name=pipeline_id,
                pipeline_config=pipeline_config,
                doc=doc,
                metadata=pipeline_metadata,
            )
            op_configs.append(op_details)
        return op_configs

    def check_matching_operation(
        self, module: "KiaraModule"
    ) -> Union[PipelineOperationDetails, None]:

        if isinstance(module, PipelineModule):

            op_details = PipelineOperationDetails.create_operation_details(
                operation_id=module.config.pipeline_name,
                pipeline_inputs_schema=module.inputs_schema,
                pipeline_outputs_schema=module.outputs_schema,
                pipeline_config=module.config,
            )
            return op_details
        else:
            return None
