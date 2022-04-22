# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import os
import structlog
from pathlib import Path
from pydantic import Field, PrivateAttr
from typing import TYPE_CHECKING, Any, Dict, Iterable, Mapping, Optional, Union

from kiara.defaults import DEFAULT_EXCLUDE_DIRS, VALID_PIPELINE_FILE_EXTENSIONS
from kiara.models.module.operation import (
    OperationConfig,
    OperationDetails,
    OperationSchema,
    PipelineOperationConfig,
)
from kiara.models.values.value import Value, ValueMap
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import KiaraModule
from kiara.modules.included_core_modules.pipeline import PipelineModule
from kiara.operations import OperationType
from kiara.utils import is_debug
from kiara.utils.class_loading import find_all_kiara_pipeline_paths
from kiara.utils.pipelines import check_doc_sidecar, get_pipeline_details_from_path

if TYPE_CHECKING:
    from kiara import Kiara

logger = structlog.getLogger()


class PipelineOperationDetails(OperationDetails):
    # @classmethod
    # def create_from_module(cls, module: KiaraModule):
    #
    #     return PipelineOperationDetails(
    #         operation_id=module.module_type_name,
    #         pipeline_inputs_schema=module.inputs_schema,
    #         pipeline_outputs_schema=module.outputs_schema,
    #     )

    pipeline_inputs_schema: Mapping[str, ValueSchema] = Field(
        description="The input schema for the pipeline."
    )
    pipeline_outputs_schema: Mapping[str, ValueSchema] = Field(
        description="The output schema for the pipeline."
    )
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

    def create_module_inputs(self, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
        return inputs

    def create_operation_outputs(self, outputs: ValueMap) -> Mapping[str, Value]:
        return outputs


class PipelineOperationType(OperationType[PipelineOperationDetails]):

    _operation_type_name = "pipeline"

    def __init__(self, kiara: "Kiara", op_type_name: str):

        super().__init__(kiara=kiara, op_type_name=op_type_name)
        self._pipelines = None

    @property
    def pipeline_data(self):

        if self._pipelines is not None:
            return self._pipelines

        ignore_errors = False
        pipeline_paths: Dict[
            str, Optional[Mapping[str, Any]]
        ] = find_all_kiara_pipeline_paths(skip_errors=ignore_errors)

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
                            md = dict(pipeline_paths[_path])
                            if md is None:
                                md = {}
                            md.update(existing_metadata)
                            data["metadata"] = md

                            # rel_path = os.path.relpath(os.path.dirname(full_path), path)
                            # if not rel_path or rel_path == ".":
                            #     raise NotImplementedError()
                            #     ns_name = name
                            # else:
                            #     _rel_path = rel_path.replace(os.path.sep, ".")
                            #     ns_name = f"{_rel_path}.{name}"
                            #
                            # if not ns_name:
                            #     raise Exception(
                            #         f"Could not determine namespace for pipeline file '{filename}'."
                            #     )
                            # if ns_name in files.keys():
                            #     raise Exception(
                            #         f"Duplicate workflow name: {ns_name}"
                            #     )

                            all_pipelines.append(data)

                        except Exception as e:
                            if is_debug():
                                import traceback

                                traceback.print_exc()
                            logger.warning(
                                "ignore.pipeline_file", path=full_path, reason=str(e)
                            )

            elif path.is_file():
                data = get_pipeline_details_from_path(path=path)
                data = check_doc_sidecar(path, data)
                existing_metadata = data.pop("metadata", {})
                md = dict(pipeline_paths[_path])
                if md is None:
                    md = {}
                md.update(existing_metadata)
                data["metadata"] = md
                all_pipelines.append(data)

        pipelines = {}
        for pipeline in all_pipelines:
            name = pipeline["data"].get("pipeline_name", None)
            if name is None:
                name = os.path.basename[pipeline["source"]]
                if "." in name:
                    name, _ = name.rsplit(".", maxsplit=1)
            pipelines[name] = pipeline

        return pipelines

    def retrieve_included_operation_configs(
        self,
    ) -> Iterable[Union[Mapping, OperationConfig]]:

        op_configs = []
        for pipeline_name, pipeline_data in self.pipeline_data.items():
            pipeline_config = dict(pipeline_data["data"])
            pipeline_id = pipeline_config.pop("pipeline_name", None)
            doc = pipeline_config.pop("doc", None)
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
    ) -> Optional[PipelineOperationDetails]:

        if isinstance(module, PipelineModule):

            op_details = PipelineOperationDetails.create_operation_details(
                operation_id=module.config.pipeline_name,
                pipeline_inputs_schema=module.inputs_schema,
                pipeline_outputs_schema=module.outputs_schema,
            )
            return op_details
        else:
            return None
