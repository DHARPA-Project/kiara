# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import json
import os.path
import sys
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Set,
    Type,
    TypeVar,
    Union,
)

import structlog
from rich.console import Group, RenderableType
from ruamel.yaml import YAML

from kiara.exceptions import (
    InvalidOperationException,
    NoSuchOperationException,
)
from kiara.interfaces.python_api.models.info import (
    OperationTypeClassesInfo,
    OperationTypeInfo,
)
from kiara.models.module.manifest import Manifest
from kiara.models.module.operation import (
    ManifestOperationConfig,
    Operation,
    OperationConfig,
    PipelineOperationConfig,
)
from kiara.models.module.pipeline import PipelineConfig
from kiara.models.module.pipeline.pipeline import Pipeline
from kiara.models.python_class import KiaraModuleInstance
from kiara.operations import OperationType
from kiara.utils import is_develop, log_exception, log_message
from kiara.utils.cli import terminal_print
from kiara.utils.output import extract_renderable
from kiara.utils.pipelines import find_pipeline_data_in_paths

if TYPE_CHECKING:
    from kiara.context import Kiara

logger = structlog.getLogger()
yaml = YAML(typ="safe")


OP_TYPE = TypeVar("OP_TYPE", bound=OperationType)


class OperationRegistry(object):
    def __init__(
        self,
        kiara: "Kiara",
        operation_type_classes: Union[Mapping[str, Type[OperationType]], None] = None,
    ):

        self._kiara: "Kiara" = kiara

        self._operation_type_classes: Union[Dict[str, Type["OperationType"]], None] = (
            None
        )

        if operation_type_classes is not None:
            self._operation_type_classes = dict(operation_type_classes)

        self._operation_type_metadata: Dict[str, OperationTypeInfo] = {}

        self._operation_types: Union[Dict[str, OperationType], None] = None

        self._operations: Union[Dict[str, Operation], None] = None
        self._operations_by_type: Union[Dict[str, List[str]], None] = None

        self._module_map: Union[Dict[str, Dict[str, Any]], None] = None

        self._invalid_operations: Dict[str, Any] = {}

    @property
    def is_initialized(self) -> bool:

        return self._operations is not None

    def get_module_map(self) -> Mapping[str, Mapping[str, Any]]:

        if not self.is_initialized:
            raise Exception(
                "Can't retrieve module map: operations not initialized yet."
            )

        if self._module_map is not None:
            return self._module_map

        module_map = {}
        for k, v in self.operations.items():
            module_map[k] = {
                "module_type": v.module_type,
                "module_config": v.module_config,
            }
        self._module_map = module_map
        return self._module_map

    @property
    def operation_types(self) -> Mapping[str, OperationType]:

        if self._operation_types is not None:
            return self._operation_types

        # TODO: support op type config
        _operation_types = {}
        for op_name, op_cls in self.operation_type_classes.items():
            try:
                _operation_types[op_name] = op_cls(
                    kiara=self._kiara, op_type_name=op_name
                )
            except Exception as e:
                log_exception(e)
                logger.debug("ignore.operation_type", operation_name=op_name, reason=e)

        self._operation_types = _operation_types
        return self._operation_types

    def get_operation_type(self, op_type: Union[str, Type[OP_TYPE]]) -> OP_TYPE:

        if not isinstance(op_type, str):
            try:
                op_type = op_type._operation_type_name  # type: ignore
            except Exception:
                raise ValueError(
                    f"Can't retrieve operation type, invalid input type '{type(op_type)}'."
                )

        if op_type not in self.operation_types.keys():
            raise Exception(
                f"No operation type '{op_type}' registered. Available operation types: {', '.join(self.operation_types.keys())}."
            )

        return self.operation_types[op_type]  # type: ignore

    def get_type_metadata(self, type_name: str) -> OperationTypeInfo:

        md = self._operation_type_metadata.get(type_name, None)
        if md is None:
            md = OperationTypeInfo.create_from_type_class(
                kiara=self._kiara, type_cls=self.operation_type_classes[type_name]
            )
            self._operation_type_metadata[type_name] = md
        return self._operation_type_metadata[type_name]

    def get_context_metadata(
        self, alias: Union[str, None] = None, only_for_package: Union[str, None] = None
    ) -> OperationTypeClassesInfo:

        result = {}
        for type_name in self.operation_type_classes.keys():
            md = self.get_type_metadata(type_name=type_name)
            if only_for_package:
                if md.context.labels.get("package") == only_for_package:
                    result[type_name] = md
            else:
                result[type_name] = md

        return OperationTypeClassesInfo(group_title=alias, item_infos=result)  # type: ignore

    @property
    def operation_type_classes(
        self,
    ) -> Mapping[str, Type["OperationType"]]:

        if self._operation_type_classes is not None:
            return self._operation_type_classes

        from kiara.utils.class_loading import find_all_operation_types

        self._operation_type_classes = find_all_operation_types()
        return self._operation_type_classes

    # @property
    # def operation_ids(self) -> List[str]:
    #     return list(self.profiles.keys())

    @property
    def operation_ids(self) -> Iterable[str]:
        return self.operations.keys()

    @property
    def operations(self) -> Mapping[str, Operation]:

        if self._operations is not None:
            return self._operations

        all_op_configs: Set[OperationConfig] = set()
        for op_type in self.operation_types.values():
            included_ops = op_type.retrieve_included_operation_configs()
            for op in included_ops:
                if isinstance(op, Mapping):
                    op = ManifestOperationConfig(**op)
                all_op_configs.add(op)

        for data_type in self._kiara.data_type_classes.values():
            if hasattr(data_type, "retrieve_included_operations"):
                included_ops = data_type.retrieve_included_operations()
                for op in included_ops:
                    if isinstance(op, Mapping):
                        op = ManifestOperationConfig(**op)
                    all_op_configs.add(op)

        operations: Dict[str, Operation] = {}
        operations_by_type: Dict[str, List[str]] = {}

        deferred_module_names: Dict[str, List[OperationConfig]] = {}

        # first iteration
        for op_config in all_op_configs:

            try:

                if isinstance(op_config, PipelineOperationConfig):
                    for mt in op_config.required_module_types:
                        if mt not in self._kiara.module_type_names:
                            deferred_module_names.setdefault(mt, []).append(op_config)
                    deferred_module_names.setdefault(
                        op_config.pipeline_name, []
                    ).append(op_config)
                    continue

            except Exception as e:
                details: Dict[str, Any] = {}
                module_id = op_config.retrieve_module_type(kiara=self._kiara)
                details["module_id"] = module_id
                if module_id == "pipeline":
                    details["pipeline_name"] = op_config.pipeline_name  # type: ignore
                msg: Union[str, Exception] = str(e)
                if not msg:
                    msg = e
                details["details"] = msg
                logger.error("invalid.operation", **details)
                self._invalid_operations[op_config.pipeline_name] = details  # type: ignore
                log_exception(e)
                continue

            try:

                module_type = op_config.retrieve_module_type(kiara=self._kiara)
                if module_type not in self._kiara.module_type_names:
                    deferred_module_names.setdefault(module_type, []).append(op_config)
                else:
                    module_config = op_config.retrieve_module_config(kiara=self._kiara)

                    manifest = Manifest(
                        module_type=module_type, module_config=module_config
                    )
                    ops = self._create_operations(manifest=manifest, doc=op_config.doc)

                    for op_type_name, _op in ops.items():
                        if _op.operation_id in operations.keys():
                            logger.debug(
                                "duplicate_operation_id",
                                op_id=_op.operation_id,
                                left_module=operations[_op.operation_id].module_type,
                                right_module=_op.module_type,
                            )
                            raise Exception(
                                f"Duplicate operation id: {_op.operation_id}"
                            )
                        operations[_op.operation_id] = _op
                        operations_by_type.setdefault(op_type_name, []).append(
                            _op.operation_id
                        )
            except Exception as e:
                details = {}
                module_id = op_config.retrieve_module_type(kiara=self._kiara)
                details["module_id"] = module_id
                if module_id == "pipeline":
                    details["pipeline_name"] = op_config.pipeline_name  # type: ignore
                msg = str(e)
                if not msg:
                    msg = e
                details["details"] = msg
                logger.error("invalid.operation", **details)
                log_exception(e)
                continue

        error_details = {}
        while deferred_module_names:

            deferred_length = len(deferred_module_names)

            remove_deferred_names = set()

            for missing_op_id in deferred_module_names.keys():
                if missing_op_id in operations.keys():
                    remove_deferred_names.add(missing_op_id)
                    continue

                for op_config in deferred_module_names[missing_op_id]:

                    try:

                        if isinstance(op_config, PipelineOperationConfig):

                            if all(
                                mt in self._kiara.module_type_names
                                or mt in operations.keys()
                                for mt in op_config.required_module_types
                            ):

                                module_map = {}
                                for mt in op_config.required_module_types:
                                    if mt in operations.keys():
                                        module_map[mt] = {
                                            "module_type": operations[mt].module_type,
                                            "module_config": operations[
                                                mt
                                            ].module_config,
                                        }
                                op_config.module_map.update(module_map)
                                module_config = op_config.retrieve_module_config(
                                    kiara=self._kiara
                                )

                                manifest = Manifest(
                                    module_type="pipeline",
                                    module_config=module_config,
                                )
                                ops = self._create_operations(
                                    manifest=manifest,
                                    doc=op_config.doc,
                                    metadata=op_config.metadata,
                                )

                            else:
                                missing = (
                                    mt
                                    for mt in op_config.required_module_types
                                    if mt not in self._kiara.module_type_names
                                    and mt not in operations.keys()
                                )
                                raise Exception(
                                    f"Can't find all required module types when processing pipeline '{missing_op_id}': {', '.join(missing)}"
                                )

                        else:
                            raise NotImplementedError(
                                f"Invalid type: {type(op_config)}"
                            )
                            # module_type = op_config.retrieve_module_type(kiara=self._kiara)
                            # module_config = op_config.retrieve_module_config(kiara=self._kiara)
                            #
                            # # TODO: merge dicts instead of update?
                            # new_module_config = dict(base_config)
                            # new_module_config.update(module_config)
                            #
                            # manifest = Manifest(module_type=operation.module_type,
                            #                       module_config=new_module_config)

                        for op_type_name, _op in ops.items():

                            if _op.operation_id in operations.keys():
                                raise Exception(
                                    f"Duplicate operation id: {_op.operation_id}"
                                )

                            operations[_op.operation_id] = _op
                            operations_by_type.setdefault(op_type_name, []).append(
                                _op.operation_id
                            )
                            assert _op.operation_id == op_config.pipeline_name

                        for _op_id in deferred_module_names.keys():
                            if op_config in deferred_module_names[_op_id]:
                                deferred_module_names[_op_id].remove(op_config)
                    except Exception as e:
                        details = {}
                        module_id = op_config.retrieve_module_type(kiara=self._kiara)
                        details["module_id"] = module_id
                        try:
                            details["module_config"] = op_config.retrieve_module_config(
                                kiara=self._kiara
                            )
                        except Exception as xe:
                            details["module_config"] = str(xe)
                        if module_id == "pipeline":
                            details["pipeline_name"] = op_config.pipeline_name  # type: ignore

                        msg = str(e)
                        if not msg:
                            msg = e
                        details["details"] = msg
                        error_details[missing_op_id] = details
                        exc_info = sys.exc_info()
                        details["parent"] = exc_info[1]

                        continue

            for name, dependencies in deferred_module_names.items():
                if not dependencies:
                    remove_deferred_names.add(name)

            for rdn in remove_deferred_names:
                deferred_module_names.pop(rdn)

            if len(deferred_module_names) == deferred_length:

                for mn in deferred_module_names:
                    if mn in operations.keys():
                        continue
                    details = error_details.get(missing_op_id, {"details": "-- n/a --"})
                    exception = details.get("parent", None)
                    if exception:
                        log_exception(exception)

                    self._invalid_operations[mn] = details
                    log_message(f"invalid.operation.{mn}", operation_id=mn, **details)
                break

        self._operations = {}
        for missing_op_id in sorted(operations.keys()):
            self._operations[missing_op_id] = operations[missing_op_id]

        self._operations_by_type = {}
        for op_type_name in sorted(operations_by_type.keys()):
            self._operations_by_type.setdefault(
                op_type_name, sorted(operations_by_type[op_type_name])
            )

        return self._operations

    def register_pipelines(self, *paths: Union[str, Path]) -> Dict[str, Operation]:
        """
        Register pipelines from one or more paths.

        Args:
        ----
            *paths: one or more paths to load pipelines from.
        """
        pipeline_data = find_pipeline_data_in_paths(
            {k if isinstance(k, str) else k.as_posix(): {} for k in paths}
        )
        duplicates = set()
        for op_id in pipeline_data.keys():
            if op_id in self.operations.keys():
                duplicates.add(op_id)

        if duplicates:
            raise Exception(
                "Can't register pipelines from the provided path(s), duplicate operation ids found: "
                + ", ".join(sorted(duplicates))
            )

        ops = {}
        for op_id, op_data in pipeline_data.items():
            # TODO: what to do with the additional data, like source and source type?
            try:
                op = self.register_pipeline(data=op_data["data"], operation_id=op_id)
            except Exception as e:
                log_message("invalid.pipeline", pipeline_id=op_id, reason=str(e))
                if is_develop():
                    renderables: List[RenderableType] = []
                    renderables.append("")
                    renderables.append(extract_renderable(e))
                    renderables.append("")
                    label = f"[red]Invalid Pipeline [/red][i]'{op_id}'[/i]"
                    terminal_print(Group(*renderables), in_panel=label)
                # log_exception(e)
                continue
            ops[op.operation_id] = op
        return ops

    def register_pipeline(
        self,
        data: Union[Path, str, Mapping[str, Any]],
        operation_id: Union[str, None] = None,
    ) -> Operation:

        if isinstance(data, Path):
            if not data.is_file():
                raise Exception(
                    f"Can't register operation from path '{data.as_posix()}: path is not a file."
                )

            pipeline_config = PipelineConfig.from_file(
                data.as_posix(), kiara=self._kiara, pipeline_name=operation_id
            )
        elif isinstance(data, Mapping):

            pipeline_config = PipelineConfig.from_config(
                pipeline_name=operation_id, data=data, kiara=self._kiara
            )
        elif isinstance(data, str):
            if os.path.isfile((os.path.realpath(data))):
                pipeline_config = PipelineConfig.from_file(
                    data, kiara=self._kiara, pipeline_name=operation_id
                )
            else:
                config_data = None
                try:
                    config_data = json.loads(data)
                except Exception:
                    try:
                        config_data = yaml.load(data)
                    except Exception:
                        pass
                if config_data:
                    pipeline_config = PipelineConfig.from_config(
                        pipeline_name=operation_id, data=config_data, kiara=self._kiara
                    )
                else:
                    raise Exception(
                        f"Can't register pipeline with id '{operation_id}': can't parse data as file path, json or yaml."
                    )
        else:
            raise Exception(
                f"Can't register pipeline with id '{operation_id}': invalid type '{type(data)}' for pipeline data: {type(data)}"
            )

        _operation_id = pipeline_config.pipeline_name
        if operation_id:
            assert _operation_id == operation_id

        if _operation_id in self.operation_ids:
            raise Exception(
                f"Can't register pipeline with id '{_operation_id}': operation id already in use."
            )

        manifest = Manifest(
            module_type="pipeline", module_config=pipeline_config.model_dump()
        )
        module = self._kiara.module_registry.create_module(manifest)

        from kiara.operations.included_core_operations.pipeline import (
            PipelineOperationDetails,
        )

        op_details = PipelineOperationDetails.create_operation_details(
            operation_id=module.config.pipeline_name,
            pipeline_inputs_schema=module.inputs_schema,
            pipeline_outputs_schema=module.outputs_schema,
            pipeline_config=module.config,
        )

        metadata: Dict[str, Any] = {}
        operation = Operation(
            module_type=manifest.module_type,
            module_config=manifest.module_config,
            operation_id=_operation_id,
            operation_details=op_details,
            module_details=KiaraModuleInstance.from_module(module),
            metadata=metadata,
            doc=pipeline_config.doc,
        )

        pc: PipelineConfig = module.config
        # make sure the pipeline can be created
        Pipeline(structure=pc.structure, kiara=self._kiara)

        operation._module = module
        assert self._operations is not None
        self._operations[_operation_id] = operation
        current_pipelines = self.operations_by_type.get("pipeline", None)
        if not current_pipelines:
            current_pipelines = []
            self._operations_by_type["pipeline"] = current_pipelines  # type: ignore

        current_pipelines.append(_operation_id)  # type: ignore
        assert self._operations_by_type is not None
        # self._operations_by_type["pipeline"] = sorted(current_pipelines)

        logger.debug("pipeline.registered", operation_id=_operation_id)
        return operation

    def _create_operations(
        self,
        manifest: Manifest,
        doc: Any,
        metadata: Union[Mapping[str, Any], None] = None,
    ) -> Dict[str, Operation]:

        module = self._kiara.module_registry.create_module(manifest)
        op_types = {}

        if metadata is None:
            metadata = {}

        for op_name, op_type in self.operation_types.items():

            op_details = op_type.check_matching_operation(module=module)
            if not op_details:
                continue

            operation = Operation(
                module_type=manifest.module_type,
                module_config=manifest.module_config,
                operation_id=op_details.operation_id,
                operation_details=op_details,
                module_details=KiaraModuleInstance.from_module(module),
                metadata=metadata,
                doc=doc,
            )
            operation._module = module

            op_types[op_name] = operation

        return op_types

    def get_operation(self, operation_id: str) -> Operation:

        if operation_id not in self.operation_ids:
            if operation_id in self._invalid_operations.keys():
                raise InvalidOperationException(self._invalid_operations[operation_id])
            else:
                raise NoSuchOperationException(
                    operation_id=operation_id,
                    available_operations=sorted(self.operation_ids),
                )

        op = self.operations[operation_id]
        return op

    def find_all_operation_types(self, operation_id: str) -> Set[str]:

        result = set()
        for op_type, ops in self.operations_by_type.items():
            if operation_id in ops:
                result.add(op_type)

        return result

    @property
    def operations_by_type(self) -> Mapping[str, Iterable[str]]:

        if self._operations_by_type is None:
            self.operations
        return self._operations_by_type  # type: ignore

    def find_operation_id(self, manifest: Manifest) -> Union[str, None]:

        for op in self.operations.values():
            if manifest.manifest_cid == op.manifest_cid:
                return op.operation_id

        return None
