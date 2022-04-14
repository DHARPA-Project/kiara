# -*- coding: utf-8 -*-
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Type,
)

from kiara.models.module import KiaraModuleClass
from kiara.models.module.manifest import Manifest
from kiara.models.module.operation import (
    ManifestOperationConfig,
    Operation,
    OperationConfig,
    OperationTypeClassesInfo,
    OperationTypeInfo,
    PipelineOperationConfig,
)
from kiara.modules.operations import OperationType

if TYPE_CHECKING:
    from kiara.kiara import Kiara


class OperationRegistry(object):
    def __init__(
        self,
        kiara: "Kiara",
        operation_type_classes: Optional[Mapping[str, Type[OperationType]]] = None,
    ):

        self._kiara: "Kiara" = kiara

        self._operation_type_classes: Optional[Dict[str, Type["OperationType"]]] = None

        if operation_type_classes is not None:
            self._operation_type_classes = dict(operation_type_classes)

        self._operation_type_metadata: Dict[str, OperationTypeInfo] = {}

        self._operation_types: Optional[Dict[str, OperationType]] = None

        self._operations: Optional[Dict[str, Operation]] = None
        self._operations_by_type: Optional[Dict[str, Iterable[str]]] = None

    @property
    def operation_types(self) -> Mapping[str, OperationType]:

        if self._operation_types is not None:
            return self._operation_types

        # TODO: support op type config
        _operation_types = {}
        for op_name, op_cls in self.operation_type_classes.items():
            _operation_types[op_name] = op_cls(kiara=self._kiara, op_type_name=op_name)

        self._operation_types = _operation_types
        return self._operation_types

    def get_operation_type(self, op_type: str) -> OperationType:

        if op_type not in self.operation_types.keys():
            raise Exception(
                f"No operation type '{op_type}' registered. Available operation types: {', '.join(self.operation_types.keys())}."
            )

        return self.operation_types[op_type]

    def get_type_metadata(self, type_name: str) -> OperationTypeInfo:

        md = self._operation_type_metadata.get(type_name, None)
        if md is None:
            md = OperationTypeInfo.create_from_type_class(
                type_cls=self.operation_type_classes[type_name]
            )
            self._operation_type_metadata[type_name] = md
        return self._operation_type_metadata[type_name]

    def get_context_metadata(
        self, alias: Optional[str] = None, only_for_package: Optional[str] = None
    ) -> OperationTypeClassesInfo:

        result = {}
        for type_name in self.operation_type_classes.keys():
            md = self.get_type_metadata(type_name=type_name)
            if only_for_package:
                if md.context.labels.get("package") == only_for_package:
                    result[type_name] = md
            else:
                result[type_name] = md

        return OperationTypeClassesInfo.construct(group_alias=alias, type_infos=result)  # type: ignore

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
                for op in all_op_configs:
                    if isinstance(op, Mapping):
                        op = ManifestOperationConfig(**op)
                    all_op_configs.add(op)

        operations: Dict[str, Operation] = {}
        operations_by_type: Dict[str, List[str]] = {}

        deferred_module_names: Dict[str, List[OperationConfig]] = {}

        # first iteration
        for op_config in all_op_configs:

            if isinstance(op_config, PipelineOperationConfig):
                for mt in op_config.required_module_types:
                    if mt not in self._kiara.module_type_names:
                        deferred_module_names.setdefault(mt, []).append(op_config)
                deferred_module_names.setdefault(op_config.pipeline_id, []).append(
                    op_config
                )
                continue

            module_type = op_config.retrieve_module_type(kiara=self._kiara)
            if module_type not in self._kiara.module_type_names:
                deferred_module_names.setdefault(module_type, []).append(op_config)
            else:
                module_config = op_config.retrieve_module_config(kiara=self._kiara)
                manifest = Manifest.construct(
                    module_type=module_type, module_config=module_config
                )

                ops = self._create_operations(manifest=manifest, doc=op_config.doc)
                for op_type_name, _op in ops.items():
                    if _op.operation_id in operations.keys():
                        raise Exception(f"Duplicate operation id: {_op.operation_id}")
                    operations[_op.operation_id] = _op
                    operations_by_type.setdefault(op_type_name, []).append(
                        _op.operation_id
                    )

        while deferred_module_names:

            deferred_length = len(deferred_module_names)

            remove_deferred_names = set()

            for missing_op_id in deferred_module_names.keys():
                if missing_op_id in operations.keys():
                    continue

                for op_config in deferred_module_names[missing_op_id]:

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
                                        "module_config": operations[mt].module_config,
                                    }

                            op_config.module_map.update(module_map)
                            module_config = op_config.retrieve_module_config(
                                kiara=self._kiara
                            )

                            manifest = Manifest.construct(
                                module_type="pipeline", module_config=module_config
                            )
                            ops = self._create_operations(
                                manifest=manifest, doc=op_config.doc
                            )

                    else:
                        raise NotImplementedError()
                        # module_type = op_config.retrieve_module_type(kiara=self._kiara)
                        # module_config = op_config.retrieve_module_config(kiara=self._kiara)
                        #
                        # # TODO: merge dicts instead of update?
                        # new_module_config = dict(base_config)
                        # new_module_config.update(module_config)
                        #
                        # manifest = Manifest.construct(module_type=operation.module_type,
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
                        assert _op.operation_id == op_config.pipeline_id

                    for _op_id in deferred_module_names.keys():
                        if op_config in deferred_module_names[_op_id]:
                            deferred_module_names[_op_id].remove(op_config)

            for name, dependencies in deferred_module_names.items():
                if not dependencies:
                    remove_deferred_names.add(name)

            for rdn in remove_deferred_names:
                deferred_module_names.pop(rdn)

            if len(deferred_module_names) == deferred_length:
                raise Exception(
                    f"Can't resolve module name(s): {', '.join(deferred_module_names.keys())}"
                )

        self._operations = {}
        for missing_op_id in sorted(operations.keys()):
            self._operations[missing_op_id] = operations[missing_op_id]

        self._operations_by_type = {}
        for op_type_name in sorted(operations_by_type.keys()):
            self._operations_by_type.setdefault(
                op_type_name, sorted(operations_by_type[op_type_name])
            )

        return self._operations

    def _create_operations(self, manifest: Manifest, doc: Any) -> Dict[str, Operation]:

        module = self._kiara.create_module(manifest)
        op_types = {}
        for op_name, op_type in self.operation_types.items():

            op_details = op_type.check_matching_operation(module=module)
            if not op_details:
                continue

            operation = Operation(
                module_type=manifest.module_type,
                module_config=manifest.module_config,
                operation_id=op_details.operation_id,
                operation_details=op_details,
                module_details=KiaraModuleClass.from_module(module),
                doc=doc,
            )
            operation._module = module

            op_types[op_name] = operation

        return op_types

    def get_operation(self, operation_id: str) -> Operation:

        if operation_id not in self.operation_ids:
            raise Exception(f"No operation registered with id: {operation_id}")

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
            self.operations  # noqa
        return self._operations_by_type  # type: ignore
