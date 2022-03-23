# -*- coding: utf-8 -*-
import abc
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    Iterable,
    Mapping,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
)

from kiara.models.module.operation import (
    BaseOperationDetails,
    Operation,
    OperationConfig,
)
from kiara.models.python_class import PythonClass

if TYPE_CHECKING:
    from kiara import Kiara, KiaraModule


OPERATION_TYPE_DETAILS = TypeVar("OPERATION_TYPE_DETAILS", bound=BaseOperationDetails)


class OperationType(abc.ABC, Generic[OPERATION_TYPE_DETAILS]):
    def __init__(self, kiara: "Kiara", op_type_name: str):
        self._kiara: Kiara = kiara
        self._op_type_name: str = op_type_name

    @property
    def operations(self) -> Mapping[str, Operation]:
        return self._kiara.operations_mgmt.operations_by_type[self._op_type_name]

    def retrieve_included_operation_configs(
        self,
    ) -> Iterable[Union[Mapping, OperationConfig]]:
        return []

    @abc.abstractmethod
    def check_matching_operation(
        self, module: "KiaraModule"
    ) -> Optional[OPERATION_TYPE_DETAILS]:
        """Check whether the provided module is a valid operation for this type."""

    def retrieve_operation_details(
        self, operation: Operation
    ) -> OPERATION_TYPE_DETAILS:
        """Retrieve operation details for provided operation.

        This is really just a utility method, to make the type checker happy.
        """
        return operation.operation_details


class OperationsMgmt(object):
    def __init__(
        self,
        kiara: "Kiara",
        operation_type_classes: Optional[Mapping[str, Type[OperationType]]] = None,
    ):

        self._kiara: "Kiara" = kiara

        self._operation_type_classes: Optional[Dict[str, Type["OperationType"]]] = None

        if operation_type_classes is not None:
            self._operation_type_classes = dict(operation_type_classes)

        self._operation_types: Optional[Dict[str, OperationType]] = None

        # self._profiles: Optional[Dict[str, Operation]] = None
        self._operations: Optional[Dict[str, Operation]] = None
        self._operations_by_type: Optional[Dict[str, Dict[str, Operation]]] = None

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

    def find_operation_types_for_package(
        self, package_name: str
    ) -> Dict[str, Type[OperationType]]:

        result = {}
        for data_type_name, data_type in self.operation_type_classes.items():
            value_md = data_type.get_type_metadata()
            package = value_md.context.labels.get("package")
            if package == package_name:
                result[data_type_name] = data_type

        return result

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

        all_op_configs = set()
        for op_type in self.operation_types.values():
            included_ops = op_type.retrieve_included_operation_configs()
            for op in included_ops:
                if isinstance(op, Mapping):
                    op = OperationConfig(**op)
                all_op_configs.add(op)

        for data_type in self._kiara.data_type_classes.values():
            if hasattr(data_type, "retrieve_included_operations"):
                for op in all_op_configs:
                    if isinstance(op, Mapping):
                        op = OperationConfig(**op)
                    all_op_configs.add(op)

        operations = {}
        operations_by_type = {}
        for op_config in all_op_configs:
            module = self._kiara.create_module(op_config)

            op_types = {}
            for op_name, op_type in self.operation_types.items():
                operations_by_type.setdefault(op_name, {})
                op_details = op_type.check_matching_operation(module=module)
                if not op_details:
                    continue
                op_types[op_name] = op_details

            m_cls = PythonClass.from_class(module.__class__)
            for op_type, details in op_types.items():
                if details.operation_id in operations.keys():
                    raise Exception(f"Duplicate operation id: {details.operation_id}")

                operation = Operation(
                    module_type=op_config.module_type,
                    module_config=op_config.module_config,
                    operation_id=details.operation_id,
                    operation_type=op_type,
                    operation_details=details,
                    module_class=m_cls,
                    doc=op_config.doc,
                )
                operation._module = module
                operations[operation.operation_id] = operation
                operations_by_type[op_type][operation.operation_id] = operation

        self._operations = {}
        for op_id in sorted(operations.keys()):
            self._operations[op_id] = operations[op_id]

        self._operations_by_type = {}
        for op_type in sorted(operations_by_type.keys()):
            self._operations_by_type.setdefault(op_type, {})
            for op_id in sorted(operations_by_type[op_type].keys()):
                self._operations_by_type.setdefault(op_type, {})[
                    op_id
                ] = operations_by_type[op_type][op_id]

        return self._operations

    def get_operation(self, operation_id: str) -> Operation:

        if operation_id not in self.operations.keys():
            raise Exception(f"No operation registered with id: {operation_id}")

        return self.operations[operation_id]

    def find_all_operation_types(self, operation_id: str) -> Set[str]:

        m_hash = self.get_operation(
            operation_id=operation_id
        ).module.module_instance_hash

        return set(
            [
                op.operation_type
                for op in self.operations.values()
                if op.module.module_instance_hash == m_hash
            ]
        )

    @property
    def operations_by_type(self) -> Mapping[str, Mapping[str, Operation]]:

        if self._operations_by_type is None:
            self.operations  # noqa
        return self._operations_by_type  # type: ignore

    def apply_operation(self, operation_type: str, **op_args: Any):

        if self._operations is None:
            self.operations  # noqa

        op_type = self.operation_types.get(operation_type, None)
        if op_type is None:
            raise Exception(
                f"Can't apply operation, operation type '{operation_type}' not registered."
            )

        result = op_type.apply(**op_args)
        return result
