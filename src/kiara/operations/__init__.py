# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import abc
from typing import TYPE_CHECKING, Generic, Iterable, Mapping, TypeVar, Union

from kiara.interfaces.python_api.models.info import OperationTypeInfo
from kiara.models.module.operation import Operation, OperationConfig, OperationDetails

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.modules import KiaraModule


OPERATION_TYPE_DETAILS = TypeVar("OPERATION_TYPE_DETAILS", bound=OperationDetails)


class OperationType(abc.ABC, Generic[OPERATION_TYPE_DETAILS]):
    def __init__(self, kiara: "Kiara", op_type_name: str):
        self._kiara: Kiara = kiara
        self._op_type_name: str = op_type_name

    @property
    def operations(self) -> Mapping[str, Operation]:
        return {
            op_id: self._kiara.operation_registry.get_operation(op_id)
            for op_id in self._kiara.operation_registry.operations_by_type[
                self._op_type_name
            ]
        }

    def retrieve_included_operation_configs(
        self,
    ) -> Iterable[Union[Mapping, OperationConfig]]:
        return []

    @abc.abstractmethod
    def check_matching_operation(
        self, module: "KiaraModule"
    ) -> Union[OPERATION_TYPE_DETAILS, None]:
        """Check whether the provided module is a valid operation for this type."""

    def retrieve_operation_details(
        self, operation: Union[Operation, str]
    ) -> OPERATION_TYPE_DETAILS:
        """Retrieve operation details for provided operation.

        This is really just a utility method, to make the type checker happy.
        """

        if isinstance(operation, str):
            operation = self.operations[operation]

        return operation.operation_details  # type: ignore

    def create_renderable(self, **config):

        info = OperationTypeInfo.create_from_type_class(
            kiara=None, type_cls=self.__class__
        )
        return info.create_renderable(**config)
