# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from typing import TYPE_CHECKING, ClassVar, Dict, Iterable, List, Mapping, Type, Union

import structlog
from pydantic import Field, PrivateAttr

from kiara.exceptions import KiaraException
from kiara.models.documentation import DocumentationMetadataModel
from kiara.models.module.operation import (
    ManifestOperationConfig,
    OperationConfig,
    OperationDetails,
    OperationSchema,
)
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import KiaraModule
from kiara.operations import OperationType

if TYPE_CHECKING:
    from multiformats import CID

    from kiara.context import Kiara

logger = structlog.getLogger()


class CustomModuleOperationDetails(OperationDetails):
    @classmethod
    def create_from_module(cls, module: KiaraModule):

        return CustomModuleOperationDetails(
            operation_id=module.module_type_name,
            module_inputs_schema=module.inputs_schema,
            module_outputs_schema=module.outputs_schema,
        )

    module_inputs_schema: Mapping[str, ValueSchema] = Field(
        description="The input schemas of the module."
    )
    module_outputs_schema: Mapping[str, ValueSchema] = Field(
        description="The output schemas of the module."
    )
    _op_schema: OperationSchema = PrivateAttr(default=None)

    def get_operation_schema(self) -> OperationSchema:

        if self._op_schema is not None:
            return self._op_schema

        self._op_schema = OperationSchema(
            alias=self.operation_id,
            inputs_schema=self.module_inputs_schema,
            outputs_schema=self.module_outputs_schema,
        )
        return self._op_schema


class CustomModuleOperationType(OperationType[CustomModuleOperationDetails]):

    _operation_type_name: ClassVar[str] = "custom_module"

    def __init__(self, kiara: "Kiara", op_type_name: str):
        self._included_operations_cache: Dict[type, List["ManifestOperationConfig"]] = (
            {}
        )
        self._included_operations_lookup_cache: Dict[type, Dict["CID", str]] = {}

        super().__init__(kiara=kiara, op_type_name=op_type_name)

    def _retrieve_included_operations(
        self, module_cls: Type[KiaraModule]
    ) -> List["ManifestOperationConfig"]:

        if self._included_operations_cache.get(module_cls, None) is not None:
            return self._included_operations_cache.get(module_cls)  # type: ignore

        from kiara.models.module.operation import ManifestOperationConfig

        this_module_type_name: str = module_cls._module_type_name  # type: ignore

        doc = None
        cache: List[ManifestOperationConfig] = []
        lookup_cache: Dict["CID", str] = {}

        op_ids: List[str] = []

        if hasattr(module_cls, "retrieve_included_operations"):
            manifests = module_cls.retrieve_included_operations()  # type: ignore
            for op_id, op in manifests.items():

                if op_id in op_ids:
                    raise KiaraException(
                        msg=f"Included operation '{op_id}' invalid.",
                        reason="Duplicate operation id.",
                    )
                op_ids.append(op_id)

                if isinstance(op, Mapping):
                    mtn = op.get("module_type", None)
                    if not mtn:
                        op = dict(op)
                        op["module_type"] = this_module_type_name
                        if "doc" not in op.keys():
                            if doc is None:
                                doc = DocumentationMetadataModel.from_class_doc(
                                    module_cls
                                )
                            op["doc"] = doc
                    elif not mtn != this_module_type_name:
                        raise KiaraException(
                            msg=f"Included operation '{op_id}' invalid.",
                            reason=f"module_type must be empty or set to the name '{this_module_type_name}'.",
                        )
                    mopc = ManifestOperationConfig(**op)
                elif not isinstance(op, ManifestOperationConfig):
                    raise KiaraException(
                        msg=f"Included operation '{op_id}' invalid.",
                        reason="Must be a Mapping or ManifestOperationConfig instance.",
                    )
                else:
                    mopc = op

                cache.append(mopc)
                resolved = self._kiara.module_registry.resolve_manifest(
                    mopc.get_manifest()
                )
                mopc._manifest_cache = resolved
                lookup_cache[resolved.manifest_cid] = op_id

        if (
            this_module_type_name not in op_ids
            and not module_cls._config_cls.requires_config()
        ):
            doc = DocumentationMetadataModel.from_class_doc(module_cls)
            mopc = ManifestOperationConfig(module_type=this_module_type_name, doc=doc)
            resolved = self._kiara.module_registry.resolve_manifest(mopc.get_manifest())
            mopc._manifest_cache = resolved

            cache.append(mopc)
            lookup_cache[resolved.manifest_cid] = this_module_type_name

        self._included_operations_cache[module_cls] = cache
        self._included_operations_lookup_cache[module_cls] = lookup_cache
        return self._included_operations_cache[module_cls]

    def retrieve_included_operation_configs(
        self,
    ) -> Iterable[Union[Mapping, OperationConfig]]:

        result = []
        for name, module_cls in self._kiara.module_type_classes.items():
            configs = self._retrieve_included_operations(module_cls=module_cls)
            result.extend(configs)

        return result

    def check_matching_operation(
        self, module: "KiaraModule"
    ) -> Union[CustomModuleOperationDetails, None]:

        op_id: Union[str, None] = None
        if not module.is_pipeline():
            manifest_cid = module.manifest.manifest_cid
            op_id = self._included_operations_lookup_cache[module.__class__].get(
                manifest_cid
            )

        if not op_id:
            return None

        is_internal = module.characteristics.is_internal
        # inputs_map = {k: k for k in module.inputs_schema.keys()}
        # outputs_map = {k: k for k in module.outputs_schema.keys()}
        op_details: CustomModuleOperationDetails = (
            CustomModuleOperationDetails.create_operation_details(
                operation_id=op_id,
                module_inputs_schema=module.inputs_schema,
                module_outputs_schema=module.outputs_schema,
                is_internal_operation=is_internal,
            )
        )
        return op_details
