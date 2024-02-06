# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import copy
import uuid
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Mapping, Union

from pydantic import Field, PrivateAttr

from kiara.defaults import SpecialValue
from kiara.models.module.manifest import Manifest
from kiara.models.python_class import KiaraModuleInstance
from kiara.models.values.value_schema import ValueSchema
from kiara.registries.ids import ID_REGISTRY

if TYPE_CHECKING:
    from kiara.context import Kiara
    from kiara.models.values.value import Value
    from kiara.modules import KiaraModule


class Destiny(Manifest):
    """
    A destiny is basically a link to a potential future transformation result involving one or several values as input.

    It is immutable, once executed, each of the input values can only have one destiny with a specific alias.
    This is similar to what is usually called a 'future' in programming languages, but more deterministic, sorta.
    """

    _kiara_model_id: ClassVar = "instance.destiny"

    @classmethod
    def create_from_values(
        cls,
        kiara: "Kiara",
        destiny_alias: str,
        values: Mapping[str, uuid.UUID],
        manifest: Manifest,
        result_field_name: Union[str, None] = None,
    ) -> "Destiny":

        module = kiara.module_registry.create_module(manifest=manifest)

        if result_field_name is None:
            if len(module.outputs_schema) != 1:
                raise Exception(
                    f"Can't determine result field name for module, not provided, and multiple outputs available for module '{module.module_type_name}': {', '.join(module.outputs_schema.keys())}."
                )

            result_field_name = next(iter(module.outputs_schema.keys()))

        result_schema = module.outputs_schema.get(result_field_name, None)
        if result_schema is None:
            raise Exception(
                f"Can't determine result schema for module '{module.module_type_name}', result field '{result_field_name}' not available. Available field: {', '.join(module.outputs_schema.keys())}"
            )

        fixed_inputs = {}
        deferred_inputs: Dict[str, None] = {}
        for field in module.inputs_schema.keys():
            if field in values.keys():
                fixed_inputs[field] = values[field]
            else:
                deferred_inputs[field] = None

        module_details = KiaraModuleInstance.from_module(module=module)

        # TODO: check whether it'd be better to 'resolve' the module config, as this might change the resulting hash
        destiny_id: uuid.UUID = ID_REGISTRY.generate(obj_type=Destiny)
        destiny = Destiny(
            destiny_id=destiny_id,
            destiny_alias=destiny_alias,
            module_details=module_details,
            module_type=manifest.module_type,
            module_config=manifest.module_config,
            result_field_name=result_field_name,
            result_schema=result_schema,
            fixed_inputs=fixed_inputs,
            inputs_schema=dict(module.inputs_schema),
            deferred_inputs=deferred_inputs,
            result_value_id=None,
        )
        destiny._module = module
        ID_REGISTRY.update_metadata(destiny_id, obj=destiny)
        return destiny

    destiny_id: uuid.UUID = Field(description="The id of this destiny.")

    destiny_alias: str = Field(description="The path to (the) destiny.")
    module_details: KiaraModuleInstance = Field(
        description="The class of the underlying module."
    )
    fixed_inputs: Dict[str, uuid.UUID] = Field(
        description="Inputs that are known in advance."
    )
    inputs_schema: Dict[str, ValueSchema] = Field(
        description="The schemas of all deferred input fields."
    )
    deferred_inputs: Dict[str, Union[uuid.UUID, None]] = Field(
        description="Potentially required external inputs that are needed for this destiny to materialize."
    )
    result_field_name: str = Field(description="The name of the result field.")
    result_schema: ValueSchema = Field(description="The value schema of the result.")
    result_value_id: Union[uuid.UUID, None] = Field(
        description="The value id of the result."
    )

    _is_stored: bool = PrivateAttr(default=False)
    _job_id: Union[uuid.UUID, None] = PrivateAttr(default=None)

    _merged_inputs: Union[Dict[str, uuid.UUID], None] = PrivateAttr(default=None)
    # _job_config_hash: Optional[int] = PrivateAttr(default=None)
    _module: Union["KiaraModule", None] = PrivateAttr(default=None)

    def _retrieve_id(self) -> str:
        return str(self.destiny_id)

    def _retrieve_data_to_hash(self) -> Any:
        return self.destiny_id.bytes

    # @property
    # def job_config_hash(self) -> int:
    #     if self._job_config_hash is None:
    #         self._job_config_hash = self._retrieve_job_config_hash()
    #     return self._job_config_hash

    @property
    def merged_inputs(self) -> Mapping[str, uuid.UUID]:

        if self._merged_inputs is not None:
            return self._merged_inputs

        result = copy.copy(self.fixed_inputs)
        missing = []
        for k in self.inputs_schema.keys():
            if k in self.fixed_inputs.keys():
                if k in self.deferred_inputs.keys():
                    raise Exception(
                        f"Destiny input field '{k}' present in both fixed and deferred inputs, this is invalid."
                    )
                else:
                    continue
            v = self.deferred_inputs.get(k, None)
            if v is None or isinstance(v, SpecialValue):
                missing.append(k)
            else:
                result[k] = v

        if missing:
            raise Exception(
                f"Destiny not valid (yet), missing inputs: {', '.join(missing)}"
            )

        self._merged_inputs = result
        return self._merged_inputs

    @property
    def module(self) -> "KiaraModule":
        if self._module is None:
            m_cls = self.module_details.get_class()
            self._module = m_cls(module_config=self.module_config)
        return self._module

    def execute(self, kiara: "Kiara") -> "Value":

        if self.result_value_id is not None:
            raise Exception("Destiny already resolved.")

        results = kiara.job_registry.execute_and_retrieve(
            manifest=self, inputs=self.merged_inputs
        )
        value = results.get_value_obj(field_name=self.result_field_name)

        self.result_value_id = value.value_id
        return value

    # def _retrieve_job_config_hash(self) -> int:
    #     obj = {"module_config": self.manifest_data, "inputs": self.merged_inputs}
    #     return compute_cid(obj)
