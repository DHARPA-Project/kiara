import copy
import uuid
from typing import Dict, Optional, Mapping, List, Union, TYPE_CHECKING, Any

from pydantic import Field, PrivateAttr

from kiara.defaults import SpecialValue, DESTINY_CATEGORY_ID
from kiara.models.module.manifest import Manifest
from kiara.models.values.value import Value
from kiara.models.values.value_schema import ValueSchema
from kiara.utils.hashing import compute_hash

if TYPE_CHECKING:
    from kiara.kiara import Kiara


class Destiny(Manifest):
    """A destiny is basically a link to a potential future transformation result involving one or several values as input.

    It is immutable, once executed, each of the input values can only have one destiny with a specific category/key combination.
    This is similar to what is usually called a 'future' in programming languages, but more deterministic.
    """

    @classmethod
    def create_from_values(cls, kiara: "Kiara", category: str, key: str, values: Mapping[str, Union[Value, uuid.UUID]], manifest: Manifest, result_field_name: Optional[str]=None):

        module = kiara.create_module(manifest=manifest)

        if result_field_name is None:
            if len(module.outputs_schema) != 1:
                raise Exception(f"Can't determine result field name for module, not provided, and multiple outputs available for module '{module.module_type_name}': {', '.join(module.outputs_schema.keys())}.")

            result_field_name = next(iter(module.outputs_schema.keys()))

        result_schema = module.outputs_schema.get(result_field_name, None)
        if result_schema is None:
            raise Exception(f"Can't determine result schema for module '{module.module_type_name}', result field '{result_field_name}' not available. Available field: {', '.join(module.outputs_schema.keys())}")

        fixed_inputs = {}
        deferred_inputs = {}
        for field in module.inputs_schema.keys():
            if field in values.keys():
                if isinstance(values[field], uuid.UUID):
                    fixed_inputs[field] = values[field]
                else:
                    fixed_inputs[field] = values[field].value_id
            else:
                deferred_inputs[field] = None

        # TODO: check whether it'd be better to 'resolve' the module config, as this might change the resulting hash
        destiny_id: uuid.UUID = uuid.uuid4()
        return Destiny(destiny_id=destiny_id, category=category, key=key, module_type=manifest.module_type, module_config=manifest.module_config, result_field_name=result_field_name, result_schema=result_schema, fixed_inputs=fixed_inputs, inputs_schema=dict(module.inputs_schema), deferred_inputs=deferred_inputs, result_value_id=None)

    destiny_id: uuid.UUID = Field(description="The id of this destiny.")
    category: str = Field(description="The category name of this destiny.")
    key: str = Field(description="The key within the category.")
    fixed_inputs: Dict[str, uuid.UUID] = Field(description="Inputs that are known in advance.")
    inputs_schema: Dict[str, ValueSchema] = Field(description="The schemas of all deferred input fields.")
    deferred_inputs: Dict[str, Union[None, SpecialValue, uuid.UUID]] = Field(description="Potentially required external inputs that are needed for this destiny to materialize.")
    result_field_name: str = Field(description="The name of the result field.")
    result_schema: ValueSchema = Field(description="The value schema of the result.")
    result_value_id: Optional[uuid.UUID] = Field(description="The value_id/result of the computed destiny. If not materialized yet, this is 'None'.")

    _merged_inputs: Optional[Dict[str, uuid.UUID]] = PrivateAttr(default=None)
    _job_config_hash: Optional[int] = PrivateAttr(default=None)

    def _retrieve_id(self) -> str:
        return str(self.destiny_id)

    def _retrieve_category_id(self) -> str:
        return DESTINY_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return self.destiny_id

    @property
    def job_config_hash(self) -> int:
        if self._job_config_hash is None:
            self._job_config_hash = self._retrieve_job_config_hash()
        return self._job_config_hash

    @property
    def merged_inputs(self) -> Mapping[str, uuid.UUID]:

        if self._merged_inputs is not None:
            return self._merged_inputs

        result = copy.copy(self.fixed_inputs)
        missing = []
        for k in self.inputs_schema.keys():
            if k in self.fixed_inputs.keys():
                if k in self.deferred_inputs.keys():
                    raise Exception(f"Destiny input field '{k}' present in both fixed and deferred inputs, this is invalid.")
                else:
                    continue
            v = self.deferred_inputs.get(k, None)
            if v is None:
                missing.append(k)
            result[k] = v

        if missing:
            raise Exception(f"Destiny not valid (yet), missing inputs: {', '.join(missing)}")

        self._merged_inputs = result
        return self._merged_inputs


    def _retrieve_job_config_hash(self) -> int:
        obj = {
            "module_config": self.manifest_data,
            "inputs": self.merged_inputs
        }
        return compute_hash(obj)

