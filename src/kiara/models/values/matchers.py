# -*- coding: utf-8 -*-
from pydantic import Field
from typing import TYPE_CHECKING, Any, List, Union

from kiara.models import KiaraModel
from kiara.models.values.value import Value

if TYPE_CHECKING:
    from kiara.context import Kiara


class ValueMatcher(KiaraModel):
    """An object describing requirements values should satisfy in order to be included in a query result."""

    @classmethod
    def create_matcher(self, **match_options: Any):

        m = ValueMatcher(**match_options)
        return m

    data_types: List[str] = Field(description="The data type.", default_factory=list)
    allow_sub_types: bool = Field(description="Allow subtypes.", default=True)
    min_size: int = Field(description="The minimum size for the dataset.", default=0)
    max_size: Union[None, int] = Field(
        description="The maximum size for the dataset.", default=None
    )
    allow_internal: bool = Field(
        description="Allow internal data types.", default=False
    )
    has_alias: bool = Field(
        description="Value must have at least one alias.", default=True
    )

    def is_match(self, value: Value, kiara: "Kiara") -> bool:
        if self.data_types:
            match = False
            if not self.allow_sub_types:
                for data_type in self.data_types:
                    if data_type == value.data_type_name:
                        match = True
                        break
            else:
                if value.data_type_name not in kiara.type_registry.data_type_names:
                    return False
                lineage = kiara.type_registry.get_type_lineage(value.data_type_name)
                for data_type in self.data_types:
                    if data_type in lineage:
                        match = True
                        break
            if not match:
                return False

        if self.min_size:
            if value.value_size < self.min_size:
                return False
        if self.max_size:
            if value.value_size > self.max_size:
                return False

        if not self.allow_internal:
            if kiara.type_registry.is_internal_type(
                data_type_name=value.data_type_name
            ):
                return False

        if self.has_alias:
            aliases = kiara.alias_registry.find_aliases_for_value_id(
                value_id=value.value_id
            )
            if not aliases:
                return False

        return True
