# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Any, List, Union

from pydantic import Field, field_validator

from kiara.defaults import DEFAULT_DATA_STORE_MARKER, DEFAULT_STORE_MARKER
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
    alias_matchers: Union[None, List[str]] = Field(
        description="Values must have an alias that matches one of the provided matchers. Assumes 'has_alias' is set to 'True'.",
        default=None,
    )
    in_data_archives: Union[None, List[str]] = Field(
        description="A list of registered names of archives the value must be in. If 'None', all archives will be used.",
        default=None,
    )

    @field_validator("in_data_archives", mode="before")
    @classmethod
    def validate_in_archives(cls, v):

        if v is None:
            return v
        elif isinstance(v, str):
            v = set(v)
        else:
            v = set(v)

        if DEFAULT_STORE_MARKER in v or DEFAULT_DATA_STORE_MARKER in v:
            v.add(DEFAULT_DATA_STORE_MARKER)
            v.add(DEFAULT_STORE_MARKER)

        return list(v)

    @field_validator("alias_matchers")
    @classmethod
    def validate_matchers(cls, v):
        if v is None:
            return v
        elif isinstance(v, str):
            return [v]
        else:
            return list(v)

    def is_match(self, value: Value, kiara: "Kiara") -> bool:

        has_alias = self.has_alias or self.alias_matchers

        match = False
        if self.data_types:
            if not self.allow_sub_types:
                for data_type in self.data_types:
                    if data_type == value.data_type_name:
                        match = True
                        break
            else:
                if value.data_type_name not in kiara.type_registry.get_data_type_names(
                    include_profiles=True
                ):
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

        if has_alias:

            aliases = kiara.alias_registry.find_aliases_for_value_id(
                value_id=value.value_id
            )
            if not aliases:
                return False

            if self.alias_matchers:
                for token in self.alias_matchers:
                    for alias in aliases:
                        if token in alias:
                            return True

                return False

        return True
