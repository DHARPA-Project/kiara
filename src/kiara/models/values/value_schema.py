# -*- coding: utf-8 -*-
import typing
from pydantic import Field
from pydantic.class_validators import validator

from kiara.defaults import VALUE_SCHEMA_CATEGORY_ID, SpecialValue
from kiara.models import KiaraModel
from kiara.models.documentation import DocumentationMetadataModel


class ValueSchema(KiaraModel):
    """The schema of a value.

    The schema contains the [ValueTypeOrm][kiara.data.values.ValueTypeOrm] of a value, as well as an optional default that
    will be used if no user input was given (yet) for a value.

    For more complex container data_types like array, tables, unions etc, data_types can also be configured with values from the ``type_config`` field.
    """

    class Config:
        use_enum_values = True
        # extra = Extra.forbid

    type: str = Field(description="The type of the value.")
    type_config: typing.Dict[str, typing.Any] = Field(
        description="Configuration for the type, in case it's complex.",
        default_factory=dict,
    )
    default: typing.Any = Field(
        description="A default value.", default=SpecialValue.NOT_SET
    )

    optional: bool = Field(
        description="Whether this value is required (True), or whether 'None' value is allowed (False).",
        default=False,
    )
    is_constant: bool = Field(
        description="Whether the value is a constant.", default=False
    )

    doc: DocumentationMetadataModel = Field(
        default="-- n/a --",
        description="A description for the value of this input field.",
    )

    @validator("doc", pre=True)
    def validate_doc(cls, value):
        return DocumentationMetadataModel.create(value)

    def _retrieve_id(self) -> str:
        return str(self.model_data_hash)

    def _retrieve_category_id(self) -> str:
        return VALUE_SCHEMA_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> typing.Any:

        return {"type": self.type, "type_config": self.type_config}

    def is_required(self):

        if self.optional:
            return False
        else:
            if self.default in [None, SpecialValue.NOT_SET, SpecialValue.NO_VALUE]:
                return True
            else:
                return False

    # def validate_types(self, kiara: "Kiara"):
    #
    #     if self.type not in kiara.value_type_names:
    #         raise ValueError(
    #             f"Invalid value type '{self.type}', available data_types: {kiara.value_type_names}"
    #         )

    def __eq__(self, other):

        if not isinstance(other, ValueSchema):
            return False

        return (self.type, self.default) == (other.type, other.default)

    def __hash__(self):

        return hash((self.type, self.default))

    def __repr__(self):

        return f"ValueSchema(type={self.type}, default={self.default}, optional={self.optional})"

    def __str__(self):

        return self.__repr__()
