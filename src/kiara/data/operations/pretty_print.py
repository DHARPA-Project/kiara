# -*- coding: utf-8 -*-
import typing
from pydantic import Field

from kiara import Kiara
from kiara.data.operations import OperationType


class PrettyPrintOperationType(OperationType):
    """Create a pretty, readable string from a dataset.

    Some information might be lost in the process, the main objective of this operation is to create a string that allows the user
    to view the content of a dataset, taking into account the constraints of the output medium (e.g. line-width/height on a terminal).
    """

    @classmethod
    def retrieve_operation_configs(
        cls, kiara: "Kiara"
    ) -> typing.Mapping[str, typing.Mapping[str, typing.Mapping[str, typing.Any]]]:

        result = {}
        for type_name, type_cls in kiara.type_mgmt.value_types.items():
            result[type_name] = {
                "pretty_print": {
                    "terminal": {
                        "module_type": "string.pretty_print",
                        "module_config": {
                            "target_profile": "terminal",
                        },
                    }
                }
            }

        return result

    target_profile: str = Field(description="The target profile.", default="default")

    # max_no_rows: int = Field(description="Maximum number of lines the output should have.", default=-1)
    # max_row_height: int = Field(description="If the data has rows (like a table or array), this option can limit the height of each row (if the value specified is > 0).", default=-1)
    # max_cell_length: int = Field(description="If the data has cells in some way (like cells in a table), this option can limit the length of each cell (if the value specified is > 0)", default=-1)
