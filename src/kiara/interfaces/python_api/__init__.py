# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import structlog
from pydantic import BaseModel, Field
from rich import box
from rich.console import RenderableType
from rich.table import Table
from typing import Any, Dict, List, Optional

from kiara.models.values.value import Value

logger = structlog.getLogger()


class StoreValueResult(BaseModel):

    value: Value = Field(description="The stored value.")
    aliases: List[str] = Field(
        description="The aliases that where assigned to the value when stored."
    )
    error: Optional[str] = Field(
        description="An error that occured while trying to store."
    )


class StoreValuesResult(BaseModel):

    __root__: Dict[str, StoreValueResult]

    def create_renderable(self, **config: Any) -> RenderableType:

        table = Table(show_header=True, show_lines=False, box=box.SIMPLE)
        table.add_column("field", style="b")
        table.add_column("data type", style="i")
        table.add_column("stored id", style="i")
        table.add_column("alias(es)")

        for field_name, value_result in self.__root__.items():
            row = [
                field_name,
                str(value_result.value.value_schema.type),
                str(value_result.value.value_id),
            ]
            if value_result.aliases:
                row.append(", ".join(value_result.aliases))
            else:
                row.append("")
            table.add_row(*row)

        return table

    def __len__(self):
        return len(self.__root__)
