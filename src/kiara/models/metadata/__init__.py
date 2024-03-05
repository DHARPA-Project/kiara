# -*- coding: utf-8 -*-
from typing import Any, ClassVar

from pydantic import Field

from kiara.models import KiaraModel


class KiaraMetadata(KiaraModel):
    def _retrieve_data_to_hash(self) -> Any:
        return {"metadata": self.model_dump(), "schema": self.schema_json()}


class CommentMetadata(KiaraMetadata):

    _kiara_model_id: ClassVar = "instance.kiara_metadata.comment"

    comment: str = Field(description="A note/comment.")
