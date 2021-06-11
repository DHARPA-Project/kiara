# -*- coding: utf-8 -*-
from pydantic import BaseModel, Field


class MetadataModel(BaseModel):
    pass


class PythonClassMetadata(MetadataModel):
    class_name: str = Field(description="The name of the Python class")
    module_name: str = Field(
        description="The name of the Python module this class lives in."
    )
    full_name: str = Field(description="The full class namespace.")
