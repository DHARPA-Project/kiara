# -*- coding: utf-8 -*-
import uuid
from pydantic import Field

from kiara.models import KiaraModel


class Workflow(KiaraModel):

    _kiara_model_id = "instance.workflow"

    full_alias: str = Field(
        description="The full alias for this workflow, within the current kiara context."
    )
    rel_alias: str = Field(
        description="The relative alias for this workflow, within the current kiara context."
    )
    workflow_id: uuid.UUID = Field(
        description="The globally unique uuid for this workflow."
    )
    workflow_archive: str = Field(
        description="The alias for the workflow archive that contains this workflow, within the current kiara context."
    )
    workflow_archive_id: uuid.UUID = Field(
        description="The globally unique id for the workflow archive that contains this workflow."
    )
