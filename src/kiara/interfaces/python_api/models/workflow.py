# -*- coding: utf-8 -*-
import uuid
from pydantic import Field
from typing import TYPE_CHECKING, Any

from kiara.models import KiaraModel

if TYPE_CHECKING:
    from kiara.context import Kiara


class WorkflowMatcher(KiaraModel):
    """An object describing requirements values should satisfy in order to be included in a query result."""

    @classmethod
    def create_matcher(self, **match_options: Any):

        m = WorkflowMatcher(**match_options)
        return m

    has_alias: bool = Field(
        description="Workflow must have at least one alias.", default=False
    )

    def is_match(self, workflow_id: uuid.UUID, kiara: "Kiara") -> bool:

        if self.has_alias:
            aliases = kiara.workflow_registry.get_aliases(workflow_id=workflow_id)
            if not aliases:
                return False

        return True
