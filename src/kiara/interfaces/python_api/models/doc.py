# -*- coding: utf-8 -*-
import collections.abc
from typing import TYPE_CHECKING, Dict

from pydantic import RootModel

if TYPE_CHECKING:
    # we don't want those imports (yet), since they take a while to load
    from kiara.interfaces.python_api.workflow import Workflow
    from kiara.models.module.operation import Operation
    from kiara.models.module.pipeline import PipelineStructure


#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


class OperationsMap(RootModel, collections.abc.Mapping):
    """A list of available context names."""

    root: Dict[str, "Operation"]

    def __getitem__(self, key):
        return self.root.__getitem__(key)

    def __iter__(self):
        return self.root.__iter__()

    def __len__(self):
        return self.root.__len__()


class PipelinesMap(RootModel, collections.abc.Mapping):
    """A list of available context names."""

    root: Dict[str, "PipelineStructure"]

    def __getitem__(self, key):
        return self.root.__getitem__(key)

    def __iter__(self):
        return self.root.__iter__()

    def __len__(self):
        return self.root.__len__()


class WorkflowsMap(RootModel, collections.abc.Mapping):
    """A list of available context names."""

    root: Dict[str, "Workflow"]

    def __getitem__(self, key):
        return self.root.__getitem__(key)

    def __iter__(self):
        return self.root.__iter__()

    def __len__(self):
        return self.root.__len__()
