# -*- coding: utf-8 -*-

#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

from pydantic import root_validator
from typing import TYPE_CHECKING, Any

from kiara.defaults import BATCH_CONFIG_TYPE_CATEGORY_ID
from kiara.models.module.pipeline import PipelineConfig

if TYPE_CHECKING:
    pass


class BatchConfig(PipelineConfig):
    """A class to hold the configuration for a single or set of batch module runs.

    This class is similar to the [`PipelineConfig`][kiara.models.pipeline.PipelineConfig] one, except in the way it
    handles references to inputs/outputs of the its steps.
    """

    def _retrieve_id(self) -> str:
        return str(self.model_data_hash)

    def _retrieve_category_id(self) -> str:
        return BATCH_CONFIG_TYPE_CATEGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        return self.dict()

    @root_validator()
    def _validate_aliases(cls, values):

        print("XXX")
        print(values)
        assert not values.get("input_aliases", None)
        assert not values.get("output_aliases", None)

        # values["input_aliases"] = "auto_all_outputs"
        # values["output_aliases"] = "auto_all_outputs"

        return values
