# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import typing
from pydantic import BaseModel, Field

from kiara import Kiara
from kiara.data import Value
from kiara.data.onboarding import ValueStoreConfig
from kiara.operations import Operation


class BatchOnboard(BaseModel):
    @classmethod
    def create(
        cls,
        module_type: str,
        inputs: typing.Mapping[str, typing.Any],
        store_config: typing.Optional[ValueStoreConfig] = None,
        module_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        kiara: typing.Optional[Kiara] = None,
    ):

        if kiara is None:
            kiara = Kiara.instance()

        operation = Operation.create_operation(
            kiara=kiara,
            operation_id="_batch_onboarding_",
            config=module_type,
            module_config=module_config,
        )
        if store_config is None:
            store_config = ValueStoreConfig()
        elif isinstance(store_config, typing.Mapping):
            store_config = ValueStoreConfig(**store_config)

        return BatchOnboard(
            operation=operation, inputs=inputs, store_config=store_config
        )

    inputs: typing.Mapping[str, typing.Any] = Field(description="The inputs.")
    operation: Operation = Field(description="The operation to use with the inputs.")
    store_config: ValueStoreConfig = Field(
        description="The configuration for storing operation/pipeline values."
    )

    def run(self, base_id: str) -> typing.Mapping[Value, typing.Iterable[str]]:

        run_result = self.operation.module.run(**self.inputs)

        result: typing.Dict[Value, typing.List[str]] = {}
        if self.store_config.inputs:
            raise NotImplementedError("Storing inputs not supported yet.")
        if self.store_config.steps:
            raise NotImplementedError("Storing step inputs/outputs not supported yet.")
        if self.store_config.outputs:
            r = ValueStoreConfig.save_fields(
                base_id=base_id,
                value_set=run_result,
                matchers=self.store_config.outputs,
            )
            for value, aliases in r.items():
                result.setdefault(value, []).extend(aliases)

        return result
