# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import typing
from pydantic import Field

from kiara import Kiara, KiaraModule
from kiara.data import ValueSet
from kiara.data.values import Value, ValueSchema
from kiara.defaults import DEFAULT_PRETTY_PRINT_CONFIG
from kiara.module_config import ModuleTypeConfigSchema
from kiara.operations import Operation, OperationType
from kiara.utils import log_message


class PrettyPrintModuleConfig(ModuleTypeConfigSchema):

    value_type: str = Field(description="The type of the value to save.")
    target_type: str = Field(
        description="The target to print the value to.", default="string"
    )


class PrettyPrintValueModule(KiaraModule):

    _config_cls = PrettyPrintModuleConfig

    @classmethod
    def retrieve_module_profiles(
        cls, kiara: Kiara
    ) -> typing.Mapping[str, typing.Union[typing.Mapping[str, typing.Any], Operation]]:

        all_metadata_profiles: typing.Dict[
            str, typing.Dict[str, typing.Dict[str, typing.Any]]
        ] = {}

        for value_type_name, value_type_cls in kiara.type_mgmt.value_types.items():

            for attr in dir(value_type_cls):
                if not attr.startswith("pretty_print_as_"):
                    continue

                target_type = attr[16:]
                if target_type not in kiara.type_mgmt.value_type_names:
                    log_message(
                        f"Pretty print target type '{target_type}' for source type '{value_type_name}' not valid, ignoring."
                    )
                    continue

                op_config = {
                    "module_type": cls._module_type_id,  # type: ignore
                    "module_config": {
                        "value_type": value_type_name,
                        "target_type": target_type,
                    },
                    "doc": f"Pretty print a value of type '{value_type_name}' as '{target_type}'.",
                }
                all_metadata_profiles[
                    f"pretty_print.{value_type_name}.as.{target_type}"
                ] = op_config

        return all_metadata_profiles

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        value_type = self.get_config_value("value_type")

        if value_type == "all":
            input_name = "value_item"
            doc = "A value of any type."
        else:
            input_name = value_type
            doc = f"A value of type '{value_type}'."

        inputs: typing.Mapping[str, typing.Any] = {
            input_name: {"type": value_type, "doc": doc},
            "print_config": {
                "type": "dict",
                "doc": "Optional print configuration.",
                "optional": True,
            },
        }

        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs: typing.Mapping[str, typing.Any] = {
            "printed": {
                "type": self.get_config_value("target_type"),
                "doc": f"The printed value as '{self.get_config_value('target_type')}'.",
            }
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        value_type: str = self.get_config_value("value_type")
        target_type: str = self.get_config_value("target_type")

        if value_type == "all":
            input_name = "value_item"
        else:
            input_name = value_type

        value_obj: Value = inputs.get_value_obj(input_name)
        print_config = dict(DEFAULT_PRETTY_PRINT_CONFIG)
        config: typing.Mapping = inputs.get_value_data("print_config")
        if config:
            print_config.update(config)

        func_name = f"pretty_print_as_{target_type}"

        if not hasattr(value_obj.type_obj, func_name):
            raise Exception(
                f"Type '{value_type}' can't be pretty printed as '{target_type}'. This is most likely a bug."
            )

        if not value_obj.is_set:
            printed = "-- not set --"
        else:
            func = getattr(value_obj.type_obj, func_name)
            # TODO: check signature
            printed = func(value=value_obj, print_config=print_config)

        outputs.set_value("printed", printed)


class PrettyPrintOperationType(OperationType):
    """This operation type renders values of any type into human readable output for different targets (e.g. terminal, html, ...).

    The main purpose of this operation type is to show the user as much content of the data as possible for the specific
    rendering target, without giving any guarantees that all information contained in the data is shown. It may print
    out the whole content (if the content is small, or the available space large enough), or
    just a few bits and pieces from the start and/or end of the data, whatever makes most sense for the data itself. For example,
    for large tables it might be a good idea to print out the first and last 30 rows, so users can see which columns are available,
    and get a rough overview of the content itself. For network graphs it might make sense to print out some graph properties
    (number of nodes, edges, available attributes, ...) on the terminal, but render the graph itself when using html as target.

    Currently, it is only possible to implement a `pretty_print` renderer if you have access to the source code of the value
    type you want to render. To add a new render method, add a new instance method to the `ValueType` class in question,
    in the format:

    ```
    def pretty_print_as_<TARGET_TYPE>(value: Value, print_config: typing.Mapping[str, typing.Any]) -> typing.Any:
       ...
       ...
    ```

    *kiara* will look at all available `ValueType` classes for methods that match this signature, and auto-generate
    operations following this naming template: `<SOURCE_TYPE>.pretty_print_as.<TARGET_TYPE>`.

    Currently, only the type `renderables` is implemented as target type (for printing out to the terminal). It is planned to
    add output suitable for use in Jupyter notebooks in the near future.
    """

    def is_matching_operation(self, op_config: Operation) -> bool:

        return issubclass(op_config.module_cls, PrettyPrintValueModule)

    def get_pretty_print_operation(
        self, value_type: str, target_type: str
    ) -> Operation:

        result = []
        for op_config in self.operations.values():
            if op_config.module_config["value_type"] != value_type:
                continue
            if op_config.module_config["target_type"] != target_type:
                continue
            result.append(op_config)

        if not result:
            raise Exception(
                f"No pretty print operation for value type '{value_type}' and output '{target_type}' registered."
            )
        elif len(result) != 1:
            raise Exception(
                f"Multiple pretty print operations for value type '{value_type}' and output '{target_type}' registered."
            )

        return result[0]

    def pretty_print(
        self,
        value: Value,
        target_type: str,
        print_config: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    ) -> typing.Any:

        ops_config = self.get_pretty_print_operation(
            value_type=value.type_name, target_type=target_type
        )
        inputs: typing.Mapping[str, typing.Any] = {
            value.type_name: value,
            "print_config": print_config,
        }
        result = ops_config.module.run(**inputs)
        printed = result.get_value_data("printed")

        return printed
