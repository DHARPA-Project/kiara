# -*- coding: utf-8 -*-
from abc import abstractmethod
from pydantic import Field
from typing import Any, Dict, List, Union

from kiara.models.module import KiaraModuleConfig
from kiara.models.values.value import ValueMap
from kiara.modules import KiaraModule, ValueMapSchema
from kiara.utils import is_develop
from kiara.utils.develop import log_dev_message


class FilterModuleConfig(KiaraModuleConfig):

    filter_name: str = Field(description="The name of the filter.")


class FilterModule(KiaraModule):

    _module_type_name: Union[str, None] = None

    @classmethod
    def get_supported_filters(cls) -> List[str]:

        result = []
        for attr in dir(cls):
            if len(attr) <= 8 or not attr.startswith("filter__"):
                continue

            filter_name = attr[8:]
            result.append(filter_name)

        if not result and is_develop():
            from rich.table import Table

            from kiara.models.python_class import PythonClass

            pcls = PythonClass.from_class(cls)
            tbl = Table.grid()
            tbl.add_column("key", style="i")
            tbl.add_column("value")
            tbl.add_row(
                "details",
                "Module class inherits from the 'FilterModule' class, but doesn't implement any methods that start with 'filter__.",
            )
            tbl.add_row("reference", "TODO")
            tbl.add_row("python class", pcls)
            log_dev_message(tbl)
        return result

    @classmethod
    @abstractmethod
    def retrieve_supported_type(cls) -> Union[Dict[str, Any], str]:
        pass

    @classmethod
    def get_supported_type(cls) -> Dict[str, Any]:

        data = cls.retrieve_supported_type()
        if isinstance(data, str):
            data = {"type": data, "type_config": {}}
        else:
            # TODO: more validation?
            assert "type" in data.keys()
            if "type_config" not in data.keys():
                data["type_config"] = {}

        return data

    _config_cls = FilterModuleConfig

    def create_filter_inputs(self, filter_name: str) -> Union[None, ValueMapSchema]:
        return None

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        filter_name = self.get_config_value("filter_name")

        data_type_data = self.get_supported_type()
        data_type = data_type_data["type"]
        data_type_config = data_type_data["type_config"]

        inputs: Dict[str, Any] = {
            "value": {
                "type": data_type,
                "type_config": data_type_config,
                "doc": f"A value of type '{data_type}'.",
            },
        }

        filter_inputs = self.create_filter_inputs(filter_name=filter_name)

        if filter_inputs:
            for field, field_schema in filter_inputs.items():
                field_schema = dict(field_schema)
                if field in inputs.keys():
                    raise Exception(
                        f"Can't create inputs schema for '{self.module_type_name}': duplicate field '{field}'."
                    )

                filter_inputs_optional = field_schema.get("optional", False)
                filter_inputs_default = field_schema.get("default", None)
                if not filter_inputs_optional and filter_inputs_default is None:
                    raise Exception(
                        f"Can't create inputs schema for '{self.module_type_name}': non-optional field '{field}' specified."
                    )
                field_schema["optional"] = True
                inputs[field] = field_schema

        return inputs

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        data_type_data = self.get_supported_type()
        data_type = data_type_data["type"]
        data_type_config = data_type_data["type_config"]

        outputs = {
            "value": {
                "type": data_type,
                "type_config": data_type_config,
                "doc": "The filtered value.",
            }
        }
        return outputs

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:

        filter_name: str = self.get_config_value("filter_name")
        data_type_data = self.__class__.get_supported_type()
        data_type = data_type_data["type"]
        # data_type_config = data_type_data["type_config"]
        # TODO: ensure value is of the right type?

        source_obj = inputs.get_value_obj("value")

        func_name = f"filter__{filter_name}"
        if not hasattr(self, func_name):
            raise Exception(
                f"Can't apply filter '{filter_name}': missing function '{func_name}' in class '{self.__class__.__name__}'. Please check this modules documentation or source code to determine which filters are supported."
            )

        func = getattr(self, func_name)
        # TODO: check signature?

        filter_inputs = {}
        for k, v in inputs.items():
            if k == data_type:
                continue
            filter_inputs[k] = v.data

        result = func(value=source_obj, filter_inputs=filter_inputs)

        if result is None:
            outputs.set_value("value", source_obj)
        else:
            outputs.set_value("value", result)
