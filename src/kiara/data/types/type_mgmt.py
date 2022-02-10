# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#  Copyright (c) 2021, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import typing

from kiara.data import Value
from kiara.data.types import ValueType
from kiara.utils.class_loading import find_all_value_types

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara


TYPE_PROFILE_MAP = {
    "csv_file": "file",
    "text_file_bundle": "file_bundle",
    "csv_file_bundle": "file_bundle",
}


class TypeMgmt(object):
    def __init__(self, kiara: "Kiara"):

        self._kiara: Kiara = kiara
        self._value_types: typing.Optional[
            typing.Dict[str, typing.Type[ValueType]]
        ] = None
        self._value_type_transformations: typing.Dict[
            str, typing.Dict[str, typing.Any]
        ] = {}
        self._registered_python_classes: typing.Dict[typing.Type, typing.List[str]] = None  # type: ignore

    def invalidate_types(self):

        self._value_types = None
        self._value_type_transformations.clear()
        self._registered_python_classes = None

    @property
    def value_types(self) -> typing.Mapping[str, typing.Type[ValueType]]:

        if self._value_types is not None:
            return self._value_types

        self._value_types = find_all_value_types()
        return self._value_types

    @property
    def value_type_names(self) -> typing.List[str]:
        return list(self.value_types.keys())

    @property
    def registered_python_classes(
        self,
    ) -> typing.Mapping[typing.Type, typing.Iterable[str]]:

        if self._registered_python_classes is not None:
            return self._registered_python_classes

        registered_types = {}
        for name, v_type in self.value_types.items():
            rel = v_type.candidate_python_types()
            if rel:
                for cls in rel:
                    registered_types.setdefault(cls, []).append(name)

        self._registered_python_classes = registered_types
        return self._registered_python_classes

    def get_type_config_for_data_profile(
        self, profile_name: str
    ) -> typing.Mapping[str, typing.Any]:

        type_name = TYPE_PROFILE_MAP[profile_name]
        return {"type": type_name, "type_config": {}}

    def determine_type(self, data: typing.Any) -> typing.Optional[ValueType]:

        if isinstance(data, Value):
            data = data.get_value_data()

        result: typing.List[ValueType] = []

        registered_types = set(self.registered_python_classes.get(data.__class__, []))
        for cls in data.__class__.__bases__:
            reg = self.registered_python_classes.get(cls)
            if reg:
                registered_types.update(reg)

        if registered_types:
            for rt in registered_types:
                _cls: typing.Type[ValueType] = self.get_value_type_cls(rt)
                match = _cls.check_data(data)
                if match:
                    result.append(match)

        # TODO: re-run all checks on all modules, not just the ones that registered interest in the class

        if len(result) == 0:
            return None
        elif len(result) > 1:
            result_str = [x._value_type_name for x in result]  # type: ignore
            raise Exception(
                f"Multiple value types found for value: {', '.join(result_str)}."
            )
        else:
            return result[0]

    def get_value_type_cls(self, type_name: str) -> typing.Type[ValueType]:

        t = self.value_types.get(type_name, None)
        if t is None:
            raise Exception(
                f"No value type '{type_name}', available types: {', '.join(self.value_types.keys())}"
            )
        return t

    # def get_value_type_transformations(
    #     self, value_type_name: str
    # ) -> typing.Mapping[str, typing.Mapping[str, typing.Any]]:
    #     """Return available transform pipelines for value types."""
    #
    #     if value_type_name in self._value_type_transformations.keys():
    #         return self._value_type_transformations[value_type_name]
    #
    #     type_cls = self.get_value_type_cls(type_name=value_type_name)
    #     _configs = type_cls.conversions()
    #     if _configs is None:
    #         module_configs = {}
    #     else:
    #         module_configs = dict(_configs)
    #     for base in type_cls.__bases__:
    #         if hasattr(base, "conversions"):
    #             _b_configs = base.conversions()  # type: ignore
    #             if not _b_configs:
    #                 continue
    #             for k, v in _b_configs.items():
    #                 if k not in module_configs.keys():
    #                     module_configs[k] = v
    #
    #     # TODO: check input type compatibility?
    #     result: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
    #     for name, config in module_configs.items():
    #         config = dict(config)
    #         module_type = config.pop("module_type", None)
    #         if not module_type:
    #             raise Exception(
    #                 f"Can't create transformation '{name}' for type '{value_type_name}', no module type specified in config: {config}"
    #             )
    #         module_config = config.pop("module_config", {})
    #         module = self._kiara.create_module(
    #             id=f"_transform_{value_type_name}_{name}",
    #             module_type=module_type,
    #             module_config=module_config,
    #         )
    #
    #         if "input_name" not in config.keys():
    #
    #             if len(module.input_schemas) == 1:
    #                 config["input_name"] = next(iter(module.input_schemas.keys()))
    #             else:
    #                 required_inputs = [
    #                     inp
    #                     for inp, schema in module.input_schemas.items()
    #                     if schema.is_required()
    #                 ]
    #                 if len(required_inputs) == 1:
    #                     config["input_name"] = required_inputs[0]
    #                 else:
    #                     raise Exception(
    #                         f"Can't create transformation '{name}' for type '{value_type_name}': can't determine input name between those options: '{', '.join(required_inputs)}'"
    #                     )
    #
    #         if "output_name" not in config.keys():
    #
    #             if len(module.input_schemas) == 1:
    #                 config["output_name"] = next(iter(module.output_schemas.keys()))
    #             else:
    #                 required_outputs = [
    #                     inp
    #                     for inp, schema in module.output_schemas.items()
    #                     if schema.is_required()
    #                 ]
    #                 if len(required_outputs) == 1:
    #                     config["output_name"] = required_outputs[0]
    #                 else:
    #                     raise Exception(
    #                         f"Can't create transformation '{name}' for type '{value_type_name}': can't determine output name between those options: '{', '.join(required_outputs)}'"
    #                     )
    #
    #         result[name] = {
    #             "module": module,
    #             "module_type": module_type,
    #             "module_config": module_config,
    #             "transformation_config": config,
    #         }
    #
    #     self._value_type_transformations[value_type_name] = result
    #     return self._value_type_transformations[value_type_name]

    def find_value_types_for_package(
        self, package_name: str
    ) -> typing.Dict[str, typing.Type[ValueType]]:

        result = {}
        for value_type_name, value_type in self.value_types.items():

            value_md = value_type.get_type_metadata()
            package = value_md.context.labels.get("package")
            if package == package_name:
                result[value_type_name] = value_type

        return result

    # def get_available_transformations_for_type(
    #     self, value_type_name: str
    # ) -> typing.Iterable[str]:
    #
    #     return self.get_value_type_transformations(value_type_name=value_type_name)

    # def transform_value(
    #     self,
    #     transformation_alias: str,
    #     value: Value,
    #     other_inputs: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    # ) -> Value:
    #
    #     transformations = self.get_value_type_transformations(value.value_schema.type)
    #
    #     if transformation_alias not in transformations.keys():
    #         raise Exception(
    #             f"Can't transform value of type '{value.value_schema.type}', transformation '{transformation_alias}' not available for this type. Available: {', '.join(transformations.keys())}"
    #         )
    #
    #     config = transformations[transformation_alias]
    #
    #     transformation_config = config["transformation_config"]
    #     input_name = transformation_config["input_name"]
    #
    #     module: KiaraModule = config["module"]
    #
    #     constants = module.get_config_value("constants")
    #     inputs = dict(constants)
    #
    #     if other_inputs:
    #
    #         for k, v in other_inputs.items():
    #             if k in constants.keys():
    #                 raise Exception(f"Invalid value '{k}' for 'other_inputs', this is a constant that can't be overwrittern.")
    #             inputs[k] = v
    #
    #     defaults = transformation_config.get("defaults", None)
    #     if defaults:
    #         for k, v in defaults.items():
    #             if k in constants.keys():
    #                 raise Exception(f"Invalid  default value '{k}', this is a constant that can't be overwrittern.")
    #             if k not in inputs.keys():
    #                 inputs[k] = v
    #
    #     if input_name in inputs.keys():
    #         raise Exception(
    #             f"Invalid value for inputs in transform arguments, can't contain the main input key '{input_name}'."
    #         )
    #
    #     inputs[input_name] = value
    #
    #     result = module.run(**inputs)
    #     output_name = transformation_config["output_name"]
    #
    #     result_value = result.get_value_obj(output_name)
    #     return result_value
