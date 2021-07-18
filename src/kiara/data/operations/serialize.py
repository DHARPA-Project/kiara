# -*- coding: utf-8 -*-
import typing

from kiara.data.operations import OperationType

if typing.TYPE_CHECKING:
    from kiara.kiara import Kiara


class SerializeOperationType(OperationType):
    """Save a dataset into the internal kiara data store."""

    @classmethod
    def retrieve_operation_configs(
        cls, kiara: "Kiara"
    ) -> typing.Mapping[str, typing.Mapping[str, typing.Mapping[str, typing.Any]]]:

        s_module = kiara.get_module_class("bytes.msgpack.from_value")

        result = {}
        for type_name, type_cls in kiara.type_mgmt.value_types.items():

            if not hasattr(s_module, f"from_{type_name}"):
                continue

            result[type_name] = {
                "serialize": {
                    "msgpack": {
                        "module_type": "bytes.msgpack.from_value",
                        "module_config": {"type_name": type_name},
                        "input_name": "value_item",
                    }
                }
            }

        return result


class DeserializeOperationType(OperationType):
    """Save a dataset into the internal kiara data store."""

    @classmethod
    def retrieve_operation_configs(
        cls, kiara: "Kiara"
    ) -> typing.Mapping[str, typing.Mapping[str, typing.Mapping[str, typing.Any]]]:

        return {
            "bytes": {
                "deserialize": {
                    "msgpack": {
                        "module_type": "bytes.msgpack.to_value",
                        "input_name": "bytes",
                    }
                }
            }
        }
