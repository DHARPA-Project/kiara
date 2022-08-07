# -*- coding: utf-8 -*-
import os
from pathlib import Path
from pydantic import BaseModel, Field, validator
from typing import Any, Dict, Iterable, List, Mapping, Union

from kiara.exceptions import KiaraProcessingException
from kiara.models.module import KiaraModuleConfig
from kiara.models.values.value import ValueMap
from kiara.modules import KiaraModule, ValueMapSchema


class DataExportResult(BaseModel):

    files: List[str] = Field(description="A list of exported files.")

    @validator("files", pre=True)
    def validate_files(cls, value):

        if isinstance(value, str):
            value = [value]

        # TODO: make sure file exists

        return value


class DataExportModuleConfig(KiaraModuleConfig):

    target_profile: str = Field(
        description="The name of the target profile. Used to distinguish different target formats for the same data type."
    )
    source_type: str = Field(
        description="The type of the source data that is going to be exported."
    )


class DataExportModule(KiaraModule):

    _config_cls = DataExportModuleConfig
    _module_type_name: Union[str, None] = None

    @classmethod
    def retrieve_supported_export_combinations(cls) -> Iterable[Mapping[str, str]]:

        result = []
        for attr in dir(cls):
            if (
                len(attr) <= 16
                or not attr.startswith("export__")
                or "__as__" not in attr
            ):
                continue

            tokens = attr.split("__", maxsplit=4)
            if len(tokens) != 4:
                continue

            source_type = tokens[1]
            target_profile = tokens[3]

            data = {
                "source_type": source_type,
                "target_profile": target_profile,
                "func": attr,
            }
            result.append(data)
        return result

    def create_optional_inputs(
        self, source_type: str, target_profile: str
    ) -> Union[None, Mapping[str, Mapping[str, Any]]]:
        return None

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        source_type = self.get_config_value("source_type")
        target_profile = self.get_config_value("target_profile")

        inputs: Dict[str, Any] = {
            source_type: {
                "type": source_type,
                "doc": f"A value of type '{source_type}'.",
            },
            "base_path": {
                "type": "string",
                "doc": "The directory to export the file(s) to.",
                "optional": True,
            },
            "name": {
                "type": "string",
                "doc": "The (base) name of the exported file(s).",
                "optional": True,
            },
            "export_metadata": {
                "type": "boolean",
                "doc": "Whether to also export the value metadata.",
                "default": False,
            },
        }

        optional = self.create_optional_inputs(
            source_type=source_type, target_profile=target_profile
        )
        if optional:
            for field, field_schema in optional.items():
                field_schema = dict(field_schema)
                if field in inputs.keys():
                    raise Exception(
                        f"Can't create inputs schema for '{self.module_type_name}': duplicate field '{field}'."
                    )
                if field == source_type:
                    raise Exception(
                        f"Can't create inputs schema for '{self.module_type_name}': invalid field name '{field}'."
                    )

                optional = field_schema.get("optional", True)
                if not optional:
                    raise Exception(
                        f"Can't create inputs schema for '{self.module_type_name}': non-optional field '{field}' specified."
                    )
                field_schema["optional"] = True
                inputs[field] = field_schema

        return inputs

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        outputs = {
            "export_details": {
                "type": "dict",
                "doc": "Details about the exported files/folders.",
            }
        }
        return outputs

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:

        target_profile: str = self.get_config_value("target_profile")
        source_type: str = self.get_config_value("source_type")

        export_metadata = inputs.get_value_data("export_metadata")

        source_obj = inputs.get_value_obj(source_type)
        source = source_obj.data

        func_name = f"export__{source_type}__as__{target_profile}"
        if not hasattr(self, func_name):
            raise Exception(
                f"Can't export '{source_type}' value: missing function '{func_name}' in class '{self.__class__.__name__}'. Please check this modules documentation or source code to determine which source types and profiles are supported."
            )

        base_path = inputs.get_value_data("base_path")
        if base_path is None:
            base_path = os.getcwd()
        name = inputs.get_value_data("name")
        if not name:
            name = str(source_obj.value_id)

        func = getattr(self, func_name)
        # TODO: check signature?

        base_path = os.path.abspath(base_path)
        os.makedirs(base_path, exist_ok=True)
        result = func(value=source, base_path=base_path, name=name)

        if isinstance(result, Mapping):
            result = DataExportResult(**result)
        elif isinstance(result, str):
            result = DataExportResult(files=[result])

        if not isinstance(result, DataExportResult):
            raise KiaraProcessingException(
                f"Can't export value: invalid result type '{type(result)}' from internal method. This is most likely a bug in the '{self.module_type_name}' module code."
            )

        if export_metadata:
            metadata_file = Path(os.path.join(base_path, f"{name}.metadata"))
            value_info = source_obj.create_info()
            value_json = value_info.json()
            metadata_file.write_text(value_json)

            result.files.append(metadata_file.as_posix())

        # schema = ValueSchema(type=self.get_target_value_type(), doc="Imported dataset.")

        # value_lineage = ValueLineage.from_module_and_inputs(
        #     module=self, output_name=output_key, inputs=inputs
        # )
        # value: Value = self._kiara.data_registry.register_data(
        #     value_data=result, value_schema=schema, lineage=None
        # )

        outputs.set_value("export_details", result)
