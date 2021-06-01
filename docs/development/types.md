
# class methods

## ``type_name``

    @classmethod
    def type_name(cls):
        """Return the name/alias of this type.

        This is the name modules will use in the 'type' field when they create their input/output schemas.

        Returns:
            the type alias
        """

        cls_name = cls.__name__
        if cls_name.lower().endswith("type"):
            cls_name = cls_name[0:-4]

        type_name = camel_case_to_snake_case(cls_name)
        return type_name

## ``conversions``

    @classmethod
    def conversions(
        self,
    ) -> typing.Optional[typing.Mapping[str, typing.Mapping[str, typing.Any]]]:
        """Return a dictionary of configuration for modules that can transform this type.

        The name of the transformation is the key of the result dictionary, the configuration is a module configuration
        (dictionary wth 'module_type' and optional 'module_config', 'input_name' and 'output_name' keys).
        """
        return {"string": {"module_type": "strings.pretty_print", "input_name": "item"}}

Example (for ``table``):

    @classmethod
    def conversions(
        self,
    ) -> typing.Optional[typing.Mapping[str, typing.Mapping[str, typing.Any]]]:
        """Return a dictionary of configuration for modules that can transform this type.

        The name of the transformation is the key of the result dictionary, the configuration is a module configuration
        (dictionary wth 'module_type' and optional 'module_config', 'input_name' and 'output_name' keys).
        """
        return {
            "string": {
                "module_type": "strings.pretty_print",
                "input_name": "item",
                "defaults": {"max_cell_length": 240, "max_no_rows": 20},
            },
            "json": {"module_type": "json.to_json", "input_name": "item"},
        }

## ``python_types``


    @classmethod
    def python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return None

Example (for ``table``):

    @classmethod
    def python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [pa.Table]


## ``check_data``


    @classmethod
    def check_data(cls, data: typing.Any) -> typing.Optional["ValueType"]:
        return None

Example (for ``table``):

    @classmethod
    def check_data(cls, data: typing.Any) -> typing.Optional["ValueType"]:

        if isinstance(data, pa.Table):
            return TableType()

        return None


## ``load_config``

Example for ``table``:

    @classmethod
    def load_config(cls) -> typing.Mapping[str, typing.Any]:

        return {
            "module_type": "read_arrow_table",
            "module_config": {
                "constants": {
                    "format": "feather"
                }
            }
        }

## ``save_config``

Example for ``table``:

    @classmethod
    def save_config(cls) -> typing.Mapping[str, typing.Any]:

        return {
            "module_type": "write_arrow_table",
            "module_config": {
                "constants": {
                    "format": "feather"
                }
            }
        }

## ``generate_load_inputs``

    @classmethod
    def generate_load_inputs(self, value_id: str, persistance_mgmt: PersistanceMgmt):

        path = persistance_mgmt.get_path(value_id=value_id) / "table.feather"
        return {
            "path": path
        }

## ``generate_save_inputs``

    @classmethod
    def generate_save_inputs(self, value: Value, persistance_mgmt: PersistanceMgmt):

        path = persistance_mgmt.get_path(value_id=value.id) / "table.feather"
        return {
            "path": path
        }

# instance methods
