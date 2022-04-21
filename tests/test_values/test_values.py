# -*- coding: utf-8 -*-

#  Copyright (c) 2021, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)


# def test_values_create(kiara: Kiara):
#
#     value_schema = ValueSchema(type="string")
#     v = kiara.data_registry.register_data(value_schema=value_schema)
#     assert not v.is_set
#
#
# def test_registry_values(kiara: Kiara):
#
#     value_schema_1 = ValueSchema(type="string", required=False)
#
#     reg = kiara.data_registry
#
#     v = reg.register_data(value_schema=value_schema_1)
#     with pytest.raises(Exception):
#         v.get_value_data()
#
#     v = reg.register_data(data=None, value_schema=value_schema_1)
#     v.get_value_data()
#
#     v = reg.register_data(data=SpecialValue.NO_VALUE, value_schema=value_schema_1)
#     assert v.get_value_data() is None
#
#     v = reg.register_data(data=SpecialValue.NOT_SET, value_schema=value_schema_1)
#     with pytest.raises(Exception) as e:
#         v.get_value_data()
#
#     assert "ValueOrm not set" in str(e.value)
#
#     v = reg.register_data(data="xxx", value_schema=value_schema_1)
#     assert v.get_value_data() == "xxx"
#
#
# def test_value_set_read_only(kiara: Kiara):
#
#     schema_a = ValueSchema(type="boolean")
#     schema_b = ValueSchema(type="boolean")
#
#     value_set = SlottedValueSet.from_schemas(
#         kiara=kiara, schemas={"a": schema_a, "b": schema_b}, title="TestOutput"
#     )
#     assert value_set.get_value_data("a") is None
#     assert value_set.get_value_data("b") is None
#
#     assert not value_set.items_are_valid()
#
#     val_a = value_set.get_value_obj("a")
#     assert val_a.get_value_data() is None
#
#     with pytest.raises(Exception):
#         value_set.set_value("a", False)
#
#
# def test_value_set_impl_read_write(kiara: Kiara):
#
#     schema_a = ValueSchema(type="boolean")
#     schema_b = ValueSchema(type="boolean")
#
#     value_set = SlottedValueSet.from_schemas(
#         kiara=kiara,
#         schemas={"a": schema_a, "b": schema_b},
#         title="TestOutput",
#         read_only=False,
#     )
#     assert value_set.get_value_data("a") is None
#     assert value_set.get_value_data("b") is None
#
#     assert not value_set.items_are_valid()
#
#     val_a = value_set.get_value_obj("a")
#     assert val_a.get_value_data() is None
#
#     value_set.set_value("a", False)
#     val_a_new = value_set.get_value_obj("a")
#     assert val_a_new != val_a
#     assert val_a_new.get_value_data() is False
#
#     assert not value_set.items_are_valid()
#
#     result = value_set.set_values(a=True, b=True)
#     val_a_new_2 = result["a"]
#     assert isinstance(val_a_new_2, Value)
#     assert val_a_new != val_a_new_2
#     assert val_a_new_2 != val_a
#
#     assert value_set.items_are_valid()
#
#     assert value_set.get_all_value_data() == {"a": True, "b": True}
