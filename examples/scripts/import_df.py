# -*- coding: utf-8 -*-
import pandas as pd

from kiara.context import Kiara

kiara = Kiara.instance()

d = {"col1": [1, 2], "col2": [3, 4]}

example_df = pd.DataFrame(data=d)
table_value = kiara.data_registry.register_data(data=example_df, schema="table")
print(table_value.model_dump())
print(table_value.data.arrow_table)
