# Filtering tables

Tables are really just a list of columns, with some metadata sprinkled on top in some cases. I would recommend looking at them that way, and not as coherent, single entities, because that helps to implement efficient and re-usable modules (and code in general).

## Filter a table using a mask array

This is the recommended way of filtering a table, esp. if the characteristic a table should be filtered upon lives in a single column of the table.

The Apache Arrow website has some explanation how to do that in code: [pyarrow.compute.filter](https://arrow.apache.org/docs/python/generated/pyarrow.compute.filter.html).

Basically, you provide a (masking) arrow of booleans, with the same length as the table has rows, and use that to filter a table to only the rows where the corresponding cell in the masking array has 'true' as value.

We can come up with different ways to create such a filter array, one of the most modular ones is to use the [``[map]``](https://dharpa.org/kiara_modules.core/modules_list/#arraysmap) in combination with another module that does the actual creation of the cell value. Say we wanted to filter a table to only include rows that match a certain date range, this is how the corresponding pipeline module could look:

{{ inline_file_as_codeblock('docs/data/tables/filter_pipeline_example.json', format='json') }}

We could then use this filter with a table we onboarded earlier like:

```
kiara run docs/data/tables/filter_pipeline_example.json table=value:topic-modeling.table column_name=extract_date_from_file_name earliest="1918-04-01"
...
...
```
