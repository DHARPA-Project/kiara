# Data operations

In *kiara*, a data operation is a collection of module executions that all do the same thing, but for different data types.

This is something that is mostly necessary for internal code, for example when *kiara* needs to print out the content of a
dataset to the terminal. In order for this output to make sense to users, different data types need to be handled differently.
For example, a simple string can be printed as is, but a table needs to be printed so row by row, and separated into columns,
which requires custom code to do.

There are a few of those instances where data operations are necessary:

 - serialization/deserialization
 - saving values to disk, and then loading them again in the next session
 - pretty printing
 - transformations to other data types
 - extracting text-content

In some cases it does not make sense for a data operation to support all possible data types, for example an ``ocr_dataset``
operation would make sense for pdf or image input data-sets, but not for tables. In other cases, like ``pretty_print``-ing,
we'd want to be able to do that for every data type.

There are a few ways to implement this, one obvious solution would be to have a convention for value type classes to have a function
for every supported data operation, maybe with prefix (e.g. ``op_ocr_dataset``), and *kiara* would inspect each class,
check the existence of all such methods, and compile a dictionary of available data-operations per data-type.  The function bodies would then be called whenever such a transformation is needed.

I've decided against this, because it would only be possible to add operations in the code that contains a value type,
and if an operation is needed for multiple types that live in different repositories, all of those would have to be changed
in order to support that operation. Also, I decided against having those operations hard-coded in Python code, because we
already have a flexible and powerful way to do stuff to data-sets: *kiara modules*.

So, the solution I decided to implement is de-coupled from the value type code, and it is declarative, instead of actual Python code:

- it is plugin-based: data operations can be added by inheriting from [OperationType][kiara.data.operation.OperationType]
- `OperationType`-classes implement a (class-) method ([retrieve_operation_configs][kiara.data.operation.OperationType.retrieve_operation_configs]), which returns a dict of dicts of dicts (keys: *value_type*/*operation_name*/*operation_id*, value: *operation_config_dict*)
- *kiara* merges the result of all of those operation configs into a single tree, with the same structure, and which can be used to query which operations are available for each value type
- once an operation is selected, *kiara* investigates the *operation config*, and creates a custom ``OperationType`` object from it
- this ``OperationType`` object is nothing more than a reference to a single *kiara* module, its optional configuration, and maps for input and output names
- the maps of input and output names can be used to 'translate' a shared operation input/output interface to the interface (input-/output-field names) of the underlying *kiara* module
- this means, that all an operation is, is an intermediate 'translation-layer', that collects and translates configurations for *kiara* module executions, and sorts them according to input types and purpose

There are a few disadvantages with this strategy (like the additional -- possibly confusion -- extra abstraction layer and indirection), but I think
the resulting flexibility will worth the trade-off. The main advantage of doing it this way is that we'll be able to re-use existing
kiara modules, and execute those operations using all existing facilities and advantages that brings (like getting data lineage, metadata,
remote execution, etc...).

## Included data operations

Here's a list of data operations that are included in *kiara*.

### ``extract_metadata``

### ``save_value``

### ``pretty_print``
