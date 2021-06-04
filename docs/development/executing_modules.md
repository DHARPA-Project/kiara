# Executing modules and pipelines

Executing modules and pipelines is the main purpose of *kiara* (well, arguably data management is, but processing stuff is a close second), and there are two different ways of doing this, depending on the circumstances.

## Creating and executing a workflow

This is the default way to run a module, and it comes with some overhead:

- every module is wrapped in a [Pipeline][kiara.pipeline.pipeline.Pipeline] object, even very simple '[core](/modules/core_modules/') modules
- every data item involved is registered in the [*kiara* data registry](/data/registry)

In most cases, this is what users want, and the overhead is not all that bad, all things considered. A very simple way to do this in code is as follows:

```python
from kiara import Kiara

kiara = Kiara.instance()

workflow = kiara.create_workflow("logic.or")
workflow.inputs.set_values(a=True, b=False)

result = workflow.outputs.get_value_data("y")
print(result)
```

TODO: explain all the different ways how this can be costumized, with different controllers, processors, etc.

## Directly run a module (or pipeline)

There is a second, more direct way to execute a module. It doesn't have as much overhead, and is simpler to do in code, but it also leaves the user less control:

```python
from kiara import Kiara

kiara = Kiara.instance()

result = kiara.run("logic.or", inputs={"a": True, "b": False}, output_name="y", resolve_result=True)
print(result)
```

This creates and executes the module object directly, without going through a workflow wrapping process. For now, this means that the values involved in the execution of this workflows won't be registered in the data registry, and will be cleaned up by the garbage collector eventually (input values can be 'registered' values, though).

This also means that *kiara* won't be able to optimize memory usage for the data involved: every result will always live in memory, fully.

The way this method works might need to change in the future, so be aware of that when you use it now, and maybe have some sort of abstraction.

In the future it will also be possible to run pipeline configurations directly like this, but that is not yet implemented.
