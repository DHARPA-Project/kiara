# *kiara* module and pipeline discovery

Because we have a requirement to record the versions of code a data set was created with, we need to have a way of versioning our modules and pipelines. This is a harder problem than it looks from the outside, and it's the reason why *kiara* does not support 'random', unregistered [KiaraModule][kiara.module.KiaraModule] sub-classes.

There are a few ways to register plugin classes like those in Python, the most common one is to use [Python entrypoints](https://packaging.python.org/specifications/entry-points/).

I have decided to also use that, but with a slight twist, to make it easier for people who are not familiar with the Python Packaging ecosystem/dumpster-fire to create their own modules and pipelines.

## Registering core modules

To register a core module, *kiara* checks the entries for the ``kiara.modules`` entrypoint in the current Python environment. As a developer, you have two options to add your module:

### Register the ``KiaraModule`` subclass

This is the most straight-forward way to accomplish this, and in line how entrypoints are usually used. You just use the name the module should be registered under as entry point key, and the path to the class as value, ala:

```
[options.entry_points]
kiara.modules =
    metadata.extract_python_class = kiara.modules.metadata:ExtractPythonClass
```

### Register a variable that contains module information

This is a bit more involved, but ultimately easier to use for end-users, since it can be setup in advance, and it will discover ``KiaraModule`` subclasses dynamically, without users even knowing.

Instead of specifying a subclass of ``KiaraModule``, you point the endpoint to a variable somewhere in your package, like this:

```
[options.entry_points]
kiara.modules =
    playground = kiara_modules.playground:modules
```

The variable can contain 3 types of values:

 - the class object for the ``KiaraModule`` subclass (but if you do that, you might as well just use the subclass directly)
 - a callable
 - tuple with up to three items (in that order):
    - a callable
    - a list of non-keyword arguments (``*args``) for the callable
    - a map of keyword arguments (``**kwargs``) for the callable

In case of callables, those will be called (with arguments, if provided). The callable must returns a dictionary with the type name of a module as key, and the ``KiaraModule`` subclass as a value. The full kiara type name will be assembled by using the entry point name as prefix, and the module type name (the key in the resulting dict): ``[entry_point_name].[result_key]``.

This sounds more complicated than it is, because you can just use the [``find_kiara_modules_under``](kiara.utils.class_loading.find_kiara_modules_under) helper method, like:

```python
from kiara import find_kiara_modules_under
modules = (find_kiara_modules_under, ["kiara_modules.playground"])
```

In this example, *kiara* will recursively walk the ``kiara_modules.playground`` Python module and its children, and register every (non-abstract) ``KiaraModule`` class it finds (using the module namespace to asssemble the final namespaced module type name).


## Registering pipeline modules

As is documented [elsewhere](/modules/pipeline_modules), *kiara* pipelines are just Python dictionaries
that follow a [certain schema](/development/entities/modules/#pipelinemoduleconfig). In practice, they are json or yaml files living in folders that are registered with *kiara*.

Similar to the way modules are registered, *kiara* looks for pipelines via an entrypoint: ``kiara.pipelines``.

Because normal resource files or folders can't be registered in Python as an entrypoint, we need to work
around that and use the module that contains a folder containing pipelines as the value of the entrypoint:

Something like:

```
[options.entry_points]
kiara.modules =
    playground = kiara_modules:playground
```

Depending on how the package is set up, this might or might not work (the reason being 'namespaced packages', if you must know). It's safer to point the entrypoint to a variable within the module, similar to the modules registration explained above:

```
[options.entry_points]
kiara.modules =
    playground = kiara_modules.playground:pipelines
```

As above, the value can have 3 types:

 - the Python module in question
 - a callable
 - tuple with up to three items (in that order):
    - a callable
    - a list of non-keyword arguments (``*args``) for the callable
    - a map of keyword arguments (``**kwargs``) for the callable

In case of callables, those are supposed to return a list of strings, representing the Python modules to search for pipeline files.

Pipeline files are supposed to be under a folder called ``pipelines`` living in the module that was specified. Pipeline names are assembled from the entry point name, the (optional) intermediate folders, and the pipeline name.

Again, *kiara* provides a helper function so you don't have to worry about setting all this up:

```python
from kiara import find_kiara_pipelines_under
pipelines = (find_kiara_pipelines_under, ["kiara_modules.language_processing"])
```

That's that.
