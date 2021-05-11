# *Kiara* modules

Modules are the building blocks of *Kiara*. The central element of a *Kiara* module is a [pure](https://en.wikipedia.org/wiki/Pure_function) function which performs a defined piece of work. The module also contains a type information/schema of the input values the function takes, as well as of the output it produces.

## Core- & Pipeline-modules

Currently, *Kiara* has two sorts of modules:

- [*Core modules*](core_modules.md): Python objects that inherit from the common abstract base class [KiaraModule][kiara.module.KiaraModule]
- [*Pipeline modules*](pipeline_modules.md): assemblies of other modules (*core* or *pipeline*), incl. descriptions of how those are connected. Usually expressed as ``json`` or ``yaml`` data structures.

## Module equality

One thing to be aware of is the distinction between the module type, and the module itself. The latter
is an instantiated object of the former 'class', and depending on the configuration provided when instantiating, the behaviour, as well as the input/output fields can differ between two modules of the same type.

Two modules can be considered equal if they are of the same module type, and their configuration is the same. For convenience, one can test this ``module_instance_hash``  with the [equally named property of a module instance][kiara.module.KiaraModule.module_instance_hash].

Currently Pipeline modules can't be configured, which means all module instances of the same type are equal. This is very likley to change in the future though.
