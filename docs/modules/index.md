# *Kiara* modules

Modules are the building blocks of *Kiara*. The central element of a *Kiara* module is a [pure](https://en.wikipedia.org/wiki/Pure_function) function which performs a defined piece of work. The module also contains a type information/schema of the input values the function takes, as well as of the output it produces.

Currently, *Kiara* has two types of modules:

- [*Core modules*](core_modules.md): Python objects that inherit from the common abstract base class [KiaraModule][kiara.module.KiaraModule]
- [*Pipeline modules*](pipeline_modules.md): assemblies of other modules (*core* or *pipeline*), incl. descriptions of how those are connected. Usually expressed as ``json`` or ``yaml`` data structures.
