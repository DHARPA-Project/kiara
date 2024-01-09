# *kiara* stores

This page contains some information about how *kiara* stores work.

Practically, there are two types of stores in *kiara*:

- *archives*: stores that can only be read from, but not written to
- *stores*: atual 'stores', those are read as well as write

*kiara* has different store types, depending on what exactly is stored:

- *data stores*: stores that store actual data, those are the most important ones
- *alias stores*: stores that keep human readable references (aliases), and link them to actual data (using their value_id)
- *job stores*: stores details and records about past jobs that were run in a *kiara* instance

## Base store

All archives & stores inherit from the base class 'kiara.registries.BaseArchive', which manages basic attributes like thie stores id, it's configuration, and it holds a reference to the current kiara context.

As a developer, you probably won't be using this directly, but you will inherit from either a higher level abstract base class, in case of data-stores that would be:

- `kiara.registries.data.DataArchive`
- `kiara.registries.data.DataStore`

Depending on what you want to store, it's a good idea to check out the source code of those base classes, and look up which methods you need to implement.
Also, you can check out the default implementation of such a store/archive ('filesystem'-based in all cases), to get an idea what needs to happen in each of those methods.
