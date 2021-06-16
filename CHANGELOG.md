# Changelog

## Version 0.0.5 (Upcoming)

## Version 0.0.4

- refactoring of most modules names, mostly centered around the name of a modules 'main' type now

## Version 0.0.3

- new way of discovering modules and pipelines -- https://dharpa.org/kiara/development/module_discovery/
- inital support for persisting values via the *kiara* data store
- module namespaces: modules type names are now namespaces strings

## Version 0.0.2

- metadata extraction method renamed to 'extract_type_metadata'; also, the type metadata format changed slightly: information extracted by type goes into 'type' subkey, python class information gets added under 'python' (automatically)
- type-hint signature of parameters in ``process`` method in a ``KiaraModule`` changed from ``StepInputs``/``StepOutputs`` to ``ValueSet``
- change all input and output data access within modules to use ``ValueSet.get_value_data()``  ``ValueSet.set_value(s)`` instead of direct attribute access -- for now, direct attribute access is removed because it's not clear whether the access should be for the value object, or the data itself
- 'dict' attribute in ValueData class renamed to 'get_all_value_data'
- added 'ModuleProcessor' class, to be able to implement different module execution strategies (multithreaded, multiprocess, ...)
- renamed ``kiara.config`` module to ``kiara.module_config``
- modules are now split up into several packages: ``kiara_modules.core`` being the most important one, others are topic specific (``language_processing``, ``network_analysis``, ...)

## Version 0.0.1

- first alpha release of *kiara*
