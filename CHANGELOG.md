# Changelog


## Version 0.0.2 (Upcoming)

- metadata extraction method renamed to 'extract_type_metadata', also type metadata format changed slightly: information extracted by type goes into 'type' subkey, python class information gets added under 'python' (automatically)
- change all input and output data access within modules to use ``StepInput.get_value_data()`` and ``StepOutput.set_value(s)`` instead of direct attribute access -- for now, direct attribute access is removed because it's not clear whether the access should be for the value object, or the data itself
- 'dict' attribute in ValueData class renamed to 'get_all_value_data'
- type-hint signature of parameters in ``process`` method in a ``KiaraModule`` changed from ``StepInputs``/``StepOutputs`` to ``ValueSet``

## Version 0.0.1

- first alpha release of *kiara*
