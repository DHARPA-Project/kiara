=========
Changelog
=========

Version 0.0.2 (Upcoming)
======================

- metadata extraction method renamed to 'extract_type_metadata', also type metadata format changed slightly: information extracted by type goes into 'type' subkey, python class information gets added under 'python' (automatically)
- change all input data access within modules to use ``StepInput.get_value_data()`` instead of direct attribute access -- I'm not sure yet whether attribute access shouldn't actually return the value object instead. So, better to be explicit.

Version 0.0.1
===========

- first alpha release of *kiara*
