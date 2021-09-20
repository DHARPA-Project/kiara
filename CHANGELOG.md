# Changelog

## Version 0.1.3 (Upcoming)

## Version 0.1.2

- module development helper subcommand (very bare bones)
- option to skip errors due to misconfigured pipeline entrypoints

## Version 0.1.1 (Upcoming)

- bug-fix release: fix error when trying to print value that wasn't set

## Version 0.1.0

- first 'official' release on Pypi.org
- the code and its API in this release can by no means be considered stable, but the largest pieces of its architecture should be in place by now
- there are still some refactorings to be made, and lots of features to implement, but this version should be good enough to do some very basic processing and data management


## Version 0.0.15

- support for saving values of type string, integer, float, bool
- some CI improvements when a tag is pushed: release to PyPi, auto-create versioned documentation

## Version 0.0.14

- this version contains a rather large refactoring of the data registry and how values are handled, so expect some breakage. Please submit issues for anything that worked, but doesn't anymore. Also, you'll have to delete the kiara shared local data (`~/.local/share/kiara` on linux) when upgrading.
- renamed:
  - kiara.pipeline.config.PipelineModuleConfig -> kiara.pipeline.config.PipelineConfig
  - kiara.module_config.ModuleTypeConfig -> kiara.module_config.ModuleTypeConfigSchema
  - kiara.module_config.ModuleConfig -> kiara.module_config.ModuleConfig
  - in kiara.pipeline.values:
    - StepInputField -> StepInputRef
    - StepOutputField -> StepOutputRef
    - PipelineInputField -> PipelineInputRef
    - PipelineOutputField -> PipelineOutputRef
  - kiara.pipeline.values -> kiara.data.registry:
    - ValueField -> ValueRef
    - PipelineValue -> removed
    - DataValue -> removed
    - LinkedValue -> removed
    - ValueUpdateHandler -> ValueUpdateHandler
  - kiara.data.values.ValueSet*-related -> kiara.data.values.value_set
- removed 'pipeline_id' attribute from 'PipelineStructure' class, but 'Pipeline" has 'id' and 'title' fields now instead
- refactored 'DataRegistry' and 'Value' object:
  - 'Value' objects are now immutable, and can only be created via a registry
  - all subclasses of 'Value' are removed, there is only one 'Value' type now, which is always connected to a data registry (which handles versioning and actual storage of the payload)
  - removed linked values, replaced by 'ValueSlot' class
  - 'ValueSlot' basically contains the history of all (immutable) Value objects on a specific spot (mostly within a pipeline, but can be used elsewhere)
  - 'set_value_data' on 'Value' class is removed (since values are no immutable)
  - the interface of 'ValueSet' however is mostly unchanged, and all 'set/get value_obj/value_data' methods should still work as before
- data store is now just a 'DataRegistry' subclass that persists to disk instead of memory, this means that getting data into the data store now uses the 'register_data' method, and getting it out uses 'get_value_obj'
- aliases can now only contain alphanumeric characters, '_' and '-"
- removed some data import modules/operations until I settled on a data onboarding strategy (current one was leaky). This is mostly relevant for the operation that imports a table from a (path) string -- use a mini-pipeline as replacement and save the table manually, something like: https://github.com/DHARPA-Project/kiara/blob/main/tests/resources/pipelines/table_import.json
- rudimentary data lineage support
- performance improvement for cli, because more stuff is now lazily loaded
- tests

### Version 0.0.13

- mostly tests

## Version 0.0.12

- renamed:
    - kiara.operations.OperationConfig -> kiara.operations.Operation
    - kiara.operations.Operations -> kiara.operations.OperationType
- re-organizing/re-naming of onboarding/import related module/operation names
- small change to how job control works in the pipeline-controller:
    - calling the `wait_for` job-ids method is now mandatory after calling the `process_step` method, even when using the synchronous non-parallel processor
    - because the `wait_for` method now comes with an argument `sync_outputs` (default: True) that allows not actually syncing the output of the processing step to the pipeline value (which gives the controller more control over when to do that)
    - if you were calling `wait_for` before, there is nothing more to do. If you used the synchronous (default) processor and omitted that step, you'll have to add a line below your `process_step` call, `wait_for`-ing for the job ids that method returned
- re-implementation/refactoring of operations (documentation still to be done)
- removed 'kiara.module.ModuleInfo' class (use 'kiara.metadata.module_models.KiaraModuleTypeMetadata' instead)
- refactorings:
    - kiara.pipeline.module.PipelineModuleInfo -> kiara.info.pipelines.PipelineModuleInfo
    - other renames/relocations of (hopefully) mostly internal classes -- if something is missing it should now be somewhere under 'kiara.info.*'
- renamed subcommand 'pipeline structure' -> 'pipeline explain'

## Version 0.0.11

- added 'BatchControllerManual' controller

## Version 0.0.10

- major refactoring:
  - renamed:
    - 'kiara.module_config.KiaraWorkflowConfig' -> 'kiara.module_config.ModuleConfig'
    - 'kiara.module_config.KiaraModuleConfig' -> 'kiara.module_config.ModuleTypeConfigSchema'
  - moved classes/functions:
    - 'kiara.data.operations' -> 'kiara.operations.type_operations'
    - 'kiara.processing.ModuleProcessor' -> 'kiara.processing.processor.ModuleProcessor'
    - from 'kiara.module_config' -> kiara.pipeline.utils:
      - create_step_value_address
      - ensure_step_value_addresses
    - from 'kiara.module_config' -> kiara.pipeline.config:
      - PipelineStepConfig
      - PipelineStructureConfig
      - PipelineConfig
    - from 'kiara.data.values' -> 'kiara.pipeline.utils':
      - generate_step_alias
    - from 'kiara.data.values' -> 'kiara.pipeline'
      - PipelineValueInfo
      - PipelineValuesInfo
    - from 'kiara.data.values' -> 'kiara.pipeline.values'
      - ValueUpdateHandler
      - StepValueAddress
      - ValueRef
      - RegisteredValue
      - RegisteredValue
      - LinkedValue
      - StepInputRef
      - StepOutputRef
      - PipelineInputRef
      - PipelineOutputRef

## Version 0.0.9

- removed 'aliases' attribute from Value class, aliases are now specified when calling 'save' on the Value object
- Job details (incl. error messages -- check the kiara.processing.Job class) for the most recent or current module executions can be retrieved: `[controller_obj].get_job_details(step_id)```
- re-write of the DataStore class:
  - support for aliases, as well as alias versions & tags (still to be documented)
  - enables the option of having different data store types down the line
  - API and overall workings of this is still a draft, so expect to see some changes to how value ids and alias are handled and look like
  - internal organisation of existing data is different, so when updating to this version you'll have to re-import your data sets and ideally also delete the old folder (``DEVELOP=true kiara data clear-data-store``)
- '--save' option in the ``kiara run`` command does now take an alias as option (previously the '--alias` flag)

## Version 0.0.6

- housekeeping

## Version 0.0.5

- this is only a stop-gap release, to a) test the release pipeline, and b) prepare for a fairly major doc/testing effort in the next few weeks
- introduced data operations: operation do the same thing to values of different types (pretty printing, serializing, etc.)

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
