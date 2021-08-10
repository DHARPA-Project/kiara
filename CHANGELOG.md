# Changelog

## Version 0.0.12 (upcoming)

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
    - 'kiara.module_config.KiaraWorkflowConfig' -> 'kiara.module_config.ModuleInstanceConfig'
    - 'kiara.module_config.KiaraModuleConfig' -> 'kiara.module_config.ModuleTypeConfig'
  - moved classes/functions:
    - 'kiara.data.operations' -> 'kiara.operations.type_operations'
    - 'kiara.processing.ModuleProcessor' -> 'kiara.processing.processor.ModuleProcessor'
    - from 'kiara.module_config' -> kiara.pipeline.utils:
      - create_step_value_address
      - ensure_step_value_addresses
    - from 'kiara.module_config' -> kiara.pipeline.config:
      - PipelineStepConfig
      - PipelineStructureConfig
      - PipelineModuleConfig
    - from 'kiara.data.values' -> 'kiara.pipeline.utils':
      - generate_step_alias
    - from 'kiara.data.values' -> 'kiara.pipeline'
      - PipelineValue
      - PipelineValues
    - from 'kiara.data.values' -> 'kiara.pipeline.values'
      - ValueUpdateHandler
      - StepValueAddress
      - ValueField
      - KiaraValue
      - DataValue
      - LinkedValue
      - StepInputField
      - StepOutputField
      - PipelineInputField
      - PipelineOutputField


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
