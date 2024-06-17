# Changelog

## Version 0.5.12 (upcoming)

## Version 0.5.11

- bugfixes:
  - cached aliases in session are not updated when aliases are stored
  - minor store result printable output fix
- updated ci & build processes

## Version 0.5.10

- archive export & import feature:
  - new cli subcommands:
    - `kiara archive import`
    - `kiara archive export`
    - `kiara archive explain`
    - `kiara data import`
    - `kiara data export`
  - new api endpoints:
    - `retrieve_archive_info`
    - `export_archive`
    - `import_archive`
    - `export_values`
    - `import_values`
- always store every job record and job result value(s)
- allow a 'comment' to be associated with a job:
  - require a 'comment' for every `run_job`/`queue_job` call
- new job record api endpoints:
  - `list_all_job_record_ids`
  - `list_job_record_ids`
  - `list_all_job_records`
  - `list_job_records`
  - `get_job_record`
  - `get_job_comment`
  - `set_job_comment`
- add convenience api endpoint `get_values`
- improved input options for 'store_values' API endpoint
- added 'value_created' property on 'Value' instances
- add '--runtime-info' cli flag to display runtime folders/files used by *kiara*
- fix: plugin info for plugins with '-' in name
- moved `KiaraAPI` class to `kiara.interfaces.python_api.kiara_api` module (the 'offical' import path `kiara.api.KiaraAPI` is still available, and should be used)
- have `KiaraAPI` proxy a `BaseAPI` class, to make it easier to extend the API and keep it stable

## Version 0.5.9

- mostly test coverage improvements
- fix: support alias prefixed strings as job inputs

## Version 0.5.8

- add 'mock' module type

## Version 0.5.5

- added pipeline related api endpoints:
  - `list_pipeline_ids`
  - `list_pipelines`
  - `get_pipeline`
  - `retrieve_pipeline_info`
  - 'retrieve_pipelines_info'

- added support for quering plugin information
  - cli: `kiara info plugin`
  - api endpoints:
    - `list_available_plugin_names`
    - `retrieve_plugin_info`
    - `retrieve_plugin_infos`

## Version 0.3.1 - 0.5.4

- changes not tracked

## Version 0.3.1

- allow 'dict' field name

## Version 0.3.0

- changed metadata format of stored value metadata: data store must be cleared when updating to this version
- refactoring of operation type input names -- this replaces most instances where the input name was 'value_item' (or similar). I've decided that using the value type as input name increases usability of those operations more than the costs associated with having different input names for operations of the same type, for example:
  - pretty_print: 'value_item' -> type name of the value to pretty print
  - extract_metadata: 'value_item' -> type name of the value to extract metadata from
  - import: 'source' input -> input file type, 'value_item' output -> target file type
  - save_value: 'value_item' input -> file type of value to save
  - convert (renamed to 'create'): value_item to source profile and target type

## Version 0.2.2

- fixes for cli pretty printing
- auto-publish kiara conda package

## Version 0.2.1

- removed 'save' and 'aliases' config/input options from import operations (it turns out its much better overall to do saving explicitely, not within modules)
- processing metrics and information will be added to value metadata
- rudimentary rendering template management
- 'kiara info' subcommand, displaying the current context (incl. all modules & operations)

## Version 0.2.0

- add conda packages
- support for extra pipeline folders via KiaraConfig & cli
- moved 'any' type into this package

## Version 0.1.3

- add sample operation type
- some minor bug fixes

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
  - kiara.module_config.ModuleTypeConfig -> kiara.module_config.KiaraModuleConfig
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
  - kiara.data.values.ValueMap*-related -> kiara.data.values.value_set
- removed 'pipeline_id' attribute from 'PipelineStructure' class, but 'Pipeline" has 'id' and 'title' fields now instead
- refactored 'DataRegistry' and 'Value' object:
  - 'Value' objects are now immutable, and can only be created via a registry
  - all subclasses of 'Value' are removed, there is only one 'Value' type now, which is always connected to a data registry (which handles versioning and actual storage of the payload)
  - removed linked values, replaced by 'ValueSlot' class
  - 'ValueSlot' basically contains the history of all (immutable) Value objects on a specific spot (mostly within a pipeline, but can be used elsewhere)
  - 'set_value_data' on 'Value' class is removed (since values are no immutable)
  - the interface of 'ValueMap' however is mostly unchanged, and all 'set/get value_obj/value_data' methods should still work as before
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
- removed 'kiara.module.ModuleInfo' class (use 'kiara.metadata.module_models.ModuleTypeInfo' instead)
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
    - 'kiara.module_config.KiaraModuleConfig' -> 'kiara.module_config.KiaraModuleConfig'
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
- ActiveJob details (incl. error messages -- check the kiara.processing.ActiveJob class) for the most recent or current module executions can be retrieved: `[controller_obj].get_job_details(step_id)```
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
- type-hint signature of parameters in ``process`` method in a ``KiaraModule`` changed from ``StepInputs``/``StepOutputs`` to ``ValueMap``
- change all input and output data access within modules to use ``ValueMap.get_value_data()``  ``ValueMap.set_value(s)`` instead of direct attribute access -- for now, direct attribute access is removed because it's not clear whether the access should be for the value object, or the data itself
- 'dict' attribute in ValueData class renamed to 'get_all_value_data'
- added 'ModuleProcessor' class, to be able to implement different module execution strategies (multithreaded, multiprocess, ...)
- renamed ``kiara.config`` module to ``kiara.module_config``
- modules are now split up into several packages: ``kiara_modules.core`` being the most important one, others are topic specific (``language_processing``, ``network_analysis``, ...)

## Version 0.0.1

- first alpha release of *kiara*
