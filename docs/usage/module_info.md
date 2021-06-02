# Module & Pipeline information

This group of subcommands deals with exposing information about what modules are available, and their details.

## commands for all modules

{{ cli("kiara", "module", "--help") }}

### list all available modules

List all available modules ([core](/modules/core_modules)- as well as as [pipeline](/modules/pipeline_modules)-ones).

{{ cli("kiara", "module", "list", max_height=400) }}

### display details about a module type

Display information about a modules, like description, configuration schema, source code (in case of a core-module).

#### ...for a core module

{{ cli("kiara", "module", "explain-type", "logic.and") }}

#### ...for a pipeline module

{{ cli("kiara", "module", "explain-type", "logic.nand") }}

### get properties of an instantiated module

In this context, an instantiated module is a module that was created with some optional configuration. It is necessary to know whether/what configuration is used, because that can change characteristics like available input/output fields and their schemas, which is why this gets it's own command section.

!!! note
This command also can take module configuration, in different forms. This will be documented in the future.

#### ...for a core module

{{ cli("kiara", "module", "explain-instance", "logic.and") }}

#### ...for a pipeline module

{{ cli("kiara", "module", "explain-instance", "logic.nand") }}

## pipeline-specific sub-commands

This subcommand lets you display module details that are specific to pipeline-type modules.
To print graphs, currently Java (JRE) needs to be installed, as well as the ``asciinet`` python package from GitHub (``pip install 'git+https://github.com/cosminbasca/asciinet.git#egg=asciinet&subdirectory=pyasciinet``). This might change in the future.

### list and explain pipeline steps

{{ cli("kiara", "pipeline", "explain-steps", "logic.xor") }}

### print details about the pipeline structure

This command outlines the inputs, outputs, as well as step details (how step inputs/outputs are connected) of a pipeline.

{{ cli("kiara", "pipeline", "structure", "logic.nand") }}

### print the data flow graph for a pipeline

Use the ``--full`` flag to display the non-simplified graph.

```
> kiara pipeline data-flow-graph nand
...
...
```

### print the execution graph for a pipeline

```
> kiara pipeline execution-graph nand
...
...
```
